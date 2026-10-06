// Cooperative coroutines: a pool-driven coroutine gives its worker back when the stall
// watchdog flags it, is re-queued (and demoted after repeated hogging), and -- because it can
// now resume on any worker -- is safe to co_await other pool work from.
//
//  A  yield at checkpoints : a coroutine hogging the only worker is yielded; a queued task
//                            gets through in ~tolerance, not after the whole hog. Control: the
//                            same hog without checkpoints blocks it for the full duration.
//  B  requeue_behind       : the continuation lands exactly `behind` tasks back in the owner's own
//                            deque (deterministic, registry-level); bounded when the deque is full.
//  C  external await       : co_await async_completed_result inside a driven coroutine (used to
//                            resume inline on the completing thread, racing the poller) -- run
//                            many concurrently under TSan.
//  D  compatibility        : plain suspend_always still completes; exceptions still surface as
//                            callback_threw; yield_point() outside a pool never suspends.
#include <FCS/Worker/workers.hpp>
#include <FCS/Worker/detail/worker_registry.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {
using namespace std::chrono_literals;
namespace W = FCS::Worker;
int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

void spin_for(std::chrono::microseconds d) {
    const auto end = std::chrono::steady_clock::now() + d;
    while (std::chrono::steady_clock::now() < end) {}
}

template<typename Pred>
bool wait_for(Pred&& pred, std::chrono::milliseconds limit = 10000ms) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > end) return false;
        std::this_thread::sleep_for(500us);
    }
    return true;
}

std::uint64_t total(W::pool_service<>& pool, std::uint64_t W::detail::worker_steal_snapshot::* field) {
    std::uint64_t sum = 0;
    for (std::size_t i = 0; i < pool.worker_count(); ++i) sum += pool.worker_profile(i).*field;
    return sum;
}

// ---- coroutines (non-capturing lambdas: parameters, not captures)
constexpr auto hog_with_checkpoints = [](int rounds) -> FCS::async_result<int> {
    int done = 0;
    for (int i = 0; i < rounds; ++i) {
        spin_for(1ms);
        ++done;
        co_await W::yield_point();
    }
    co_return done;
};
constexpr auto hog_without_checkpoints = [](int rounds) -> FCS::async_result<int> {
    int done = 0;
    for (int i = 0; i < rounds; ++i) { spin_for(1ms); ++done; }
    co_return done;
};

template<typename Hog>
std::chrono::milliseconds latency_of_small_task_behind(W::pool_service<>& pool, Hog hog, int rounds, int* hog_result) {
    std::atomic<std::int64_t> ran_at{0};
    const auto start = std::chrono::steady_clock::now();
    auto fut = pool.enqueue_and_poll(W::workload::Fast, hog, rounds);
    std::this_thread::sleep_for(25ms); // let the hog take the only worker
    const auto queued_at = std::chrono::steady_clock::now();
    CHECK(pool.enqueue([&ran_at] { ran_at.store(std::chrono::steady_clock::now().time_since_epoch().count()); }));
    CHECK(wait_for([&] { return ran_at.load() != 0; }, 5000ms));
    const auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::time_point{std::chrono::steady_clock::duration{ran_at.load()}} - queued_at);
    CHECK(wait_for([&] { return fut.ready(); }));
    auto r = fut.user_data();
    CHECK(r.has_value());
    if (r) *hog_result = *r;
    (void)start;
    return latency;
}

void test_a_yield_at_checkpoints() {
    std::printf("A: a flagged coroutine yields at its checkpoint\n");
    constexpr int rounds = 300; // ~300 ms of work
    {
        W::pool_service<> pool;
        pool.concurrency(1).watchdog_interval(2ms).watchdog_mark_stall(10ms).start();
        int result = 0;
        const auto latency = latency_of_small_task_behind(pool, hog_with_checkpoints, rounds, &result);
        std::printf("   with yield_point : small task ran after %lld ms (hog = ~%d ms), forced yields=%llu\n",
                    static_cast<long long>(latency.count()), rounds, static_cast<unsigned long long>(total(pool, &W::detail::worker_steal_snapshot::coop_yields)));
        CHECK(result == rounds);                                              // every round still ran, in order
        CHECK(latency < 120ms);                                               // ~tolerance, nowhere near the whole hog
        CHECK(total(pool, &W::detail::worker_steal_snapshot::coop_yields) >= 1);
        CHECK(total(pool, &W::detail::worker_steal_snapshot::coop_global_requeues) == 0); // stayed in the owner's own deque
        pool.stop();
    }
    {
        W::pool_service<> pool;
        pool.concurrency(1).watchdog_interval(2ms).watchdog_mark_stall(10ms).start();
        int result = 0;
        const auto latency = latency_of_small_task_behind(pool, hog_without_checkpoints, rounds, &result);
        std::printf("   control (no yield): small task ran after %lld ms\n", static_cast<long long>(latency.count()));
        CHECK(result == rounds);
        CHECK(latency > 150ms);                                               // nothing to yield at: blocked for the rest of the hog
        pool.stop();
    }
}

void test_b_requeue_behind() {
    std::printf("B: requeue_behind places the continuation N tasks back in the owner's deque\n");
    namespace D = W::detail;
    using registry_t = D::worker_registry<8, 4>;               // LocalCapacity 8: small, so "full" is reachable
    using fast_t = D::priority_mpmc_queue<D::queued_task, 64>;
    using slow_t = D::bounded_mpmc_queue<D::queued_task, 64>;
    D::scheduler_metrics metrics;

    // pop order of a registry whose worker 0 owns tasks t1..t5 (t5 on the bottom) after requeueing X
    const auto pop_order = [&](std::size_t behind) {
        registry_t reg; reg.configure(1);
        fast_t fast; slow_t slow;
        std::vector<int> ran;
        const auto make = [&](int id) { return D::queued_task{W::workload::Fast, W::source_kind::synthetic, D::task{[&ran, id] { ran.push_back(id); }}}; };
        for (int id = 1; id <= 5; ++id) { CHECK(fast.try_push(make(id))); metrics.adjust_mpmc_depth(1); }
        CHECK(reg.drain_global(0, fast, slow, metrics));       // owner pops 5,4,3,2,1 (LIFO from the bottom)
        CHECK(reg.requeue_behind(0, make(99), behind, metrics));
        D::queued_task out{};
        while (reg.take_local(0, out, metrics)) out.invoke();
        return ran;
    };
    const auto eq = [](const std::vector<int>& a, std::vector<int> b) { return a == b; };
    CHECK(eq(pop_order(1), {5, 99, 4, 3, 2, 1}));                // swap with the next one
    CHECK(eq(pop_order(3), {5, 4, 3, 99, 2, 1}));                // by three
    CHECK(eq(pop_order(5), {5, 4, 3, 2, 1, 99}));
    CHECK(eq(pop_order(16), {5, 4, 3, 2, 1, 99}));               // fewer tasks than asked: behind all of them, never lost

    {   // an empty deque: just the continuation
        registry_t reg; reg.configure(1);
        std::vector<int> ran;
        CHECK(reg.requeue_behind(0, D::queued_task{W::workload::Fast, W::source_kind::synthetic, D::task{[&ran] { ran.push_back(7); }}}, 3, metrics));
        D::queued_task out{};
        while (reg.take_local(0, out, metrics)) out.invoke();
        CHECK(eq(ran, {7}));
    }
    {   // bounded: a full deque refuses, and nothing is disturbed
        registry_t reg; reg.configure(1);
        fast_t fast; slow_t slow; std::vector<int> ran;
        const auto make = [&](int id) { return D::queued_task{W::workload::Fast, W::source_kind::synthetic, D::task{[&ran, id] { ran.push_back(id); }}}; };
        for (int id = 1; id <= 8; ++id) { CHECK(fast.try_push(make(id))); metrics.adjust_mpmc_depth(1); }
        CHECK(reg.drain_global(0, fast, slow, metrics));       // exactly LocalCapacity (8)
        CHECK(!reg.requeue_behind(0, make(99), 3, metrics));
        D::queued_task out{};
        while (reg.take_local(0, out, metrics)) out.invoke();
        CHECK(eq(ran, {8, 7, 6, 5, 4, 3, 2, 1}));
    }
}

// ---- C: external awaits from inside driven coroutines
constexpr auto add_one_after_work = [](int v) -> int { spin_for(200us); return v + 1; };
// Under load the pool may refuse the inner submit (error::queue_full -- backpressure is real); a
// well-behaved coroutine retries, and the checkpoint in the loop is what lets the watchdog pull
// it off the worker if every worker ends up retrying while the queue needs draining.
constexpr auto awaits_twice = [](W::pool_service<>* pool, int x) -> FCS::async_result<int> {
    int value = x;
    for (int step = 0; step < 2; ++step) {
        for (;;) {
            auto r = co_await pool->enqueue_and_poll(W::workload::Fast, add_one_after_work, value);
            if (r) { value = *r; break; }
            if (r.error() != W::error::queue_full) co_return -1;
            std::this_thread::yield();
            co_await W::yield_point();
        }
        co_await W::yield_point();
    }
    co_return value;
};

void test_c_external_await() {
    std::printf("C: co_await pool work from inside a driven coroutine (no inline resume races)\n");
    W::pool_service<> pool;
    pool.concurrency(6).start();
    constexpr int n = 300;
    using fut_t = decltype(pool.enqueue_and_poll(W::workload::Fast, awaits_twice, &pool, 0));
    std::vector<fut_t> futures;
    futures.reserve(n);
    for (int i = 0; i < n; ++i) {
        for (;;) {
            auto f = pool.enqueue_and_poll(W::workload::Fast, awaits_twice, &pool, i);
            if (f.sequence() != std::uint64_t(-1)) { futures.push_back(std::move(f)); break; }
        }
    }
    int good = 0, bad = 0;
    for (int i = 0; i < n; ++i) {
        CHECK(wait_for([&] { return futures[static_cast<std::size_t>(i)].ready(); }));
        auto r = futures[static_cast<std::size_t>(i)].user_data();
        if (r && *r == i + 2) ++good; else ++bad;
    }
    std::printf("   %d/%d correct\n", good, n);
    CHECK(bad == 0);
    pool.stop();
}

// ---- D: compatibility
constexpr auto plain_suspends = [](int base) -> FCS::async_result<int> {
    int v = base;
    co_await std::suspend_always{}; ++v;
    co_await std::suspend_always{}; ++v;
    co_await std::suspend_always{}; ++v;
    co_return v;
};
constexpr auto throws_in_body = [](int) -> FCS::async_result<int> { throw std::runtime_error("boom"); co_return 0; };
constexpr auto throws_before_coroutine = [](int) -> FCS::async_result<int> { throw std::runtime_error("early"); };
constexpr auto pull_with_checkpoint = []() -> FCS::pull_result<int> { co_await W::yield_point(); co_return 5; };

void test_d_compat() {
    std::printf("D: plain suspensions, exceptions and standalone coroutines are unchanged\n");
    W::pool_service<> pool;
    pool.concurrency(2).start();
    auto a = pool.enqueue_and_poll(W::workload::Fast, plain_suspends, 4);
    auto b = pool.enqueue_and_poll(W::workload::Fast, throws_in_body, 0);
    auto c = pool.enqueue_and_poll(W::workload::Fast, throws_before_coroutine, 0);
    CHECK(wait_for([&] { return a.ready() && b.ready() && c.ready(); }));
    auto ra = a.user_data(); auto rb = b.user_data(); auto rc = c.user_data();
    CHECK(ra.has_value() && *ra == 7);
    CHECK(!rb.has_value() && rb.error() == W::error::callback_threw);
    CHECK(!rc.has_value() && rc.error() == W::error::callback_threw);
    pool.stop();

    auto pulled = pull_with_checkpoint().pull();   // not pool-driven: the checkpoint must not strand it
    CHECK(pulled.has_value() && *pulled == 5);
}

// ---- E: everything at once, for TSan
void test_e_mixed_load() {
    std::printf("E: yields + external awaits + plain tasks together\n");
    W::pool_service<> pool;
    pool.concurrency(4).watchdog_interval(1ms).watchdog_mark_stall(3ms).coroutine_yield_depth(3).start();
    std::atomic<int> plain_done{0};
    constexpr int hogs = 24, awaiters = 120, plain = 2000;
    using hog_t = decltype(pool.enqueue_and_poll(W::workload::Fast, hog_with_checkpoints, 0));
    using aw_t = decltype(pool.enqueue_and_poll(W::workload::Fast, awaits_twice, &pool, 0));
    std::vector<hog_t> hog_futures; std::vector<aw_t> aw_futures;
    hog_futures.reserve(hogs); aw_futures.reserve(awaiters);
    for (int i = 0; i < hogs; ++i) hog_futures.push_back(pool.enqueue_and_poll(W::workload::Fast, hog_with_checkpoints, 15));
    for (int i = 0; i < awaiters; ++i) aw_futures.push_back(pool.enqueue_and_poll(W::workload::Fast, awaits_twice, &pool, i));
    for (int i = 0; i < plain; ++i) while (!pool.enqueue([&plain_done] { plain_done.fetch_add(1); })) std::this_thread::yield();
    int ok = 0;
    for (int i = 0; i < hogs; ++i) { CHECK(wait_for([&] { return hog_futures[static_cast<std::size_t>(i)].ready(); }, 20000ms)); auto r = hog_futures[static_cast<std::size_t>(i)].user_data(); if (r && *r == 15) ++ok; }
    for (int i = 0; i < awaiters; ++i) { CHECK(wait_for([&] { return aw_futures[static_cast<std::size_t>(i)].ready(); }, 20000ms)); auto r = aw_futures[static_cast<std::size_t>(i)].user_data(); if (r && *r == i + 2) ++ok; }
    CHECK(wait_for([&] { return plain_done.load() == plain; }));
    std::printf("   %d/%d coroutines correct, %d/%d plain tasks, yields=%llu\n", ok, hogs + awaiters, plain_done.load(), plain,
                static_cast<unsigned long long>(total(pool, &W::detail::worker_steal_snapshot::coop_yields)));
    CHECK(ok == hogs + awaiters);
    pool.stop();
}

} // namespace

int main() {
    test_a_yield_at_checkpoints();
    test_b_requeue_behind();
    test_c_external_await();
    test_d_compat();
    test_e_mixed_load();
    std::printf(failures ? "coop_yield_test: %d FAILED\n" : "coop_yield_test: all passed\n", failures);
    return failures ? 1 : 0;
}
