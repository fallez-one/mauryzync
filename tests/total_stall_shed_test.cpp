// Total-stall poll + runtime shed (pool_service::watchdog_total_stall_poll / watchdog_finalize_wait /
// watchdog_on_migrate; FCS_EXPERIMENTAL_ALWAYS_ON).
//
//  Built with the macro ON  (fcs_workers_total_stall_shed_test):
//    part 1  white-box, deterministic: the migration batch, evacuating / injecting a deque, the
//            proof-of-life marks, the drain/refill claims, the cushion and its refill staging, and the
//            rebuild-after-clone resets.
//    part 2  pool, stand-in resurrector: a stall that ends inside the window is NOT escalated; a
//            quorum that is not met IS; the shed refuses new work while it runs and, when the clone
//            fails, undoes itself -- every queued task still runs exactly once.
//    part 3  pool, REAL fork (POSIX only): three workers wedged, one of them holding a mutex. The
//            clone gets the tasks (30 in a worker's deque, 8 in the cushion, across both lanes and
//            both priorities), the hook repairs the mutex, every task runs exactly once, the parent
//            exits. Run in a throw-away process so the parent's exit() is the scenario's, not the test's.
//  Built with the macro OFF (fcs_workers_total_stall_shed_off_test): the three calls exist, chain, and
//    change nothing -- a totally wedged pool is left exactly as wedged as it was.
#include <FCS/Worker/workers.hpp>
#include <FCS/Worker/detail/worker_registry.hpp>
#include <FCS/Worker/sync/interruptible_mutex.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <thread>
#include <vector>

#if defined(__SANITIZE_ADDRESS__)
#  include <sanitizer/lsan_interface.h>
#endif

#if FCS_EXPERIMENTAL_ALWAYS_ON && !defined(_WIN32)
#  include <cerrno>
#  include <csignal>
#  include <poll.h>
#  include <dirent.h>
#  include <sys/wait.h>
#  include <unistd.h>
#  define FCS_TEST_REAL_FORK 1
#else
#  define FCS_TEST_REAL_FORK 0
#endif

namespace {
using namespace std::chrono_literals;
namespace W = FCS::Worker;
namespace D = FCS::Worker::detail;
int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

template<typename Pred>
bool wait_for(Pred&& pred, std::chrono::milliseconds limit) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= end) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

// ============================================================================ macro OFF
#if !FCS_EXPERIMENTAL_ALWAYS_ON

void off_is_noop() {
    std::printf("macro OFF: the calls exist, chain, and do nothing\n");
    W::pool_service<> pool;
    bool hook_ran = false;
    // The hook type exists too, so code that names it compiles either way.
    W::pool_service<>::migration_hook named = {};
    (void)named;
    auto& chained = pool.concurrency(2).execution_policy(W::execution::dedicated_poller)
                        .watchdog_interval(2ms).watchdog_mark_stall(10ms)
                        .watchdog_total_stall_poll(50ms, 1)
                        .watchdog_finalize_wait(10ms)
                        .watchdog_on_migrate([&hook_ran](W::pool_service<>::migration_context&) { hook_ran = true; });
    CHECK(&chained == &pool);
    pool.start();

    std::atomic<bool> release{false};
    std::atomic<int> wedged{0};
    for (int i = 0; i < 2; ++i) CHECK(pool.enqueue([&] { wedged.fetch_add(1); while (!release.load()) std::this_thread::sleep_for(1ms); }));
    CHECK(wait_for([&] { return wedged.load() == 2; }, 2000ms));

    std::this_thread::sleep_for(400ms);   // total stall for 40x the (ignored) window
    CHECK(!pool.shedding_migration());
    CHECK(!hook_ran);
    std::atomic<int> ran{0};
    CHECK(pool.enqueue([&ran] { ran.fetch_add(1); }));   // still accepting work: no shed refusal
    CHECK(pool.last_migration_report().evacuated() == 0);

    release.store(true);
    CHECK(wait_for([&] { return ran.load() == 1; }, 2000ms));
    pool.stop();
}

#else
// ============================================================================ macro ON

// ---------------------------------------------------------------- part 1: white-box
using registry_t = D::worker_registry<256, 8>;
using fast_t = D::priority_mpmc_queue<D::queued_task, 8>;
using slow_t = D::bounded_mpmc_queue<D::queued_task, 16>;
using gate_t = D::admission_gate<64, 16, D::queued_task>;

D::queued_task make_task(std::atomic<int>& sum, int value, W::workload lane = W::workload::Fast, D::task_priority prio = D::task_priority::normal) {
    return D::queued_task{lane, W::source_kind::synthetic, D::task{[&sum, value] { sum.fetch_add(value); }}, prio};
}

void part1_batch() {
    std::printf("part 1a: migration batch (bounded 2-D array)\n");
    std::atomic<int> sum{0};
    D::migration_batch<D::queued_task> batch;
    CHECK(!batch.configured());
    batch.configure(2, 4);
    CHECK(batch.rows() == 2 + D::migration_batch<D::queued_task>::shared_rows);
    CHECK(batch.fast_row() == 2 && batch.slow_row() == 3 && batch.pending_row() == 4 && batch.cushion_row() == 5);

    for (int i = 1; i <= 4; ++i) CHECK(batch.put(1, make_task(sum, i)));
    CHECK(!batch.room(1));
    auto refused = make_task(sum, 100);
    CHECK(!batch.put(1, std::move(refused)));     // full: refused ...
    CHECK(static_cast<bool>(refused.invoke));     // ... and the caller still owns it
    CHECK(batch.size(1) == 4 && batch.size(0) == 0 && batch.total() == 4);

    int order = 0;
    for (auto& t : batch.row(1)) { t.invoke(); order = order * 10 + (sum.load() > 0 ? 1 : 0); }
    CHECK(sum.load() == 1 + 2 + 3 + 4);           // all four are there, in a row you can walk
    batch.row(1)[2].invoke.reset();               // the hook drops one
    CHECK(!batch.row(1)[2].invoke);

    batch.clear_row(1);
    CHECK(batch.size(1) == 0);
    CHECK(batch.put(0, make_task(sum, 5)));
    batch.abandon();                              // leaked on purpose (parent of a clone): no destructor run
    CHECK(batch.total() == 0);
}

void part1_registry() {
    std::printf("part 1b: registry -- evacuate, inject, proof of life, claims, reset\n");
    D::scheduler_metrics metrics;
    const auto t0 = std::chrono::steady_clock::now();
    std::atomic<int> sum{0};

    // evacuate_local: oldest first, as a thief, capped by room()
    {
        registry_t reg; reg.configure(3);
        for (int i = 1; i <= 10; ++i) CHECK(reg.inject_local(1, make_task(sum, i), metrics));
        CHECK(metrics.local_depth() == 10);

        std::vector<int> seen;
        const auto got = reg.evacuate_local(1, metrics, [&] { return seen.size() < 4; },
            [&](D::queued_task&& t) { const auto before = sum.load(); t.invoke(); seen.push_back(sum.load() - before); });
        CHECK(got == 4);
        CHECK((seen == std::vector<int>{1, 2, 3, 4}));           // head (oldest) first
        CHECK(metrics.local_depth() == 6);

        seen.clear();
        CHECK(reg.evacuate_local(1, metrics, [] { return true; },
            [&](D::queued_task&& t) { const auto before = sum.load(); t.invoke(); seen.push_back(sum.load() - before); }) == 6);
        CHECK((seen == std::vector<int>{5, 6, 7, 8, 9, 10}));
        CHECK(metrics.local_depth() == 0);
        D::queued_task none{};
        CHECK(!reg.take_local(1, none, metrics));                // really empty afterwards
    }

    // inject_local is bounded by LocalCapacity
    {
        D::worker_registry<8, 8> small; small.configure(1);
        for (int i = 0; i < 8; ++i) CHECK(small.inject_local(0, make_task(sum, 1), metrics));
        auto ninth = make_task(sum, 1);
        CHECK(!small.inject_local(0, std::move(ninth), metrics));
        CHECK(static_cast<bool>(ninth.invoke));
        D::queued_task out{};
        while (small.take_local(0, out, metrics)) {}
    }

    // proof of life: a tick that moved is a worker that ran; stalled is counted
    {
        registry_t reg; reg.configure(3);
        registry_t::progress_marks marks;
        reg.mark_progress(marks);
        CHECK(reg.progressed_since(marks) == 0);
        { const auto in_task = reg.scope(0); }
        CHECK(reg.progressed_since(marks) == 1);
        { const auto a = reg.scope(0); }                         // the same worker again is still ONE worker
        CHECK(reg.progressed_since(marks) == 1);
        { const auto b = reg.scope(2); }
        CHECK(reg.progressed_since(marks) == 2);

        const auto w0 = reg.scope(0); const auto w1 = reg.scope(1); const auto w2 = reg.scope(2);
        (void)reg.scan_stalls(t0, 10ms);
        const auto scan = reg.scan_stalls(t0 + 20ms, 10ms);
        CHECK(scan.busy == 3 && scan.stalled == 3 && scan.newly_stalled == 3);   // total stall
        CHECK(reg.scan_stalls(t0 + 40ms, 10ms).stalled == 3);                    // `stalled` is a level, newly_stalled an edge
        CHECK(reg.in_task(0) && reg.in_task(1) && reg.in_task(2));
    }

    // claims: held, they stop a global->local drain
    {
        registry_t reg; reg.configure(2);
        fast_t fast; slow_t slow;
        CHECK(fast.try_push(make_task(sum, 1)));
        metrics.adjust_mpmc_depth(1);
        CHECK(reg.claim_drain());
        CHECK(!reg.claim_drain());
        CHECK(!reg.drain_global(0, fast, slow, metrics));        // refused: nothing slips into a swept deque
        reg.release_drain();
        CHECK(reg.drain_global(0, fast, slow, metrics));
        D::queued_task out{};
        while (reg.take_local(0, out, metrics)) {}
    }

    // reset_after_clone: a deque frozen mid-life comes back empty and usable, claims released
    {
#if defined(__SANITIZE_ADDRESS__)
        __lsan_disable();                                        // the old deques are leaked ON PURPOSE (see reset_after_clone)
#endif
        registry_t reg; reg.configure(2);
        for (int i = 0; i < 250; ++i) CHECK(reg.inject_local(0, make_task(sum, 1), metrics));   // 2 segments
        CHECK(reg.claim_drain());
        const auto frozen = reg.scope(1);                        // "a worker frozen inside a task"
        (void)reg.scan_stalls(t0, 10ms); (void)reg.scan_stalls(t0 + 50ms, 10ms);
        CHECK(reg.stalled(1));
        reg.reset_after_clone();
        metrics.reset_gauges();
        CHECK(!reg.stalled(1) && !reg.in_task(1));
        D::queued_task out{};
        CHECK(!reg.take_local(0, out, metrics));
        CHECK(reg.claim_drain());                                // the claim a vanished thread held is gone
        reg.release_drain();
        CHECK(reg.inject_local(0, make_task(sum, 7), metrics));
        CHECK(reg.take_local(0, out, metrics));
        const auto before = sum.load(); out.invoke(); CHECK(sum.load() == before + 7);
#if defined(__SANITIZE_ADDRESS__)
        __lsan_enable();
#endif
    }
}

void part1_gate() {
    std::printf("part 1c: admission gate -- evacuate cushion + refill staging, readmit, reset\n");
    D::scheduler_metrics metrics;
    std::atomic<int> sum{0};
    gate_t gate;
    fast_t fast; slow_t slow;

    // Fill the fast lane so a refill CAN'T place what it pops: the popped tasks stay in the staging array.
    for (int i = 0; i < 8; ++i) { CHECK(fast.try_push(make_task(sum, 1000))); }
    for (int i = 1; i <= 5; ++i) CHECK(gate.submit(make_task(sum, i), metrics));
    CHECK(metrics.cushion_depth() == 5);
    CHECK(!gate.refill(metrics, fast, slow));                    // nothing moved: all 5 sit staged
    CHECK(metrics.cushion_depth() == 5);                         // staged tasks still count as cushion
    for (int i = 6; i <= 8; ++i) CHECK(gate.submit(make_task(sum, i), metrics));   // 3 more in the queue proper

    CHECK(gate.claim_refill());
    CHECK(!gate.claim_refill());
    std::vector<int> pending, cushion;
    const auto record = [&](std::vector<int>& into) {
        return [&into, &sum](D::queued_task&& t) { const auto before = sum.load(); t.invoke(); into.push_back(sum.load() - before); };
    };
    CHECK(gate.evacuate_pending(metrics, [] { return true; }, record(pending)) == 5);
    CHECK(gate.evacuate_cushion(metrics, [] { return true; }, record(cushion)) == 3);
    CHECK((pending == std::vector<int>{1, 2, 3, 4, 5}));        // the older layer, in order
    CHECK((cushion == std::vector<int>{6, 7, 8}));
    CHECK(metrics.cushion_depth() == 0);
    gate.release_refill();

    // readmit: no submit accounting, honors capacity
    const auto submitted_before = metrics.snapshot(gate.cushion_state(), W::queue_state::empty, gate.admission_state()).submitted;
    auto back = make_task(sum, 9);
    CHECK(gate.readmit(back, metrics));
    CHECK(metrics.cushion_depth() == 1);
    CHECK(metrics.snapshot(gate.cushion_state(), W::queue_state::empty, gate.admission_state()).submitted == submitted_before);
    CHECK(gate.cushion_state() != W::queue_state::empty);

    // reset_after_clone: a refill claim a vanished worker held, and staged leftovers, are gone
    CHECK(gate.claim_refill());                                  // "frozen mid-refill"
    gate.reset_after_clone();
    metrics.reset_gauges();
    CHECK(gate.claim_refill());
    gate.release_refill();
    CHECK(gate.cushion_state() == W::queue_state::empty);
    CHECK(gate.submit(make_task(sum, 1), metrics));              // usable again, thresholds intact
}

// ---------------------------------------------------------------- scenario plumbing for parts 2 and 3
struct stand_in_resurrect final : D::process_resurrect<stand_in_resurrect> {
    static inline std::atomic<int> calls{0};
    static inline std::atomic<int> delay_ms{0};
    // The clone "fails" -- after a delay the test can use as a window into the shed in progress.
    [[nodiscard]] FCS::Expected<D::fork_role, D::resurrect_error> try_fork_impl() const noexcept {
        calls.fetch_add(1);
        if (const auto d = delay_ms.load()) std::this_thread::sleep_for(std::chrono::milliseconds{d});
        return FCS::Unexpected<D::resurrect_error>{11u};
    }
};
struct stand_in_traits : W::default_pool_traits { using resurrector = stand_in_resurrect; };
using stand_in_pool = W::pool_service<1024, stand_in_traits>;

constexpr int kSmalls = 30;     // end up in the deque of the one worker that is not wedged yet
constexpr int kExtras = 8;      // submitted once everything is wedged: they stay in the cushion
constexpr int kTasks = kSmalls + kExtras;

// Three workers, all wedged, with tasks queued in two layers:
//   * workers 0/1 each sit in a blocker (the one on `blocker_hook` runs it first);
//   * the third is held by a gate task while kSmalls + a last blocker are submitted, then released:
//     it pulls the whole backlog into ITS deque, pops the newest (the blocker) and wedges, leaving
//     the kSmalls behind it in its deque;
//   * kExtras more go in afterwards, into the cushion (nobody is left to refill it).
template<typename Pool>
struct wedge_scenario {
    std::atomic<bool> release{false};
    std::atomic<int> wedged{0};
    std::atomic<bool> gate_open{false};
    std::array<std::atomic<int>, kTasks + 1> ran{};
    std::function<void()> before_wedge;       // run by blocker 1 right before it wedges (e.g. take a lock)
    std::function<void(int)> on_task;         // run by every counted task before it is counted

    void wedge_here() {
        wedged.fetch_add(1);
        while (!release.load(std::memory_order_acquire)) std::this_thread::sleep_for(1ms);
    }

    void build(Pool& pool) {
        for (int b = 0; b < 2; ++b) {
            CHECK(pool.enqueue([this, b] { if (b == 1 && before_wedge) before_wedge(); wedge_here(); }));
        }
        CHECK(wait_for([this] { return wedged.load() == 2; }, 3000ms));

        CHECK(pool.enqueue([this] { while (!gate_open.load(std::memory_order_acquire)) std::this_thread::sleep_for(100us); }));
        std::this_thread::sleep_for(5ms);     // the gate task is on the third worker
        for (int i = 1; i <= kSmalls; ++i) CHECK(pool.enqueue([this, i] { if (on_task) on_task(i); ran[static_cast<std::size_t>(i)].fetch_add(1); }));
        CHECK(pool.enqueue([this] { wedge_here(); }));
        gate_open.store(true, std::memory_order_release);
        CHECK(wait_for([this] { return wedged.load() == 3; }, 3000ms));

        int next = kSmalls + 1;
        for (int i = 0; i < 4; ++i, ++next) CHECK(pool.enqueue([this, next] { if (on_task) on_task(next); ran[static_cast<std::size_t>(next)].fetch_add(1); }));
        for (int i = 0; i < 2; ++i, ++next) CHECK(pool.enqueue_priority(D::task_priority::high, W::source_kind::synthetic, [this, next] { if (on_task) on_task(next); ran[static_cast<std::size_t>(next)].fetch_add(1); }));
        for (int i = 0; i < 2; ++i, ++next) CHECK(pool.enqueue(W::workload::Slow, [this, next] { if (on_task) on_task(next); ran[static_cast<std::size_t>(next)].fetch_add(1); }));
    }

    [[nodiscard]] int total_ran() const {
        int n = 0;
        for (int i = 1; i <= kTasks; ++i) n += ran[static_cast<std::size_t>(i)].load();
        return n;
    }
    [[nodiscard]] bool each_exactly_once() const {
        for (int i = 1; i <= kTasks; ++i) if (ran[static_cast<std::size_t>(i)].load() != 1) return false;
        return true;
    }
};

// ---------------------------------------------------------------- part 2: pool, stand-in resurrector
void part2_false_alarm() {
    std::printf("part 2a: a stall that ends inside the window is not escalated\n");
    stand_in_resurrect::calls = 0;
    stand_in_pool pool;
    pool.concurrency(2).execution_policy(W::execution::dedicated_poller)
        .watchdog_interval(2ms).watchdog_mark_stall(10ms).watchdog_total_stall_poll(500ms, 1);
    pool.start();

    std::atomic<bool> release{false};
    std::atomic<int> wedged{0};
    for (int i = 0; i < 2; ++i) CHECK(pool.enqueue([&] { wedged.fetch_add(1); while (!release.load()) std::this_thread::sleep_for(1ms); }));
    CHECK(wait_for([&] { return wedged.load() == 2; }, 2000ms));
    std::this_thread::sleep_for(80ms);        // total stall detected, poll running (window 500ms)
    CHECK(!pool.shedding_migration());
    release.store(true);                      // they "get the CPU back"
    std::this_thread::sleep_for(700ms);       // well past the window
    CHECK(!pool.shedding_migration());
    CHECK(stand_in_resurrect::calls.load() == 0);
    {
        const auto st = pool.resurrection_stats();
        CHECK(st.polls_opened >= 1 && st.polls_cleared == st.polls_opened && st.sheds_started == 0);
        CHECK(st.last_poll_ns > 0 && st.last_poll_ns < 500'000'000ull);
    }

    std::atomic<int> ran{0};
    for (int i = 0; i < 20; ++i) CHECK(pool.enqueue([&ran] { ran.fetch_add(1); }));
    CHECK(wait_for([&] { return ran.load() == 20; }, 2000ms));
    pool.stop();
}

void part2_quorum() {
    std::printf("part 2b: quorum counts DISTINCT workers (1 of 2 alive is not enough for quorum 2)\n");
    stand_in_resurrect::calls = 0; stand_in_resurrect::delay_ms = 0;
    stand_in_pool pool;
    pool.concurrency(2).execution_policy(W::execution::dedicated_poller)
        .watchdog_interval(2ms).watchdog_mark_stall(10ms).watchdog_total_stall_poll(150ms, 2);
    pool.start();

    std::array<std::atomic<bool>, 2> go{};
    std::atomic<int> wedged{0};
    for (int i = 0; i < 2; ++i) CHECK(pool.enqueue([&, i] { wedged.fetch_add(1); while (!go[static_cast<std::size_t>(i)].load()) std::this_thread::sleep_for(1ms); }));
    CHECK(wait_for([&] { return wedged.load() == 2; }, 2000ms));
    std::this_thread::sleep_for(40ms);
    go[0].store(true);                        // one worker proves it is alive; the other stays wedged
    CHECK(wait_for([] { return stand_in_resurrect::calls.load() >= 1; }, 2000ms));   // window ended: escalated
    go[1].store(true);
    CHECK(wait_for([&] { return !pool.shedding_migration(); }, 2000ms));             // the failed clone was undone
    pool.stop();
}

void part2_rollback() {
    std::printf("part 2c: shed in progress refuses work; a failed clone is undone, every task runs exactly once\n");
    stand_in_resurrect::calls = 0;
    stand_in_resurrect::delay_ms = 400;       // the "clone" takes a while, then fails
    stand_in_pool pool;
    pool.concurrency(3).execution_policy(W::execution::dedicated_poller)
        .watchdog_interval(2ms).watchdog_mark_stall(10ms).watchdog_total_stall_poll(100ms, 1);
    pool.start();

    wedge_scenario<stand_in_pool> sc;
    sc.build(pool);

    // Window: the flag is up and the queues are already evacuated, the clone has not answered yet.
    CHECK(wait_for([] { return stand_in_resurrect::calls.load() >= 1; }, 3000ms));
    CHECK(pool.shedding_migration());
    CHECK(!pool.enqueue([] {}));                                   // refused: it would be lost with this process
    const auto report = pool.last_migration_report();
    std::printf("   evacuated: local=%zu fast=%zu slow=%zu pending=%zu cushion=%zu  residual gauge=%llu\n",
                report.evacuated_local, report.evacuated_fast, report.evacuated_slow, report.evacuated_pending,
                report.evacuated_cushion, static_cast<unsigned long long>(report.residual_gauge));
    CHECK(report.evacuated_local == static_cast<std::size_t>(kSmalls));
    CHECK(report.evacuated_cushion == static_cast<std::size_t>(kExtras));
    CHECK(report.evacuated() == static_cast<std::size_t>(kTasks));
    CHECK(!report.refill_staging_claimed_elsewhere);
    CHECK(report.residual_gauge == 0);

    sc.release.store(true);                                        // the wedged workers get unstuck mid-shed ...
    std::this_thread::sleep_for(100ms);
    CHECK(sc.total_ran() == 0);                                    // ... and, being stood down, run nothing

    CHECK(wait_for([&] { return !pool.shedding_migration(); }, 3000ms));   // clone failed -> undone
    CHECK(wait_for([&] { return sc.total_ran() >= kTasks; }, 3000ms));
    std::this_thread::sleep_for(100ms);
    CHECK(sc.each_exactly_once());
    CHECK(stand_in_resurrect::calls.load() == 1);
    const auto after = pool.last_migration_report();
    std::printf("   re-injected: direct=%zu cushion=%zu mpmc=%zu refused=%zu\n", after.injected_direct, after.injected_cushion, after.injected_mpmc, after.refused);
    CHECK(after.injected_direct == 0);                             // owners are live: shared queues only
    CHECK(after.injected_cushion + after.injected_mpmc == static_cast<std::size_t>(kTasks));
    CHECK(after.refused == 0);
    const auto st = pool.resurrection_stats();
    CHECK(st.polls_opened == 1 && st.polls_cleared == 0);
    CHECK(st.sheds_started == 1 && st.sheds_aborted == 1 && st.resurrections == 0 && st.generation == 0);
    CHECK(st.tasks_evacuated == static_cast<std::uint64_t>(kTasks) && st.tasks_reinjected == static_cast<std::uint64_t>(kTasks));
    CHECK(st.tasks_refused == 0 && st.tasks_unreachable == 0);
    CHECK(st.submits_refused >= 1);
    CHECK(st.last_poll_ns >= 100'000'000ull && st.last_evacuation_ns > 0);
    std::printf("   telemetry: poll %.0f ms, sweep %.2f ms, whole shed %.0f ms, %llu submit(s) refused\n", static_cast<double>(st.last_poll_ns) / 1e6,
                static_cast<double>(st.last_evacuation_ns) / 1e6, static_cast<double>(st.last_shed_ns) / 1e6, static_cast<unsigned long long>(st.submits_refused));

    std::atomic<int> ran{0};                                       // and the pool is simply a pool again
    CHECK(pool.enqueue([&ran] { ran.fetch_add(1); }));
    CHECK(wait_for([&] { return ran.load() == 1; }, 2000ms));
    pool.stop();
}

// ---------------------------------------------------------------- part 3: real fork
#if FCS_TEST_REAL_FORK
struct wire {
    std::uint32_t kind;
    std::uint64_t a;
    std::uint64_t b;
};
enum : std::uint32_t { k_hook = 1, k_task = 2, k_evacuated = 3, k_injected = 4, k_injected2 = 5, k_done = 6, k_parent_exit = 7 };

int g_fd = -1;
void send(std::uint32_t kind, std::uint64_t a = 0, std::uint64_t b = 0) {
    const wire w{kind, a, b};
    const auto n = ::write(g_fd, &w, sizeof w);   // <= PIPE_BUF: atomic
    (void)n;
}
std::size_t thread_count_of_this_process() {
    std::size_t n = 0;
    if (DIR* d = ::opendir("/proc/self/task")) {
        while (const auto* e = ::readdir(d)) if (e->d_name[0] != '.') ++n;
        ::closedir(d);
    }
    return n;
}

using fork_pool = W::pool_service<>;
FCS::Worker::synchronization::interruptible_mutex g_mutex;   // held by a worker that wedges: nobody will ever unlock it

// Runs in the throw-away process S. Never returns: S ends in the pool's parent path (resurrector.exit)
// -- or, if the shed never comes, in the timeout at the bottom.
[[noreturn]] void fork_scenario() {
    static fork_pool pool;                                      // static: the parent's exit() must not wait on its wedged workers
    static wedge_scenario<fork_pool> sc;

    sc.before_wedge = [] { g_mutex.lock(); };                  // wedged WHILE HOLDING it
    static std::atomic<int> reported{0};
    sc.on_task = [](int i) {
        if (i == 5) { g_mutex.lock(); g_mutex.unlock(); }      // needs the mutex the dead worker holds
        send(k_task, static_cast<std::uint64_t>(i));
        if (reported.fetch_add(1) + 1 == kTasks) {             // the clone has no main thread: the last task closes it
            const auto st = pool.resurrection_stats();
            send(k_injected2, st.generation, st.resurrections);
            send(k_done, st.tasks_reinjected, st.tasks_evacuated);
            ::_exit(0);
        }
    };

    pool.concurrency(3).execution_policy(W::execution::dedicated_poller)
        .watchdog_interval(2ms).watchdog_mark_stall(10ms)
        .watchdog_total_stall_poll(100ms, 1)
        .watchdog_finalize_wait(50ms)
        .watchdog_on_migrate([](fork_pool::migration_context& ctx) {
            send(k_hook, static_cast<std::uint64_t>(::getpid()), thread_count_of_this_process());
            if (g_mutex.locking()) g_mutex.unlock();           // repair what the vanished worker held
            send(k_evacuated, ctx.report.evacuated_local, ctx.report.evacuated_cushion);
            const auto st = pool.resurrection_stats();
            send(k_injected, st.sheds_started, st.polls_opened);   // inherited from the parent: 1 shed, >=1 poll
        });
        // after the clone the stats also say which generation this is, reported by the finisher below
    pool.start();
    sc.build(pool);

    // The clone reports its own telemetry once every task has run (it has no main thread: a pool
    // worker's task does it). Registered as one more task, submitted by the hook-free path below.
    // Nothing else may happen in this process after the clone but the parent's exit().
    std::this_thread::sleep_for(10s);
    ::_exit(3);                                                // the shed never came
}

void part3_real_fork() {
    std::printf("part 3: real fork -- 3 workers wedged (one holding a mutex), tasks in a deque and the cushion\n");
    int fds[2];
    if (::pipe(fds) != 0) { ++failures; std::printf("  FAIL pipe\n"); return; }

    std::fflush(stdout);   // a fork copies unflushed stdio buffers: both processes would print them
    const pid_t scenario = ::fork();
    if (scenario < 0) { ++failures; std::printf("  FAIL fork\n"); return; }
    if (scenario == 0) {
        ::close(fds[0]);
        g_fd = fds[1];
        fork_scenario();
    }
    ::close(fds[1]);

    // Collect records until every writer (the parent S and the clone) is gone, or time runs out.
    std::vector<wire> got;
    pid_t clone_pid = 0;
    bool timed_out = false;
    const auto end = std::chrono::steady_clock::now() + 20s;
    for (;;) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(end - std::chrono::steady_clock::now()).count();
        if (left <= 0) { timed_out = true; break; }
        pollfd p{fds[0], POLLIN, 0};
        const int r = ::poll(&p, 1, static_cast<int>(left));
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) { timed_out = (r == 0); break; }
        wire w{};
        const auto n = ::read(fds[0], &w, sizeof w);
        if (n == 0) break;                                    // EOF: both processes closed their end
        if (n != static_cast<ssize_t>(sizeof w)) continue;
        got.push_back(w);
        if (w.kind == k_hook) clone_pid = static_cast<pid_t>(w.a);
    }
    ::close(fds[0]);
    if (timed_out) {
        std::printf("  (timed out waiting for the clone)\n");
        if (clone_pid > 0) ::kill(clone_pid, SIGKILL);
        ::kill(scenario, SIGKILL);
    }
    int status = 0;
    (void)::waitpid(scenario, &status, 0);

    CHECK(!timed_out);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);     // the parent left through resurrector.exit(0), not the 10 s timeout (3)

    std::array<int, kTasks + 1> ran{};
    int hooks = 0; std::uint64_t threads_in_hook = 0, evac_local = 0, evac_cushion = 0;
    for (const auto& w : got) {
        if (w.kind == k_task && w.a >= 1 && w.a <= kTasks) ++ran[static_cast<std::size_t>(w.a)];
        if (w.kind == k_hook) { ++hooks; threads_in_hook = w.b; }
        if (w.kind == k_evacuated) { evac_local = w.a; evac_cushion = w.b; }
    }
    int smalls_once = 0;
    for (int i = 1; i <= kSmalls; ++i) smalls_once += ran[static_cast<std::size_t>(i)] == 1 ? 1 : 0;
    std::printf("   hook ran %d time(s) in pid %d with %llu thread(s); evacuated local=%llu cushion=%llu; %d/%d deque tasks ran exactly once\n",
                hooks, static_cast<int>(clone_pid), static_cast<unsigned long long>(threads_in_hook),
                static_cast<unsigned long long>(evac_local), static_cast<unsigned long long>(evac_cushion), smalls_once, kSmalls);
    CHECK(hooks == 1);
    CHECK(clone_pid != scenario && clone_pid > 0);            // it really ran in a different process
    CHECK(threads_in_hook == 1);                              // and only the watchdog's thread existed there
    CHECK(evac_local == static_cast<std::uint64_t>(kSmalls));
    CHECK(evac_cushion == static_cast<std::uint64_t>(kExtras));
    CHECK(smalls_once == kSmalls);                            // includes task 5, which needed the mutex the hook repaired
    int extras_once = 0;
    for (int i = kSmalls + 1; i <= kTasks; ++i) extras_once += ran[static_cast<std::size_t>(i)] == 1 ? 1 : 0;
    CHECK(extras_once == kExtras);                            // the cushion ones too, both lanes, both priorities
    for (const auto& w : got) if (w.kind == k_injected) { CHECK(w.a == 1); CHECK(w.b >= 1); }
    bool done = false;
    for (const auto& w : got) {
        if (w.kind == k_injected2) { CHECK(w.a == 1 && w.b == 1); }          // generation 1, one resurrection
        if (w.kind == k_done) { done = true; CHECK(w.a == static_cast<std::uint64_t>(kTasks)); CHECK(w.b == static_cast<std::uint64_t>(kTasks)); }   // telemetry survived the clone
    }
    CHECK(done);
}
#endif // FCS_TEST_REAL_FORK

#endif // FCS_EXPERIMENTAL_ALWAYS_ON

} // namespace

int main() {
#if !FCS_EXPERIMENTAL_ALWAYS_ON
    off_is_noop();
#else
    part1_batch();
    part1_registry();
    part1_gate();
    part2_false_alarm();
    part2_quorum();
    part2_rollback();
#  if FCS_TEST_REAL_FORK
    part3_real_fork();
#  else
    std::printf("part 3: skipped (no fork on this platform / see the NT notes in process_resurrect.hpp)\n");
#  endif
#endif
    std::printf(failures ? "total_stall_shed: %d FAILED\n" : "total_stall_shed: all passed\n", failures);
    return failures ? 1 : 0;
}
