// Stall watchdog tests.
//
//  Part 1 (deterministic, no threads/sleeps): drives worker_registry directly with
//  an explicit clock -- the stall flag is set exactly at the tolerance, thieves
//  respect the steal threshold until then and take everything after, short tasks
//  are never flagged, and a recovering worker is un-flagged.
//
//  Part 2 (end to end): a pool whose worker is wedged in one long task while it
//  holds a small (< minimum_steal_depth) pile in its local deque. With the
//  watchdog the pile is rescued in ~tolerance; the control run (watchdog off)
//  is printed for comparison but not asserted (it depends on where the pile
//  happens to land).
#include <FCS/Worker/workers.hpp>
#include <FCS/Worker/detail/worker_registry.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace {
using namespace std::chrono_literals;
namespace W = FCS::Worker;
namespace D = FCS::Worker::detail;
int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

constexpr std::size_t min_depth = 7;
using registry_t = D::worker_registry<256, 8>;
using fast_t = D::priority_mpmc_queue<D::queued_task, 64>;
using slow_t = D::bounded_mpmc_queue<D::queued_task, 64>;

D::queued_task make_task(std::atomic<int>& ran) {
    return D::queued_task{W::workload::Fast, W::source_kind::synthetic, D::task{[&ran] { ran.fetch_add(1); }}};
}

void part1_registry() {
    std::printf("part 1: registry-level (virtual clock)\n");
    const auto t0 = std::chrono::steady_clock::now();
    const auto tolerance = 10ms;
    D::scheduler_metrics metrics;

    // ---- a wedged worker holding a small pile
    {
        registry_t reg; reg.configure(2);
        fast_t fast; slow_t slow; std::atomic<int> ran{0};
        for (int i = 0; i < 5; ++i) { CHECK(fast.try_push(make_task(ran))); metrics.adjust_mpmc_depth(1); }
        CHECK(reg.drain_global(0, fast, slow, metrics));        // worker 0 now owns 5 tasks (< 7)

        D::queued_task out{};
        CHECK(!reg.steal(1, out, metrics, min_depth));          // etiquette: below threshold, hands off

        {
            const auto in_task = reg.scope(0);                  // worker 0 enters a long task
            CHECK(reg.scan_stalls(t0, tolerance).busy == 1);
            CHECK(!reg.stalled(0));
            (void)reg.scan_stalls(t0 + 9ms, tolerance);
            CHECK(!reg.stalled(0));                             // just under tolerance
            CHECK(!reg.steal(1, out, metrics, min_depth));
            (void)reg.scan_stalls(t0 + 10ms, tolerance);
            CHECK(reg.stalled(0));                              // exactly at tolerance
            CHECK(reg.profile(0).times_stalled == 1);
            (void)reg.scan_stalls(t0 + 40ms, tolerance);
            CHECK(reg.profile(0).times_stalled == 1);           // counted once per stall, not per scan

            int rescued = 0;
            while (reg.take_local(1, out, metrics) || reg.steal(1, out, metrics, min_depth)) { out.invoke(); ++rescued; }
            CHECK(rescued == 5);                                // everything, including the boundary item
            CHECK(ran.load() == 5);
            CHECK(reg.profile(1).hostage_stolen == 5);
        }
        CHECK(!reg.stalled(0));                                 // leaving the task clears the flag
    }

    // ---- many short tasks are never "stalled", even if every scan is far apart
    {
        registry_t reg; reg.configure(1);
        auto now = t0;
        for (int i = 0; i < 200; ++i) {
            const auto in_task = reg.scope(0);
            (void)reg.scan_stalls(now, tolerance);
            now += 50ms;                                        // each scan sees a *new* task
        }
        CHECK(!reg.stalled(0));
        CHECK(reg.profile(0).times_stalled == 0);
    }

    // ---- an idle worker (even tick) is never stalled, however long it idles
    {
        registry_t reg; reg.configure(1);
        for (int i = 0; i < 20; ++i) CHECK(reg.scan_stalls(t0 + std::chrono::seconds{i}, tolerance).busy == 0);
        CHECK(!reg.stalled(0));
    }

    // ---- a stalled worker is not counted as an idle peer when sizing steals
    {
        registry_t reg; reg.configure(3);
        fast_t fast; slow_t slow; std::atomic<int> ran{0};
        for (int i = 0; i < 40; ++i) { CHECK(fast.try_push(make_task(ran))); metrics.adjust_mpmc_depth(1); }
        CHECK(reg.drain_global(0, fast, slow, metrics));        // worker 0: 40 tasks
        const auto wedged = reg.scope(1);                       // worker 1 wedged with nothing queued
        (void)reg.scan_stalls(t0, tolerance);
        (void)reg.scan_stalls(t0 + 20ms, tolerance);
        CHECK(reg.stalled(1));
        reg.invalidate_idle_estimate();
        D::queued_task out{};
        CHECK(reg.steal(2, out, metrics, min_depth));           // worker 2 steals from the pile...
        // ...as the only idle thief (worker 1 doesn't count): "take half" => 20, not 40/2peers=20 either
        // way, so assert the sizing via the thief's own queue depth instead of exact numbers:
        std::size_t got = 1;
        D::queued_task more{};
        while (reg.take_local(2, more, metrics)) ++got;
        CHECK(got >= 19 && got <= 21);
    }
}

// ----------------------------------------------------------------- part 2

struct trial_result { double rescue_ms{}; bool all_done{}; };

trial_result run_trial(bool watchdog_on) {
    W::pool_service<> pool;
    pool.concurrency(2).execution_policy(W::execution::dedicated_poller);
    if (watchdog_on) pool.watchdog_interval(2ms).watchdog_mark_stall(10ms); else pool.watchdog_interval(0ms);
    pool.start();

    std::atomic<int> small_done{0};
    std::atomic<bool> release{false};
    constexpr int smalls = 5;
    // Enqueued last so it is the *newest* item in whichever deque drains them and
    // the owner pops it first (owners pop the bottom) -- the smalls are left behind it.
    for (int i = 0; i < smalls; ++i) (void)pool.enqueue([&small_done] { small_done.fetch_add(1); });
    (void)pool.enqueue([&release] { while (!release.load(std::memory_order_acquire)) std::this_thread::sleep_for(1ms); });

    const auto start = std::chrono::steady_clock::now();
    trial_result r;
    while (std::chrono::steady_clock::now() - start < 600ms) {
        if (small_done.load() == smalls) { r.all_done = true; break; }
        std::this_thread::sleep_for(500us);
    }
    r.rescue_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
    release.store(true, std::memory_order_release);
    pool.stop();
    return r;
}

void part2_pool() {
    std::printf("part 2: end to end (2 workers; one wedged for up to 600ms, tolerance 10ms)\n");
    constexpr int trials = 15;
    int ok = 0; double worst = 0;
    for (int i = 0; i < trials; ++i) {
        const auto r = run_trial(true);
        ok += r.all_done ? 1 : 0;
        worst = std::max(worst, r.rescue_ms);
    }
    std::printf("  watchdog ON : %d/%d trials rescued everything, worst %.1f ms\n", ok, trials, worst);
    CHECK(ok == trials);
    CHECK(worst < 300.0);

    int stuck = 0; double worst_off = 0;
    for (int i = 0; i < trials; ++i) {
        const auto r = run_trial(false);
        stuck += r.all_done ? 0 : 1;
        worst_off = std::max(worst_off, r.rescue_ms);
    }
    std::printf("  watchdog OFF: %d/%d trials left the small pile stuck behind the wedged worker (control, not asserted)\n", stuck, trials);
}

} // namespace

int main() {
    part1_registry();
    part2_pool();
    std::printf(failures ? "stall_rescue: %d FAILED\n" : "stall_rescue: all passed\n", failures);
    return failures ? 1 : 0;
}
