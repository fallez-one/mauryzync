#pragma once

// pool_service: total-stall poll + runtime shed. Only compiled with FCS_EXPERIMENTAL_ALWAYS_ON.
// The contract is written up at watchdog_total_stall_poll() in pool_service.hpp; this file is
// the mechanism.

#include <algorithm>
#include <chrono>
#include <memory>
#include <new>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace FCS::Worker {

    // ---- configuration -------------------------------------------------------------------

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::watchdog_total_stall_poll(std::chrono::nanoseconds window, std::size_t quorum) noexcept {
        shed_quorum_.store(quorum == 0 ? 1 : quorum, std::memory_order_relaxed);
        shed_window_ns_.store(window.count() < 0 ? 0 : window.count(), std::memory_order_relaxed);
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::watchdog_finalize_wait(std::chrono::milliseconds wait) noexcept {
        shed_finalize_ms_.store(wait.count() < 0 ? 0 : wait.count(), std::memory_order_relaxed);
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    template<typename F>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::watchdog_on_migrate(F&& fn) {
        if (!running_.load(std::memory_order_relaxed)) migrate_hook_ = migration_hook{std::forward<F>(fn)};
        return *this;
    }

    // ---- worker side ---------------------------------------------------------------------

    // A worker that reaches the top of its loop with the flag up. It must not touch any queue,
    // run anything, or help: it acknowledges, then sleeps until the flag drops (a failed clone was
    // undone -> true: carry on) or the pool is stopping (-> false: leave, so stop() can join it).
    template<std::size_t QueueCapacity, typename Traits>
    bool pool_service<QueueCapacity, Traits>::shed_stand_down(std::size_t id) {
        shed_acked_[id].store(true, std::memory_order_seq_cst);
        for (;;) {
            const auto seen = parking_.epoch();   // sampled before the checks: a wake after this is not slept through
            if (!shedding_migration_.load(std::memory_order_seq_cst)) {
                shed_acked_[id].store(false, std::memory_order_seq_cst);
                return true;
            }
            if (!running_.load(std::memory_order_acquire)) return false;
            parking_.park(id, seen, std::nullopt, [this] {
                return !shedding_migration_.load(std::memory_order_acquire) || !running_.load(std::memory_order_acquire);
            });
        }
    }

    // ---- watchdog side -------------------------------------------------------------------

    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::escalate_total_stall() {
        if (!running_.load(std::memory_order_acquire)) return;   // a normal shutdown, not a wedge
        const auto stopping = [this] { return watchdog_stop_.load(std::memory_order_seq_cst); };
        const std::chrono::nanoseconds window{shed_window_ns_.load(std::memory_order_relaxed)};
        const std::size_t quorum = std::clamp<std::size_t>(shed_quorum_.load(std::memory_order_relaxed), 1, threads_);

        // ---- 1. poll: do `quorum` distinct workers show proof of life inside the window? ----
        // A descheduled thread that gets the CPU back finishes or starts a task, and either moves
        // its tick. Nothing here asks a worker to DO anything: a wedged one could not answer.
        typename registry_type::progress_marks marks;
        registry_.mark_progress(marks);
        const auto poll_start = std::chrono::steady_clock::now();
        auto deadline = poll_start + window;
        resurrection_.polls_opened.fetch_add(1, std::memory_order_relaxed);
        const auto poll_took = [&] {
            resurrection_.last_poll_ns.store(static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - poll_start).count()), std::memory_order_relaxed);
        };
        const std::chrono::nanoseconds slice{std::max<std::int64_t>(watchdog_interval_ns_.load(std::memory_order_relaxed), 1'000'000)};
        for (;;) {
            if (registry_.progressed_since(marks) >= quorum) {            // descheduled, not wedged
                resurrection_.polls_cleared.fetch_add(1, std::memory_order_relaxed);
                poll_took();
                return;
            }
            if (stopping() || !running_.load(std::memory_order_acquire)) return;
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) break;
            const auto asked = std::min<std::chrono::nanoseconds>(slice, deadline - now);
            watchdog_parker_.park(asked, stopping);

            // If THIS thread overslept by a lot, the whole process was probably not running (SIGSTOP, a
            // paused VM, a debugger, a starved container): the workers are as late as we are, and have
            // not yet had a chance to show proof of life. Time nobody could run in is not evidence of a
            // wedge -- credit it back and look again. (A thread merely a little late is just jitter.)
            const auto woke = std::chrono::steady_clock::now();
            const auto late = (woke - now) - asked;
            if (late > std::max<std::chrono::nanoseconds>(4 * slice, std::chrono::milliseconds{4})) {
                deadline += late;
                resurrection_.polls_extended.fetch_add(1, std::memory_order_relaxed);
            }
        }

        // ---- 2. point of no return ----
        poll_took();
        const auto shed_start = std::chrono::steady_clock::now();
        const auto since_shed = [&] { return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - shed_start).count()); };
        resurrection_.sheds_started.fetch_add(1, std::memory_order_relaxed);
        shedding_migration_.store(true, std::memory_order_seq_cst);
        signal_all();                 // a parked worker must see the flag too
        await_stand_down();
        await_submitters();
        evacuate_all();
        resurrection_.last_evacuation_ns.store(since_shed(), std::memory_order_relaxed);
        resurrection_.tasks_evacuated.fetch_add(shed_report_.evacuated(), std::memory_order_relaxed);
        resurrection_.tasks_unreachable.fetch_add(shed_report_.residual_gauge, std::memory_order_relaxed);

        // ---- 3. clone ----
        const auto cloned = resurrector_.try_fork();
        if (!cloned.has_value()) { abort_shed(); resurrection_.last_shed_ns.store(since_shed(), std::memory_order_relaxed); return; }
        if (cloned.value() == detail::fork_role::child) {
            resurrection_.last_shed_ns.store(0, std::memory_order_relaxed);
            resume_in_clone(since_shed());
            return;
        }

        // ---- 4. parent: the tasks live in the clone now ----
        migration_committed_.store(true, std::memory_order_release);
        shed_batch_->abandon();       // never run, never destroyed here
        std::this_thread::sleep_for(std::chrono::milliseconds{shed_finalize_ms_.load(std::memory_order_relaxed)});
        resurrector_.exit(0);
    }

    // Workers that are BETWEEN tasks are about to see the flag: give them a moment to
    // acknowledge, so the sweep below doesn't race a worker that is still taking work. Workers
    // inside a task will not touch a queue until that task returns, and may never return; they
    // cannot be waited for, and the sweep copes with them (every queue operation it uses is
    // already safe against the owner running at the same time).
    // submit() callers that announced themselves before seeing the flag are about to push into a
    // layer; wait them out (bounded: one frozen mid-push cannot be waited for).
    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::await_submitters() {
        constexpr auto cap = std::chrono::milliseconds{2};
        const auto give_up = std::chrono::steady_clock::now() + cap;
        while (submits_in_flight_.load(std::memory_order_seq_cst) != 0 && std::chrono::steady_clock::now() < give_up) std::this_thread::yield();
    }

    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::await_stand_down() {
        constexpr auto cap = std::chrono::milliseconds{2};
        const auto give_up = std::chrono::steady_clock::now() + cap;
        for (std::size_t i = 0; i < threads_; ++i) {
            while (!registry_.in_task(i) && !shed_acked_[i].load(std::memory_order_acquire) && std::chrono::steady_clock::now() < give_up) std::this_thread::yield();
        }
    }

    // Moves everything queued into shed_batch_. Layers are swept oldest-first (deques, fast and slow lanes,
    // refill staging, cushion) and the sweep repeats until a whole pass finds nothing, because a
    // late thief can still hop tasks between deques while it runs. The drain and refill claims are
    // taken first and KEPT, so nothing can slip from a layer not yet swept into one already swept.
    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::evacuate_all() {
        auto& batch = *shed_batch_;
        shed_report_ = {};
        shed_holds_drain_ = registry_.claim_drain();
        shed_holds_refill_ = gate_.claim_refill();
        shed_report_.refill_staging_claimed_elsewhere = !shed_holds_refill_;

        const auto sweep_lane = [&](auto& lane, std::size_t row) {
            queued_task_type next{};
            std::size_t taken = 0;
            while (batch.room(row) && lane.try_pop(next)) {
                (void)batch.put(row, std::move(next));
                metrics_.adjust_mpmc_depth(-1);
                ++taken;
            }
            return taken;
        };

        constexpr unsigned max_passes = 8;   // a bound, not a tuning knob: a pass that finds nothing ends it
        for (unsigned pass = 0; pass < max_passes; ++pass) {
            std::size_t moved = 0;
            for (std::size_t id = 0; id < threads_; ++id) {
                const auto row = batch.local_row(id);
                moved += registry_.evacuate_local(id, metrics_,
                    [&batch, row] { return batch.room(row); },
                    [&batch, row](queued_task_type&& task) { (void)batch.put(row, std::move(task)); });
            }
            moved += sweep_lane(fast_, batch.fast_row());
            moved += sweep_lane(slow_, batch.slow_row());
            if (shed_holds_refill_) {
                const auto row = batch.pending_row();
                moved += gate_.evacuate_pending(metrics_,
                    [&batch, row] { return batch.room(row); },
                    [&batch, row](queued_task_type&& task) { (void)batch.put(row, std::move(task)); });
            }
            {
                const auto row = batch.cushion_row();
                moved += gate_.evacuate_cushion(metrics_,
                    [&batch, row] { return batch.room(row); },
                    [&batch, row](queued_task_type&& task) { (void)batch.put(row, std::move(task)); });
            }
            if (moved == 0) break;
        }

        for (std::size_t id = 0; id < threads_; ++id) shed_report_.evacuated_local += batch.size(batch.local_row(id));
        shed_report_.evacuated_fast = batch.size(batch.fast_row());
        shed_report_.evacuated_slow = batch.size(batch.slow_row());
        shed_report_.evacuated_pending = batch.size(batch.pending_row());
        shed_report_.evacuated_cushion = batch.size(batch.cushion_row());
        // Whatever the gauges still count was not reachable (a row filled up, or a producer is frozen mid-push).
        const auto residual = metrics_.cushion_depth() + metrics_.mpmc_depth() + metrics_.local_depth();
        shed_report_.residual_gauge = residual > (std::uint64_t{1} << 62) ? 0 : residual;   // a wrapped gauge is skew, not a backlog
    }

    // Puts the batch back into queues, oldest first. `direct_local`: the clone, before any worker
    // exists, so nobody else owns a deque and a task can go back into the very deque it came from.
    // Otherwise (undoing a failed clone) the owners are live, so only the shared queues are used.
    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::reinject_all(bool direct_local) {
        auto& batch = *shed_batch_;
        auto& report = shed_report_;

        // The cushion is the front door. If it is physically full, straight into the lane the
        // task belongs to. If that is full too, the caller keeps the task.
        const auto place_shared = [&](queued_task_type& task) {
            if (gate_.readmit(task, metrics_)) { ++report.injected_cushion; return true; }
            const bool placed = task.lane == workload::Fast ? fast_.try_push(std::move(task)) : slow_.try_push(std::move(task));
            if (!placed) return false;
            metrics_.adjust_mpmc_depth(1);
            ++report.injected_mpmc;
            return true;
        };

        for (std::size_t id = 0; id < threads_; ++id) {
            for (auto& task : batch.row(batch.local_row(id))) {
                if (!task.invoke) { ++report.dropped_empty; continue; }
                if (direct_local && registry_.inject_local(id, std::move(task), metrics_)) { ++report.injected_direct; continue; }
                if (!place_shared(task)) ++report.refused;
            }
        }
        for (const auto row : {batch.fast_row(), batch.slow_row(), batch.pending_row(), batch.cushion_row()}) {
            for (auto& task : batch.row(row)) {
                if (!task.invoke) { ++report.dropped_empty; continue; }
                if (!place_shared(task)) ++report.refused;
            }
        }
        batch.clear();   // moved-from shells, and whatever was refused
    }

    // The clone failed (fork: ENOMEM / EAGAIN / process limit; NT: no RtlCloneUserProcess). Nothing was
    // handed over, so give everything back and let the pool carry on.
    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::abort_shed() {
        reinject_all(/*direct_local=*/false);
        resurrection_.sheds_aborted.fetch_add(1, std::memory_order_relaxed);
        resurrection_.tasks_reinjected.fetch_add(shed_report_.injected_cushion + shed_report_.injected_mpmc, std::memory_order_relaxed);
        resurrection_.tasks_refused.fetch_add(shed_report_.refused, std::memory_order_relaxed);
        if (shed_holds_refill_) gate_.release_refill();
        if (shed_holds_drain_) registry_.release_drain();
        shed_holds_refill_ = shed_holds_drain_ = false;
        shedding_migration_.store(false, std::memory_order_seq_cst);
        signal_all();   // stood-down workers wake, see the flag down, and resume
    }

    // ---- the clone -----------------------------------------------------------------------

    // Runs on the clone's ONLY thread: the one that was the watchdog. Everything the other
    // threads were doing is frozen in this copy of memory, possibly half-done, and none of them
    // will ever run again -- so state is rebuilt, never repaired, and the old objects are leaked
    // rather than destroyed. Nothing here may wait on anything a vanished thread could have held.
    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::resume_in_clone(std::uint64_t shed_ns) noexcept {
        // The std::thread objects name threads that do not exist in this process, and
        // destroying a joinable one terminates: park them in a heap block nobody frees.
        (void)new std::vector<std::thread>(std::move(workers_));
        workers_.clear();
        (void)new std::thread(std::move(watchdog_));   // that is this very thread; it is the watchdog still

        registry_.reset_after_clone();
        metrics_.reset_gauges();
        fast_.reset_after_clone();
        std::construct_at(&slow_);
        gate_.reset_after_clone();
        timers_.release_lock_after_clone();   // pending timers (and their deadlines) survive the clone
        poll_registry_.clear_busy_after_clone();
        active_pollers_.store(0, std::memory_order_relaxed);
        submits_in_flight_.store(0, std::memory_order_relaxed);   // submitters frozen in the parent are not coming back
        std::construct_at(&parking_);
        parking_.configure(threads_);
        watchdog_parked_.store(false, std::memory_order_relaxed);
        for (auto& acked : shed_acked_) acked.store(false, std::memory_order_relaxed);
        shed_holds_drain_ = shed_holds_refill_ = false;
        migration_committed_.store(false, std::memory_order_relaxed);

        // The pool accepts work again from here, so the hook may enqueue(). (Workers do not exist
        // yet; whatever it enqueues is ahead of the migrated cushion tasks but behind deque ones.)
        shedding_migration_.store(false, std::memory_order_seq_cst);

        if (migrate_hook_) {
            migration_context context{*shed_batch_, shed_report_};
            try { migrate_hook_(context); } catch (...) {}   // boundary: a throwing hook must not cost the clone its tasks
        }

        reinject_all(/*direct_local=*/true);
        resurrection_.generation.fetch_add(1, std::memory_order_relaxed);
        resurrection_.resurrections.fetch_add(1, std::memory_order_relaxed);
        resurrection_.tasks_reinjected.fetch_add(shed_report_.injected_direct + shed_report_.injected_cushion + shed_report_.injected_mpmc, std::memory_order_relaxed);
        resurrection_.tasks_refused.fetch_add(shed_report_.refused, std::memory_order_relaxed);
        launch_workers();
        resurrection_.last_shed_ns.store(shed_ns, std::memory_order_relaxed);   // up to the clone; the hook + re-injection are the clone's own cost
    }

    // stop() after the parent committed: it must neither wait for a wedged worker (it would
    // never come back) nor tear a std::thread down joinable (that terminates). Workers that
    // acknowledged the flag are the late, merely-descheduled ones -- they join as usual.
    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::stop_after_commit() noexcept {
        for (std::size_t i = 0; i < workers_.size(); ++i) {
            auto& thread = workers_[i];
            if (!thread.joinable()) continue;
            if (i < shed_acked_.size() && shed_acked_[i].load(std::memory_order_acquire)) thread.join();
            else thread.detach();
        }
        workers_.clear();
        watchdog_stop_.store(true, std::memory_order_seq_cst);
        (void)watchdog_parker_.unpark();
        if (watchdog_.joinable()) watchdog_.detach();   // may be the very thread that is exiting the process
    }

}
