#pragma once

#include <thread>
#include <utility>

namespace FCS::Worker::detail {

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    void worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::configure(std::size_t threads) noexcept {
        threads_ = threads;
        if (!ttl_overridden_) idle_.configure(threads < 4 ? 4 : threads);
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    void worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::idle_cache_ttl(std::size_t ttl) noexcept {
        ttl_overridden_ = true;
        idle_.configure(ttl);
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    std::size_t worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::thread_count() const noexcept { return threads_; }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    bool worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::requeue_behind(std::size_t id, QueuedTask&& task, std::size_t behind, scheduler_metrics& metrics) {
        auto& local = locals_[id];
        if (local.depth.load(std::memory_order_relaxed) >= LocalCapacity) return false; // bounded: no growing past the cap
        if (behind > max_requeue_behind) behind = max_requeue_behind;

        const auto push = [&](QueuedTask&& item) {
            local.queue.push_bottom(std::move(item), hazards_);
            local.depth.fetch_add(1, std::memory_order_relaxed);
            metrics.adjust_local_depth(1);
        };

        // Lift off the tasks the owner would have popped next. held[0] is the very next one.
        std::array<QueuedTask, max_requeue_behind> held{};
        std::size_t lifted = 0;
        while (lifted < behind && take_local(id, held[lifted], metrics)) ++lifted;

        // The only push that can need a fresh segment while the lifted tasks' slots are free
        // is this one, so a failure here is recoverable: put them back (guaranteed room) and
        // report "not requeued". (A failure later, re-pushing the tasks into the slot this one
        // took, is a bad_alloc on a few-hundred-byte segment -- the same exposure drain_global
        // has, and treated the same way: not survivable.)
        try {
            push(std::move(task));
        } catch (...) {
            for (std::size_t i = lifted; i-- > 0;) push(std::move(held[i]));
            return false;
        }
        for (std::size_t i = lifted; i-- > 0;) push(std::move(held[i])); // reversed, so held[0] ends up on the bottom again
        return true;
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    bool worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::take_local(std::size_t id, QueuedTask& out, scheduler_metrics& metrics) noexcept {
        if (!locals_[id].queue.pop_bottom(out, hazards_)) return false;
        locals_[id].depth.fetch_sub(1, std::memory_order_relaxed);
        metrics.adjust_local_depth(-1);
        return true;
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    void worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::begin_task(std::size_t id) noexcept {
        auto& tick = locals_[id].tick;
        tick.store(tick.load(std::memory_order_relaxed) + 1, std::memory_order_release); // -> odd
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    void worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::end_task(std::size_t id) noexcept {
        auto& slot = locals_[id];
        slot.tick.store(slot.tick.load(std::memory_order_relaxed) + 1, std::memory_order_release); // -> even
        // Recover promptly instead of waiting for the next watchdog pass. Only a
        // load on the (usual) not-stalled path; if the watchdog flags us a hair
        // after this, its next pass sees an even tick and clears it again.
        if (slot.stalled.load(std::memory_order_relaxed)) slot.stalled.store(false, std::memory_order_relaxed);
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    typename worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::stall_scan worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::scan_stalls(std::chrono::steady_clock::time_point now, std::chrono::nanoseconds tolerance) noexcept {
        stall_scan result{};
        for (std::size_t i = 0; i < threads_; ++i) {
            auto& slot = locals_[i];
            auto& watch = watch_[i];
            const auto tick = slot.tick.load(std::memory_order_acquire);
            if ((tick & 1u) == 0) { // between tasks: cannot be stalled
                watch.tick = tick;
                watch.since = now;
                if (slot.stalled.load(std::memory_order_relaxed)) slot.stalled.store(false, std::memory_order_relaxed);
                continue;
            }
            ++result.busy;
            if (tick != watch.tick) { // a task we haven't seen before: start its clock now
                watch.tick = tick;
                watch.since = now;
                if (slot.stalled.load(std::memory_order_relaxed)) slot.stalled.store(false, std::memory_order_relaxed);
            } else if (now - watch.since >= tolerance && !slot.stalled.load(std::memory_order_relaxed)) {
                slot.stalled.store(true, std::memory_order_relaxed);
                slot.profile.record_stalled();
                ++result.newly_stalled;
            }
            if (slot.stalled.load(std::memory_order_relaxed)) ++result.stalled;
        }
        return result;
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    bool worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::steal_single(std::size_t victim, std::size_t thief_id, QueuedTask& out, scheduler_metrics& metrics) noexcept {
        auto& victim_slot = locals_[victim];
        if (!victim_slot.queue.steal_one(out, thief_id, hazards_)) return false;
        victim_slot.depth.fetch_sub(1, std::memory_order_relaxed);
        metrics.adjust_local_depth(-1);
        victim_slot.profile.record_incoming_single();
        locals_[thief_id].profile.record_outgoing_single();
        return true;
    }

    // Thief-driven, negotiation-free batch steal. The thief never contacts the
    // owner: it estimates how many peers are also idle right now (best-effort,
    // relaxed reads), picks a victim with real surplus, and reserves a share of
    // that surplus for itself in one CAS-bounded steal_batch call. The owner is
    // never involved and never blocked. If the batch attempt comes up empty
    // (contended victim, or a stale estimate), the classic single-item try_pop
    // path is still there as a fallback -- it is never discarded, just no longer
    // the primary mechanism.
    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    bool worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::steal(std::size_t id, QueuedTask& out, scheduler_metrics& metrics, std::size_t minimum_depth) noexcept {
        auto& thief_slot = locals_[id];

        // Lazily resolved, at most once per steal() call, and only when a victim
        // actually has surplus to size a batch against. Most steal() calls on a
        // quiet pool find nothing worth taking and now never touch the estimate
        // at all; the ones that do share a TTL-bounded answer (idle_estimate.hpp)
        // instead of each paying an O(threads) scan of every worker's depth.
        bool estimate_resolved = false;
        std::size_t other_idle = 0;
        auto resolve_other_idle = [&]() noexcept {
            estimate_resolved = true;
            const auto idle = idle_.get([this, minimum_depth]() noexcept {
                std::size_t n = 0;
                for (std::size_t i = 0; i < threads_; ++i) {
                    // A stalled worker has little queued but is NOT about to finish and
                    // go thieving, so it is not counted as an idle peer.
                    if (locals_[i].depth.load(std::memory_order_relaxed) < minimum_depth
                        && !locals_[i].stalled.load(std::memory_order_relaxed)) ++n;
                }
                return n;
            });
            switch (idle.from) {
            case idle_estimate::source::cached: thief_slot.profile.record_idle_cache_hit(); break;
            case idle_estimate::source::scanned: thief_slot.profile.record_idle_scan(); break;
            case idle_estimate::source::stale_while_refreshing: thief_slot.profile.record_idle_stale_reuse(); break;
            }
            // The estimate counts every peer (including this thief) that looks
            // about to finish anyway; other_idle excludes the thief itself.
            other_idle = idle.idle > 0 ? idle.idle - 1 : 0;
        };

        for (std::size_t offset = 1; offset < threads_; ++offset) {
            const auto victim = (id + offset) % threads_;
            auto& victim_slot = locals_[victim];

            thief_slot.profile.record_outgoing_attempt();
            thief_slot.profile.record_probe_of(victim); // thief-owned: no shared write on a failed probe

            const auto overall_depth = victim_slot.depth.load(std::memory_order_relaxed);
            // Normal etiquette: a victim below the threshold is about to go idle
            // itself, leave it alone. Unless it is stalled (stuck in one long task,
            // see scan_stalls()) -- then whatever it holds is hostage, and any depth
            // is fair game.
            const bool hostage = victim_slot.stalled.load(std::memory_order_relaxed);
            if (hostage ? overall_depth == 0 : overall_depth < minimum_depth) continue;

            if (!estimate_resolved) resolve_other_idle();

            std::size_t requested_work;
            if (other_idle == 0 && hostage) {
                // Sole thief against a stalled victim: its owner is not coming back
                // soon, so there is nothing to leave behind -- take it all.
                requested_work = overall_depth;
            } else if (other_idle == 0) {
                // Thief is the only idle worker: take half of a substantial pile,
                // or greedily take the whole stealable surplus of a small one --
                // splitting a small pile in half leaves everyone starved.
                if (overall_depth > minimum_depth * 2) requested_work = overall_depth / 2;
                else if (overall_depth > minimum_depth) requested_work = overall_depth - minimum_depth;
                else requested_work = overall_depth;
            } else {
                // Reserve roughly a fair share against the other idle peers that
                // might also come asking. Best-effort, not mathematical perfection.
                requested_work = overall_depth / other_idle;
            }
            if (requested_work == 0) requested_work = 1;

            const auto claimed = victim_slot.queue.steal_batch(requested_work, thief_slot.queue, id, hazards_);
            if (claimed > 0) {
                thief_slot.depth.fetch_add(claimed, std::memory_order_relaxed);
                victim_slot.depth.fetch_sub(claimed, std::memory_order_relaxed);
                victim_slot.profile.record_incoming_batch(claimed);
                thief_slot.profile.record_outgoing_batch(claimed);
                metrics.record_steal();
                if (hostage) thief_slot.profile.record_hostage_stolen(claimed);
                if (take_local(id, out, metrics)) return true;
                // Batch vanished under us between claiming and popping (should not
                // normally happen) -- fall through to the direct single-item path.
            }

            if (steal_single(victim, id, out, metrics)) {
                metrics.record_steal();
                if (hostage) thief_slot.profile.record_hostage_stolen(1);
                return true;
            }
        }
        return false;
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    worker_steal_snapshot worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::profile(std::size_t id) const noexcept {
        auto snapshot = locals_[id].profile.snapshot();
        // "Incoming attempts" for this worker = every other worker's probe count of it.
        // Summed here, at read time, so recording a probe never touches the victim.
        for (std::size_t thief = 0; thief < threads_; ++thief) snapshot.incoming_attempts += locals_[thief].profile.probes_of(id);
        return snapshot;
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    template<typename FastQueue, std::size_t SlowCapacity>
    bool worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::drain_global(
        std::size_t owner, FastQueue& fast,
        bounded_mpmc_queue<QueuedTask, SlowCapacity>& slow, scheduler_metrics& metrics) {
        bool expected = false;
        if (!draining_.compare_exchange_strong(expected, true)) return false;
        metrics.record_drain();

        auto& local = locals_[owner];
        std::size_t moved{};
        auto drain_lane = [&](auto& global) {
            QueuedTask item{};
            while (local.depth.load(std::memory_order_relaxed) < LocalCapacity) {
                if (!global.try_pop(item)) break;
                ++moved;
                local.queue.push_bottom(std::move(item), hazards_);
                local.depth.fetch_add(1, std::memory_order_relaxed);
                metrics.adjust_mpmc_depth(-1);
                metrics.adjust_local_depth(1);
            }
        };

        drain_lane(fast);
        drain_lane(slow);

        draining_.store(false, std::memory_order_release);
        return moved != 0; // progress, not "won the claim" (see admission_gate::refill)
    }

}
