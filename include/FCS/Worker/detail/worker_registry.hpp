#pragma once

#include "hazard_domain.hpp"
#include "idle_estimate.hpp"
#include "mpmc_queue.hpp"
#include "scheduler_metrics.hpp"
#include "segmented_deque.hpp"
#include "task.hpp"
#include "worker_profile.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace FCS::Worker::detail {

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask = queued_task>
    class worker_registry {
    public:
        void configure(std::size_t threads) noexcept;

        // Uses a cached idle-peer estimate may serve before it is rescanned
        // (use-counter TTL, see idle_estimate.hpp). Default: max(threads, 4),
        // which amortizes the O(threads) scan to O(1) per steal attempt.
        // 0 disables the cache (every steal rescans, the pre-cache behaviour).
        // Call before workers start.
        void idle_cache_ttl(std::size_t ttl) noexcept;

        // ---- stall watchdog support (see pool_service::watchdog_mark_stall) ----
        //
        // A worker that is stuck inside one long task cannot run its own local
        // deque, but thieves normally refuse to touch a victim below
        // `minimum_depth` (it "will finish soon anyway"). That etiquette is
        // wrong for a victim that is *not* going to finish soon. A stalled
        // victim is therefore fair game at any depth.
        //
        // Detection is progress-based, not clock-based, so workers never read a
        // clock: each worker bumps its own tick counter when it enters a task
        // (odd) and leaves it (even), two relaxed stores to its own cache line.
        // Whoever scans (the watchdog thread; a test can call it by hand) keeps the
        // time: a tick that is odd and *unchanged* since the previous look means
        // the same task is still running.
        //
        // Marks the calling worker as inside a task for the lifetime of the scope
        // (RAII, exception-safe).
        class task_scope {
        public:
            task_scope(worker_registry& registry, std::size_t id) noexcept : registry_(registry), id_(id) { registry_.begin_task(id_); }
            ~task_scope() { registry_.end_task(id_); }
            task_scope(const task_scope&) = delete;
            task_scope& operator=(const task_scope&) = delete;
        private:
            worker_registry& registry_;
            std::size_t id_;
        };
        [[nodiscard]] task_scope scope(std::size_t id) noexcept { return task_scope{*this, id}; }

        void begin_task(std::size_t id) noexcept;
        void end_task(std::size_t id) noexcept;

        struct stall_scan {
            std::size_t busy{};           // workers currently inside a task
            std::size_t newly_stalled{};  // workers this pass flagged for the first time
        };

        // One watchdog pass at time `now`. Flags workers whose current task has
        // been observed running for at least `tolerance` (so detection lands
        // between tolerance and tolerance + the scan interval after the task
        // started), and un-flags workers that moved on. `busy` lets the caller park when
        // it is zero; `newly_stalled` tells it thieves may now have something to
        // do, i.e. it should wake any that are asleep (a sleeping thief never
        // looks at the flag).
        // Single caller at a time (the watchdog thread): per-worker observation
        // state is plain, unsynchronised memory.
        [[nodiscard]] stall_scan scan_stalls(std::chrono::steady_clock::time_point now, std::chrono::nanoseconds tolerance) noexcept;
        [[nodiscard]] bool stalled(std::size_t id) const noexcept { return locals_[id].stalled.load(std::memory_order_relaxed); }

        // Owner-only. Puts `task` into worker `id`'s OWN deque so that the owner will pop up to
        // `behind` other tasks before it -- the continuation of a coroutine that is yielding.
        // Deliberately not a global-queue push and not an inbox: it needs nobody's cooperation
        // and nothing unbounded. The owner pops its deque from the bottom, so the tasks it would
        // have run next are lifted off, the continuation goes under them, and they go back on
        // top in their original order; thieves, who take from the head, can still take the
        // continuation without the owner's consent if the owner stays stalled. Bounded: refuses
        // (returns false, task untouched) when the deque is already at LocalCapacity.
        static constexpr std::size_t max_requeue_behind = 16;
        [[nodiscard]] bool requeue_behind(std::size_t id, QueuedTask&& task, std::size_t behind, scheduler_metrics& metrics);

        // Handed to the worker thread so coroutines running on it can see the watchdog's flag and
        // record cooperative yields (see detail/coop.hpp). Stable for the registry's lifetime.
        [[nodiscard]] const std::atomic_bool& stalled_flag(std::size_t id) const noexcept { return locals_[id].stalled; }
        [[nodiscard]] worker_steal_profile& profile_ref(std::size_t id) noexcept { return locals_[id].profile; }

        // Diagnostics only: a relaxed snapshot of one worker, for pool_service::debug_dump().
        struct worker_state {
            std::size_t depth{};
            std::uint64_t tick{};   // odd == inside a task (or poll pass / timer callback)
            bool stalled{};
        };
        [[nodiscard]] worker_state debug_state(std::size_t id) const noexcept {
            return {locals_[id].depth.load(std::memory_order_relaxed), locals_[id].tick.load(std::memory_order_relaxed),
                    locals_[id].stalled.load(std::memory_order_relaxed)};
        }

        // The idle population changed (a worker is parking / has woken): the
        // cached estimate no longer reflects it, rescan on the next steal.
        void invalidate_idle_estimate() noexcept { idle_.invalidate(); }
        [[nodiscard]] std::size_t thread_count() const noexcept;

        [[nodiscard]] bool take_local(std::size_t id, QueuedTask& out, scheduler_metrics& metrics) noexcept;
        [[nodiscard]] bool steal(std::size_t id, QueuedTask& out, scheduler_metrics& metrics, std::size_t minimum_depth) noexcept;

        template<typename FastQueue, std::size_t SlowCapacity>
        bool drain_global(std::size_t owner, FastQueue& fast,
                           bounded_mpmc_queue<QueuedTask, SlowCapacity>& slow, scheduler_metrics& metrics);

        // Per-worker steal telemetry — see worker_profile.hpp for field meanings.
        [[nodiscard]] worker_steal_snapshot profile(std::size_t id) const noexcept;

    private:
        static constexpr std::size_t segment_capacity = 128;
        using local_queue = segmented_deque<QueuedTask, segment_capacity, MaxWorkers>;
        using hazards_type = hazard_domain<MaxWorkers>;

        struct worker_slot {
            local_queue queue;
            std::atomic_size_t depth{};
            worker_steal_profile profile;
            // Own cache lines: `tick` is written by the owner on every task,
            // `stalled` is read by thieves and almost never written -- keeping
            // them apart stops thieves' reads from bouncing the owner's line.
            alignas(64) std::atomic<std::uint64_t> tick{0};     // odd == inside a task
            alignas(64) std::atomic_bool stalled{false};
        };

        // Watchdog-private observation state (see scan_stalls()).
        struct watch_state {
            std::uint64_t tick{};
            std::chrono::steady_clock::time_point since{};
        };
        std::array<watch_state, MaxWorkers> watch_{};

        [[nodiscard]] bool steal_single(std::size_t victim, std::size_t thief_id, QueuedTask& out, scheduler_metrics& metrics) noexcept;

        std::array<worker_slot, MaxWorkers> locals_{};
        hazards_type hazards_{};
        idle_estimate idle_;
        bool ttl_overridden_{false};
        std::atomic_bool draining_{};
        std::size_t threads_{1};
    };

}

#include "worker_registry.inl"
