#pragma once

#include "mpmc_queue.hpp"
#include "queue_classifier.hpp"
#include "scheduler_metrics.hpp"
#include "task.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <optional>

namespace FCS::Worker::detail {

    // Everything submitted lands here first. Depth against configurable thresholds
    // decides whether the caller sees admission::full and future submits are rejected.
    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask = queued_task>
    class admission_gate {
    public:
        void configure(std::size_t full, std::size_t available, std::size_t candidates) noexcept;

        // Admission slots only normal-priority work may use. High-priority submits are
        // refused once the cushion holds `full_threshold - reserve` items, so a flood of
        // high can never fill the cushion and shut normal out at the door -- the aging
        // in priority_mpmc_queue only reorders work that was *admitted*, it cannot help
        // a task that is rejected before it gets in. `kAuto` (the default) reserves
        // max(1, full/16); 0 disables the reserve (high may use every slot, as before).
        static constexpr std::size_t auto_reserve = static_cast<std::size_t>(-1);
        void reserve_for_normal(std::size_t slots) noexcept { normal_reserve_ = slots; }
        [[nodiscard]] std::size_t high_priority_limit() const noexcept;

        [[nodiscard]] bool submit(QueuedTask work, scheduler_metrics& metrics) noexcept;
        void refresh(scheduler_metrics& metrics) noexcept;

        // `FastQueue` is whatever pool_service's fast_ actually is (a plain
        // bounded_mpmc_queue or, with the priority classification, a
        // priority_mpmc_queue) -- this only ever calls try_push on it, so
        // either works with no change here.
        template<typename FastQueue>
        bool refill(scheduler_metrics& metrics, FastQueue& fast, bounded_mpmc_queue<QueuedTask, SlowCapacity>& slow);

        [[nodiscard]] queue_state cushion_state() const noexcept;
        [[nodiscard]] queue_state admission_state() const noexcept;

    private:
        bounded_mpmc_queue<QueuedTask, CushionCapacity> cushion_;
        std::array<std::optional<QueuedTask>, CushionCapacity> pending_{};
        std::size_t pending_count_{};

        std::atomic<queue_state> cushion_state_{queue_state::empty};
        std::atomic<queue_state> admission_{queue_state::available};
        std::atomic_bool refilling_{};

        std::size_t full_threshold_{CushionCapacity - 1};
        std::size_t available_threshold_{CushionCapacity / 8}; // == old FastCapacity/4, since pool_service sets CushionCapacity = FastCapacity*2
        std::size_t candidate_budget_{80};
        std::size_t normal_reserve_{auto_reserve};
    };

}

#include "admission_gate.inl"
