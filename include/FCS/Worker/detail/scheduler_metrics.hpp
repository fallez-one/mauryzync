#pragma once

#include "../experimental.hpp"
#include "../types.hpp"

#include <atomic>
#include <cstdint>

namespace FCS::Worker::detail {

    class scheduler_metrics {
    public:
        void record_submit(source_kind source) noexcept;
        void record_rejected() noexcept;
        void record_completed() noexcept;
        void adjust_active(std::int64_t delta) noexcept;
        void adjust_cushion_depth(std::int64_t delta) noexcept;
        void adjust_mpmc_depth(std::int64_t delta) noexcept;
        void adjust_local_depth(std::int64_t delta) noexcept;
        void record_refill() noexcept;
        void record_drain() noexcept;
        void record_steal() noexcept;

        [[nodiscard]] std::uint64_t cushion_depth() const noexcept;
        [[nodiscard]] std::uint64_t mpmc_depth() const noexcept;
        [[nodiscard]] std::uint64_t local_depth() const noexcept;

        [[nodiscard]] scheduler_snapshot snapshot(queue_state cushion, queue_state mpmc, queue_state admission) const noexcept;

#if FCS_EXPERIMENTAL_ALWAYS_ON
        // In a clone: nothing is queued and nothing is running any more, whatever a wedged
        // worker's half-finished bookkeeping left in the gauges. Cumulative counters are kept.
        void reset_gauges() noexcept {
            active_.store(0, std::memory_order_relaxed);
            cushion_depth_.store(0, std::memory_order_relaxed);
            mpmc_depth_.store(0, std::memory_order_relaxed);
            local_depth_.store(0, std::memory_order_relaxed);
        }
#endif

    private:
        alignas(64) std::atomic_uint64_t submitted_{}, completed_{}, active_{}, rejected_{};
        alignas(64) std::atomic_uint64_t synthetic_{}, io_{}, network_{};
        alignas(64) std::atomic_uint64_t cushion_depth_{}, mpmc_depth_{}, local_depth_{};
        alignas(64) std::atomic_uint64_t refills_{}, drains_{}, steals_{};
    };

}

#include "scheduler_metrics.inl"
