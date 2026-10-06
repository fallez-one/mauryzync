#pragma once

namespace FCS::Worker::detail {

    inline void scheduler_metrics::record_submit(source_kind source) noexcept {
        submitted_.fetch_add(1, std::memory_order_relaxed);
        switch (source) {
            case source_kind::synthetic: synthetic_.fetch_add(1, std::memory_order_relaxed); break;
            case source_kind::io: io_.fetch_add(1, std::memory_order_relaxed); break;
            case source_kind::network:
            case source_kind::datagram: network_.fetch_add(1, std::memory_order_relaxed); break;
        }
    }

    inline void scheduler_metrics::record_rejected() noexcept { rejected_.fetch_add(1, std::memory_order_relaxed); }
    inline void scheduler_metrics::record_completed() noexcept { completed_.fetch_add(1, std::memory_order_relaxed); }

    inline void scheduler_metrics::adjust_active(std::int64_t delta) noexcept {
        delta >= 0 ? (void)active_.fetch_add(static_cast<std::uint64_t>(delta), std::memory_order_relaxed)
                   : (void)active_.fetch_sub(static_cast<std::uint64_t>(-delta), std::memory_order_relaxed);
    }
    inline void scheduler_metrics::adjust_cushion_depth(std::int64_t delta) noexcept {
        delta >= 0 ? (void)cushion_depth_.fetch_add(static_cast<std::uint64_t>(delta), std::memory_order_relaxed)
                   : (void)cushion_depth_.fetch_sub(static_cast<std::uint64_t>(-delta), std::memory_order_relaxed);
    }
    inline void scheduler_metrics::adjust_mpmc_depth(std::int64_t delta) noexcept {
        delta >= 0 ? (void)mpmc_depth_.fetch_add(static_cast<std::uint64_t>(delta), std::memory_order_relaxed)
                   : (void)mpmc_depth_.fetch_sub(static_cast<std::uint64_t>(-delta), std::memory_order_relaxed);
    }
    inline void scheduler_metrics::adjust_local_depth(std::int64_t delta) noexcept {
        delta >= 0 ? (void)local_depth_.fetch_add(static_cast<std::uint64_t>(delta), std::memory_order_relaxed)
                   : (void)local_depth_.fetch_sub(static_cast<std::uint64_t>(-delta), std::memory_order_relaxed);
    }

    inline void scheduler_metrics::record_refill() noexcept { refills_.fetch_add(1, std::memory_order_relaxed); }
    inline void scheduler_metrics::record_drain() noexcept { drains_.fetch_add(1, std::memory_order_relaxed); }
    inline void scheduler_metrics::record_steal() noexcept { steals_.fetch_add(1, std::memory_order_relaxed); }

    inline std::uint64_t scheduler_metrics::cushion_depth() const noexcept { return cushion_depth_.load(std::memory_order_relaxed); }
    inline std::uint64_t scheduler_metrics::mpmc_depth() const noexcept { return mpmc_depth_.load(std::memory_order_relaxed); }
    inline std::uint64_t scheduler_metrics::local_depth() const noexcept { return local_depth_.load(std::memory_order_relaxed); }

    inline scheduler_snapshot scheduler_metrics::snapshot(queue_state cushion, queue_state mpmc, queue_state admission) const noexcept {
        return scheduler_snapshot{
            submitted_.load(std::memory_order_relaxed), completed_.load(std::memory_order_relaxed),
            active_.load(std::memory_order_relaxed), rejected_.load(std::memory_order_relaxed),
            synthetic_.load(std::memory_order_relaxed), io_.load(std::memory_order_relaxed), network_.load(std::memory_order_relaxed),
            cushion_depth_.load(std::memory_order_relaxed), mpmc_depth_.load(std::memory_order_relaxed), local_depth_.load(std::memory_order_relaxed),
            refills_.load(std::memory_order_relaxed), drains_.load(std::memory_order_relaxed), steals_.load(std::memory_order_relaxed),
            cushion, mpmc, admission,
        };
    }

}
