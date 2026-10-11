#pragma once

// Total-stall shed support for admission_gate (see pool_service_shed.inl). Only compiled with
// FCS_EXPERIMENTAL_ALWAYS_ON.

#include <memory>
#include <utility>

namespace FCS::Worker::detail {

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    template<typename Room, typename Put>
    std::size_t admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::evacuate_pending(scheduler_metrics& metrics, Room&& room, Put&& put) {
        // Caller holds claim_refill(): pending_ is ours alone.
        std::size_t taken = 0;
        for (std::size_t i = 0; i < pending_count_; ++i) {
            auto& slot = pending_[i];
            if (!slot) continue;
            if (!room()) break;
            put(std::move(*slot));
            slot.reset();                       // optional's move leaves it engaged: reset it ourselves
            metrics.adjust_cushion_depth(-1);   // staged tasks were still counted in the cushion
            ++taken;
        }
        std::size_t remaining = 0;
        for (std::size_t i = 0; i < pending_count_; ++i) {
            if (!pending_[i]) continue;
            if (remaining != i) { pending_[remaining] = std::move(pending_[i]); pending_[i].reset(); }
            ++remaining;
        }
        pending_count_ = remaining;
        return taken;
    }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    template<typename Room, typename Put>
    std::size_t admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::evacuate_cushion(scheduler_metrics& metrics, Room&& room, Put&& put) {
        QueuedTask next{};
        std::size_t taken = 0;
        while (room() && cushion_.try_pop(next)) {
            put(std::move(next));
            metrics.adjust_cushion_depth(-1);
            ++taken;
        }
        refresh(metrics);
        return taken;
    }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    bool admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::readmit(QueuedTask& work, scheduler_metrics& metrics) noexcept {
        if (!cushion_.try_push(std::move(work))) return false;
        metrics.adjust_cushion_depth(1);
        if (metrics.cushion_depth() >= full_threshold_) admission_.store(queue_state::full, std::memory_order_release);
        refresh(metrics);
        return true;
    }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    void admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::reset_after_clone() noexcept {
        std::construct_at(&cushion_);
        std::construct_at(&pending_);
        pending_count_ = 0;
        refilling_.store(false, std::memory_order_relaxed);
        cushion_state_.store(queue_state::empty, std::memory_order_relaxed);
        admission_.store(queue_state::available, std::memory_order_relaxed);
    }

}
