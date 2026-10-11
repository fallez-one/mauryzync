#pragma once

// Total-stall shed support for worker_registry (see pool_service_shed.inl). Only compiled with
// FCS_EXPERIMENTAL_ALWAYS_ON.

#include <memory>
#include <utility>

namespace FCS::Worker::detail {

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    void worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::mark_progress(progress_marks& marks) const noexcept {
        for (std::size_t i = 0; i < threads_; ++i) marks.tick[i] = locals_[i].tick.load(std::memory_order_acquire);
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    std::size_t worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::progressed_since(const progress_marks& marks) const noexcept {
        std::size_t alive = 0;
        for (std::size_t i = 0; i < threads_; ++i) {
            if (locals_[i].tick.load(std::memory_order_acquire) != marks.tick[i]) ++alive;
        }
        return alive;
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    template<typename Room, typename Put>
    std::size_t worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::evacuate_local(std::size_t id, scheduler_metrics& metrics, Room&& room, Put&& put) {
        auto& victim = locals_[id];
        QueuedTask taken_task{};
        std::size_t taken = 0;
        // MaxWorkers is the watchdog's hazard slot (see hazard_slots): no worker ever publishes there.
        while (room() && victim.queue.steal_one(taken_task, MaxWorkers, hazards_)) {
            victim.depth.fetch_sub(1, std::memory_order_relaxed);
            metrics.adjust_local_depth(-1);
            put(std::move(taken_task));
            ++taken;
        }
        return taken;
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    bool worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::inject_local(std::size_t id, QueuedTask&& work, scheduler_metrics& metrics) {
        auto& local = locals_[id];
        if (local.depth.load(std::memory_order_relaxed) >= LocalCapacity) return false;
        local.queue.push_bottom(std::move(work), hazards_);
        local.depth.fetch_add(1, std::memory_order_relaxed);
        metrics.adjust_local_depth(1);
        return true;
    }

    template<std::size_t LocalCapacity, std::size_t MaxWorkers, typename QueuedTask>
    void worker_registry<LocalCapacity, MaxWorkers, QueuedTask>::reset_after_clone() noexcept {
        for (std::size_t i = 0; i < MaxWorkers; ++i) {
            auto& slot = locals_[i];
            std::construct_at(&slot.queue);   // old segments leaked on purpose, see the declaration
            slot.depth.store(0, std::memory_order_relaxed);
            slot.tick.store(0, std::memory_order_relaxed);
            slot.stalled.store(false, std::memory_order_relaxed);
        }
        std::construct_at(&hazards_);
        draining_.store(false, std::memory_order_relaxed);
        watch_.fill(watch_state{});
        idle_.invalidate();
    }

}
