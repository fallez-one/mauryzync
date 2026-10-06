#pragma once

#include <utility>

namespace FCS::Worker::detail {

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    void admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::configure(std::size_t full, std::size_t available, std::size_t candidates) noexcept {
        full_threshold_ = std::clamp(full, std::size_t{1}, CushionCapacity);
        available_threshold_ = std::min(available, full_threshold_ - 1);
        candidate_budget_ = std::clamp(candidates, std::size_t{1}, CushionCapacity);
    }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    std::size_t admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::high_priority_limit() const noexcept {
        std::size_t reserve = normal_reserve_ == auto_reserve ? std::max<std::size_t>(1, full_threshold_ / 16) : normal_reserve_;
        // Never reserve the whole cushion: high keeps at least one slot, or high could not run at all.
        if (reserve >= full_threshold_) reserve = full_threshold_ - 1;
        return full_threshold_ - reserve;
    }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    bool admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::submit(QueuedTask work, scheduler_metrics& metrics) noexcept {
        // Read before the move: `work` is gone after try_push.
        const bool is_high = work.priority == task_priority::high;
        if (admission_.load(std::memory_order_acquire) == queue_state::full
            // High-priority work stops short of "full", leaving the last slots to normal
            // (see reserve_for_normal). Depth is advisory (relaxed), so this is a soft cap.
            || (is_high && metrics.cushion_depth() >= high_priority_limit())
            || !cushion_.try_push(std::move(work))) {
            metrics.record_rejected();
            return false;
        }
        metrics.record_submit(work.source);
        metrics.adjust_cushion_depth(1);
        if (metrics.cushion_depth() >= full_threshold_) admission_.store(queue_state::full, std::memory_order_release);
        refresh(metrics);
        return true;
    }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    void admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::refresh(scheduler_metrics& metrics) noexcept {
        cushion_state_.store(classify_queue_state(metrics.cushion_depth(), available_threshold_, full_threshold_), std::memory_order_release);
        if (metrics.cushion_depth() <= available_threshold_) admission_.store(queue_state::available, std::memory_order_release);
    }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    template<typename FastQueue>
    bool admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::refill(
        scheduler_metrics& metrics, FastQueue& fast, bounded_mpmc_queue<QueuedTask, SlowCapacity>& slow) {
        bool expected = false;
        if (!refilling_.compare_exchange_strong(expected, true)) return false;
        metrics.record_refill();

        while (pending_count_ < candidate_budget_) {
            QueuedTask next{};
            if (!cushion_.try_pop(next)) break;
            pending_[pending_count_++].emplace(std::move(next));
        }

        std::size_t moved{};
        for (std::size_t i{}; i < pending_count_; ++i) {
            auto& slot = pending_[i];
            if (!slot) continue;
            const bool placed = slot->lane == workload::Fast ? fast.try_push(std::move(*slot)) : slow.try_push(std::move(*slot));
            if (!placed) continue;
            ++moved;
            slot.reset();
            metrics.adjust_cushion_depth(-1);
            metrics.adjust_mpmc_depth(1);
        }

        // Compacts pending_[0, pending_count_) down to the still-unplaced
        // entries. IMPORTANT: std::optional's move-assignment does NOT
        // clear the source's has_value() -- after `dst = std::move(src)`,
        // `src` still reads as engaged (holding a moved-from, garbage
        // QueuedTask). Left unreset, that "ghost" entry looks like a
        // legitimate pending item on the NEXT refill() call: it gets
        // "placed" (pushing a spent std::function into fast_/slow_, which
        // later executes as a silent no-op or a caught bad_function_call)
        // and its placement wrongly decrements cushion_depth_ a second
        // time for a task that was already counted -- a real, observed
        // underflow under sustained heavy concurrent submission. Only
        // scanning [0, pending_count_) (not the full, much larger
        // CushionCapacity-sized array) is also just the correct bound:
        // nothing beyond the old pending_count_ was ever written.
        std::size_t remaining{};
        for (std::size_t i{}; i < pending_count_; ++i) {
            if (!pending_[i]) continue;
            if (remaining != i) {
                pending_[remaining] = std::move(pending_[i]);
                pending_[i].reset();
            }
            ++remaining;
        }
        pending_count_ = remaining;

        refilling_.store(false, std::memory_order_release);
        refresh(metrics);
        // Progress, not "won the claim": a refill that found the cushion empty or the
        // target queue full moved nothing. Reporting that as true made every worker
        // `continue` straight back to the top of its loop -- skipping the park / poll
        // steps -- and spin on refill() for as long as the cushion stayed non-empty.
        return moved != 0;
    }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    queue_state admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::cushion_state() const noexcept { return cushion_state_.load(std::memory_order_acquire); }

    template<std::size_t CushionCapacity, std::size_t SlowCapacity, typename QueuedTask>
    queue_state admission_gate<CushionCapacity, SlowCapacity, QueuedTask>::admission_state() const noexcept { return admission_.load(std::memory_order_acquire); }

}
