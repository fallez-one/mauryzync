#pragma once

#include <algorithm>
#include <atomic>
#include <utility>

namespace FCS::Worker::detail {

    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    segmented_deque<T, SegmentCapacity, MaxWorkers>::segmented_deque() {
        auto* first = new segment_t();
        chain_.push_back(first);
        head_.store(first, std::memory_order_relaxed);
    }

    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    segmented_deque<T, SegmentCapacity, MaxWorkers>::~segmented_deque() {
        // No concurrent thieves at teardown time — pool_service joins every worker
        // thread before any local deque is destroyed.
        for (auto* seg : chain_) delete seg;
        for (auto* seg : pending_) delete seg;
    }

    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    void segmented_deque<T, SegmentCapacity, MaxWorkers>::unlink_if_exhausted(segment_t* seg) noexcept {
        auto* next = seg->next.load(std::memory_order_acquire);
        if (!next) return; // still the active tail (or a benign race) — nothing to advance to yet
        segment_t* expected = seg;
        head_.compare_exchange_strong(expected, next, std::memory_order_acq_rel);
    }

    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    void segmented_deque<T, SegmentCapacity, MaxWorkers>::retire(segment_t* seg, hazards_t& hazards) noexcept {
        unlink_if_exhausted(seg);
        pending_.push_back(seg);
        std::size_t write = 0;
        for (std::size_t read = 0; read < pending_.size(); ++read) {
            auto* candidate = pending_[read];
            // Never free something still visibly reachable: either a thief is mid-pin
            // on it, or the shared head cursor still names it (e.g. it had no `next`
            // to advance to the moment we tried).
            const bool still_head = head_.load(std::memory_order_acquire) == candidate;
            if (still_head || hazards.protected_by_someone(candidate)) pending_[write++] = candidate;
            else delete candidate;
        }
        pending_.resize(write);
    }

    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    void segmented_deque<T, SegmentCapacity, MaxWorkers>::reap_front(hazards_t& hazards) noexcept {
        // Only ever reclaim from the front (oldest) end, and only once both the
        // owner and any thief agree a segment is fully drained — never the segment
        // the owner is currently using (tail_index_ == 0 guards that).
        while (chain_.size() > 1 && tail_index_ > 0) {
            auto* front = chain_.front();
            const auto t = front->top.load(std::memory_order_acquire);
            const auto b = front->bottom.load(std::memory_order_acquire);
            if (t != b) break;
            chain_.pop_front();
            --tail_index_;
            retire(front, hazards);
        }
    }

    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    void segmented_deque<T, SegmentCapacity, MaxWorkers>::push_bottom(T&& value, hazards_t& hazards) {
        reap_front(hazards);
        auto* tail = chain_.back();
        if (tail->bottom.load(std::memory_order_relaxed) == SegmentCapacity) {
            auto* fresh = new segment_t();
            tail->next.store(fresh, std::memory_order_release);
            chain_.push_back(fresh);
            tail = fresh;
        }
        tail_index_ = chain_.size() - 1; // a fresh push always resumes LIFO at the true newest segment
        const auto b = tail->bottom.load(std::memory_order_relaxed);
        tail->slots[b].emplace(std::move(value));
        tail->bottom.store(b + 1, std::memory_order_release);
    }

    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    bool segmented_deque<T, SegmentCapacity, MaxWorkers>::pop_bottom(T& out, hazards_t& hazards) noexcept {
        reap_front(hazards);
        for (;;) {
            auto* tail = chain_[tail_index_];
            auto b = tail->bottom.load(std::memory_order_relaxed);
            auto t = tail->top.load(std::memory_order_relaxed);
            if (b == t) {
                if (tail_index_ == 0) return false; // whole deque empty
                --tail_index_;                      // resume popping in the previous segment
                continue;
            }

            b -= 1;
            tail->bottom.store(b, std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            t = tail->top.load(std::memory_order_relaxed);

            if (t > b) {
                // A thief already cleared this segment out from under us.
                tail->bottom.store(b + 1, std::memory_order_relaxed);
                if (tail_index_ == 0) return false;
                --tail_index_;
                continue;
            }
            if (t == b) {
                // Last element in this segment — race a possible concurrent thief for it.
                const bool owner_wins = tail->top.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed);
                tail->bottom.store(b + 1, std::memory_order_relaxed);
                if (!owner_wins) {
                    if (tail_index_ == 0) return false;
                    --tail_index_;
                    continue;
                }
            }
            out = std::move(*tail->slots[b]);
            tail->slots[b].reset();
            return true;
        }
    }

    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    bool segmented_deque<T, SegmentCapacity, MaxWorkers>::steal_one(T& out, std::size_t thief_id, hazards_t& hazards) noexcept {
        for (int hop = 0; hop < max_hops; ++hop) {
            auto* seg = hazards.protect(thief_id, head_);
            if (!seg) { hazards.clear(thief_id); return false; }

            const auto t = seg->top.load(std::memory_order_acquire);
            const auto b = seg->bottom.load(std::memory_order_acquire);
            if (t >= b) {
                unlink_if_exhausted(seg);
                hazards.clear(thief_id);
                continue;
            }

            auto expected = t;
            if (seg->top.compare_exchange_strong(expected, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
                out = std::move(*seg->slots[t]);
                seg->slots[t].reset();
                hazards.clear(thief_id);
                return true;
            }
            hazards.clear(thief_id); // lost the race (owner or another thief); retry
        }
        return false;
    }

    // Root cause of a real, reported race: `take` used to be computed from a
    // single (t, b) snapshot with no further validation. If the owner's
    // concurrent, *unprotected* pop_bottom fast path (t < b, no CAS -- see
    // pop_bottom above) decremented `bottom` past part of that snapshot before
    // this CAS committed, the batch claim could silently include the exact
    // index the owner had already read, i.e. a genuine double-consume /
    // use-after-free on the same queued_task. A single-item CAS can't do this
    // (it only ever claims the one index it just validated), but a
    // multi-item jump can leap straight over the owner's boundary check
    // without ever contending on it. The fix: re-read `bottom` as close to
    // the CAS as possible, and never let the batch claim reach the
    // owner-adjacent boundary item -- that one is only ever safe to take via
    // the same CAS-protected transition pop_bottom itself contends on
    // (mirrored in steal_one).
    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    std::size_t segmented_deque<T, SegmentCapacity, MaxWorkers>::steal_batch(std::size_t requested, segmented_deque& dest, std::size_t thief_id, hazards_t& hazards) noexcept {
        std::size_t claimed = 0;
        for (int hop = 0; hop < max_hops && claimed < requested; ++hop) {
            auto* seg = hazards.protect(thief_id, head_);
            if (!seg) { hazards.clear(thief_id); break; }

            const auto t = seg->top.load(std::memory_order_acquire);
            auto b = seg->bottom.load(std::memory_order_acquire);
            if (t >= b) {
                unlink_if_exhausted(seg);
                hazards.clear(thief_id);
                continue;
            }

            // Re-read bottom immediately before the CAS to shrink the window
            // as much as this design allows, then always leave the single
            // most-recent (owner-boundary-adjacent) item unclaimed.
            b = seg->bottom.load(std::memory_order_acquire);
            if (t >= b) { unlink_if_exhausted(seg); hazards.clear(thief_id); continue; }
            const auto available = b - t;
            if (available <= 1) {
                // Only the boundary item is (maybe) here; don't risk it via a
                // batch CAS. Let the caller's steal_one fallback -- which
                // *does* safely contend for it -- pick it up instead.
                hazards.clear(thief_id);
                break;
            }

            const auto take = std::min(available - 1, requested - claimed);
            auto expected = t;
            if (!seg->top.compare_exchange_strong(expected, t + take, std::memory_order_seq_cst, std::memory_order_relaxed)) {
                hazards.clear(thief_id);
                continue; // lost this range to a racing thief (or the owner); retry
            }

            for (std::size_t i = 0; i < take; ++i) {
                dest.push_bottom(std::move(*seg->slots[t + i]), hazards);
                seg->slots[t + i].reset();
            }
            claimed += take;
            hazards.clear(thief_id);
        }
        return claimed;
    }

}
