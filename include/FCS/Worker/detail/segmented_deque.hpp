#pragma once

#include "hazard_domain.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <deque>
#include <optional>
#include <type_traits>
#include <vector>

namespace FCS::Worker::detail {

    // A single fixed-capacity slab in a worker's local deque. Segments are chained
    // via `next` (owner-published, thief-read) so a thief can walk from the oldest
    // (head) segment toward the newest without ever needing the owner's private
    // bookkeeping. `top`/`bottom` follow the classic Chase-Lev convention: the owner
    // grows/shrinks `bottom`, thieves only ever advance `top`.
    template<typename T, std::size_t SegmentCapacity>
    struct deque_segment {
        static_assert(SegmentCapacity != 0);

        std::array<std::optional<T>, SegmentCapacity> slots{};
        alignas(64) std::atomic_size_t top{0};
        alignas(64) std::atomic_size_t bottom{0};
        std::atomic<deque_segment*> next{nullptr};
    };

    // Chase-Lev-inspired *segmented* work-stealing deque.
    //
    //   active segment chain     owner appends/pops via push_bottom/pop_bottom
    //   claimed batch queue      thief moves a claimed range into its own deque
    //   retired segments         reclaimed only once no thief's hazard pointer
    //                            still names them (see hazard_domain.hpp)
    //
    // The owning worker pushes/pops the bottom of its current segment (wait-free,
    // single-threaded — no other thread ever touches bottom/prev bookkeeping).
    // Thieves steal from the top (head) segment: one item at a time (`steal_one`),
    // or a contiguous range reserved with a single CAS per segment touched
    // (`steal_batch`), moved directly into the thief's own local deque with
    // std::move — never a full copy-then-delete of the queue, never O(size of the
    // whole queue) for the steal bookkeeping itself.
    template<typename T, std::size_t SegmentCapacity, std::size_t MaxWorkers>
    class segmented_deque {
    public:
        using segment_t = deque_segment<T, SegmentCapacity>;
        using hazards_t = hazard_domain<MaxWorkers>;

        segmented_deque();
        segmented_deque(const segmented_deque&) = delete;
        segmented_deque& operator=(const segmented_deque&) = delete;
        ~segmented_deque();

        // Owner-only (the worker that owns this slot). Never fails; grows on demand.
        void push_bottom(T&& value, hazards_t& hazards);

        // Owner-only.
        [[nodiscard]] bool pop_bottom(T& out, hazards_t& hazards) noexcept;

        // Thief-only: steal exactly one item from the head of this (foreign) deque.
        [[nodiscard]] bool steal_one(T& out, std::size_t thief_id, hazards_t& hazards) noexcept;

        // Thief-only: reserve up to `requested` items from the head of this (foreign)
        // deque — O(1) CAS reservation per segment touched — moving them straight
        // into `dest` (the thief's own local deque). Returns the number actually
        // claimed: best-effort, may be less than requested (or zero) if the victim
        // is contended or ran out of work under us.
        [[nodiscard]] std::size_t steal_batch(std::size_t requested, segmented_deque& dest, std::size_t thief_id, hazards_t& hazards) noexcept;

    private:
        static constexpr int max_hops = 6;

        void unlink_if_exhausted(segment_t* seg) noexcept;
        void retire(segment_t* seg, hazards_t& hazards) noexcept;
        void reap_front(hazards_t& hazards) noexcept;

        std::atomic<segment_t*> head_{nullptr};
        std::deque<segment_t*> chain_;     // owner-private bookkeeping, oldest..newest
        std::size_t tail_index_{0};        // owner's current pop cursor into chain_
        std::vector<segment_t*> pending_;  // owner-private: retired, awaiting reclamation
    };

}

#include "segmented_deque.inl"
