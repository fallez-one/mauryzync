#pragma once

#include <array>
#include <atomic>
#include <cstddef>

namespace FCS::Worker::detail {

    // A small, fixed-size hazard pointer domain: one publish slot per worker thread.
    // Before a thief dereferences a segment it does not own, it "protects" the
    // pointer here first; the owner will not actually free a retired segment while
    // any slot still names it. This is the "deferred reclamation after concurrent
    // readers are safe" step in the segmented deque's retirement path — a bounded,
    // lock-free alternative to a mutex or to giving every segment a shared_ptr-style
    // control block.
    template<std::size_t MaxWorkers>
    class hazard_domain {
    public:
        // Publishes `source`'s current value as protected before returning it, so the
        // pointer is safe to dereference even if some other thread concurrently
        // unlinks and retires it. Re-checks after publishing and retries on a race
        // instead of ever handing back (or using) a pointer that was not protected.
        template<typename P>
        [[nodiscard]] P* protect(std::size_t thread_id, const std::atomic<P*>& source) noexcept {
            for (;;) {
                P* ptr = source.load(std::memory_order_acquire);
                slots_[thread_id].store(static_cast<void*>(ptr), std::memory_order_seq_cst);
                if (source.load(std::memory_order_seq_cst) == ptr) return ptr;
                // `source` moved between our read and our publish — the pointer we
                // grabbed may already be mid-retirement. Republish and re-check
                // rather than ever touching it.
            }
        }

        void clear(std::size_t thread_id) noexcept { slots_[thread_id].store(nullptr, std::memory_order_release); }

        [[nodiscard]] bool protected_by_someone(const void* ptr) const noexcept {
            for (auto& slot : slots_) if (slot.load(std::memory_order_acquire) == ptr) return true;
            return false;
        }

    private:
        std::array<std::atomic<void*>, MaxWorkers> slots_{};
    };

}
