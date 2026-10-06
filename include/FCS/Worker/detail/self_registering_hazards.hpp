#pragma once

#include "hazard_domain.hpp"

#include <atomic>
#include <cstddef>

namespace FCS::Worker::detail {

    // hazard_domain<N> needs an explicit, small, known thread id per caller —
    // fine for the fixed worker pool, not for something like
    // static_channel::post(), which is reachable from any thread (worker
    // threads, a backend's dedicated poller thread, or arbitrary external
    // threads calling pool_service::post() directly). This wraps it so any
    // number of distinct threads can use it safely: each calling thread
    // lazily claims a slot on first use via a process-wide round-robin
    // counter, cached in a thread_local so the same thread always reuses the
    // same slot thereafter. If more distinct threads are ever concurrently
    // active than there are slots, multiple threads simply share one — that
    // only ever makes reclamation more conservative (a shared slot stays
    // "protected" as long as any sharer needs it), never unsafe.
    template<std::size_t Slots>
    class self_registering_hazard_domain {
    public:
        template<typename P>
        [[nodiscard]] P* protect(const std::atomic<P*>& source) noexcept { return domain_.protect(slot_for_this_thread(), source); }
        void clear() noexcept { domain_.clear(slot_for_this_thread()); }
        [[nodiscard]] bool protected_by_someone(const void* ptr) const noexcept { return domain_.protected_by_someone(ptr); }

    private:
        // Deliberately a static member function: the thread->slot mapping is
        // shared by every instance of self_registering_hazard_domain<Slots>
        // (one counter per Slots value, not per object), while each instance
        // still has its own independent hazard_domain — reusing a slot index
        // across different domains is harmless since their slot arrays don't
        // interact.
        [[nodiscard]] static std::size_t slot_for_this_thread() noexcept {
            static std::atomic_size_t next{};
            static thread_local const std::size_t id = next.fetch_add(1, std::memory_order_relaxed) % Slots;
            return id;
        }

        hazard_domain<Slots> domain_{};
    };

}
