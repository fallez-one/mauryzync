#ifndef FCS_SYNC_ORDERED_MUTEX
#define FCS_SYNC_ORDERED_MUTEX

#include <atomic>
#include <concepts>
#include <cstdint>
#include <type_traits>

#include "mutex_traits.hpp"

namespace FCS::synchronization {
    template<std::unsigned_integral EntrySizeType>
    class ordered_mutex {
        static constexpr std::int8_t EPOCH = WAITERS;
        static_assert(!std::is_same_v<EntrySizeType, bool>);

        std::atomic<EntrySizeType> ticket{0};
        std::atomic<EntrySizeType> serving{0};
        std::atomic<std::uint8_t> state{0};

        static constexpr EntrySizeType step(EntrySizeType value) noexcept {
            return static_cast<EntrySizeType>(value + 1u);
        }

        // Dekker pair: serving store vs WAITERS, both seq_cst.
        void release_turn() noexcept {
            serving.store(step(serving.load(std::memory_order_relaxed)), std::memory_order_seq_cst);
            std::uint8_t seen = state.load(std::memory_order_seq_cst);
            // Epoch bits stop ABA so a sleeper never misses a wake.
            while (seen & WAITERS) {
                const auto bumped = static_cast<std::uint8_t>((seen + EPOCH) & ~WAITERS);
                if (state.compare_exchange_weak(seen, bumped, std::memory_order_seq_cst, std::memory_order_relaxed)) {
                    state.notify_all();
                    return;
                }
            }
        }

        bool wait_turn(EntrySizeType my, bool interruptible) noexcept {
            bool abandoned = false;
            for (;;) {
                const std::uint8_t seen = state.load(std::memory_order_acquire);
                if (serving.load(std::memory_order_seq_cst) == my) {
                    if (!abandoned) return true;
                    release_turn();
                    return false;
                }
                if (!abandoned && interruptible && (seen & INTERRUPTED)) {
                    EntrySizeType last = step(my);
                    if (ticket.compare_exchange_strong(last, my, std::memory_order_relaxed, std::memory_order_relaxed)) return false;
                    abandoned = true;
                    continue;
                }
                if (!(seen & WAITERS)) {
                    state.fetch_or(WAITERS, std::memory_order_seq_cst);
                    continue;
                }
                state.wait(seen, std::memory_order_acquire);
            }
        }

        bool acquire(bool interruptible) noexcept {
            const EntrySizeType my = ticket.fetch_add(1, std::memory_order_relaxed);
            if (serving.load(std::memory_order_acquire) == my) return true;
            return wait_turn(my, interruptible);
        }

    public:
        void lock() noexcept {
            (void)acquire(false);
        }

        [[nodiscard]] bool try_lock() noexcept {
            EntrySizeType head = serving.load(std::memory_order_acquire);
            const EntrySizeType next = step(head);
            return ticket.compare_exchange_strong(head, next, std::memory_order_acquire, std::memory_order_relaxed);
        }

        [[nodiscard]] bool lock_interruptible() noexcept {
            if (is_interrupted()) return false;
            return acquire(true);
        }

        void unlock() noexcept {
            release_turn();
        }

        [[nodiscard]] bool locking() const noexcept {
            const EntrySizeType head = serving.load(std::memory_order_acquire);
            return ticket.load(std::memory_order_acquire) != head;
        }

        [[nodiscard]] bool is_interrupted() const noexcept {
            return (state.load(std::memory_order_acquire) & INTERRUPTED) != 0;
        }

        void interrupt() noexcept {
            const std::uint8_t prev = state.fetch_or(INTERRUPTED, std::memory_order_acq_rel);
            if ((prev & WAITERS) && !(prev & INTERRUPTED)) state.notify_all();
        }

        void reset_interrupt() noexcept {
            state.fetch_and(static_cast<std::uint8_t>(~INTERRUPTED), std::memory_order_release);
        }
    };

    using small_ordered_mutex = ordered_mutex<std::uint8_t>;
}
#endif