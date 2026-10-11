#ifndef FCS_SYNC_MUTEX_TRAITS
#define FCS_SYNC_MUTEX_TRAITS
#include <concepts>
#include <cstdint>

namespace FCS::synchronization {
template<typename T>
    concept BasicLockable = requires(T t) {
        t.lock();
        t.unlock();
    };
    template<typename T>
    concept InterruptibleMutexTrait = BasicLockable<T> && requires(T& t) {
        t.interrupt();
        t.reset_interrupt();
        { t.lock_interruptible() } -> std::same_as<bool>;
    };
    static constexpr std::int8_t LOCKED = 1u;
    static constexpr std::int8_t INTERRUPTED = 2u;
    static constexpr std::int8_t WAITERS = 4u;
}
#endif
