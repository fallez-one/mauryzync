#ifndef FCS_SYNC_INTERRUPTIBLE_MUTEX
#define FCS_SYNC_INTERRUPTIBLE_MUTEX
#include <atomic>
#include <cstdint>
#include <system_error>

#include "mutex_traits.hpp"

namespace FCS::synchronization {
    // The word holds THREE independent facts as bits, not one three-valued state:
    //   LOCKED      somebody is inside the critical section
    //   INTERRUPTED interrupt() was called; lock_interruptible() fails until reset_interrupt()
    //   WAITERS     somebody may be asleep in wait(): unlock() must notify
    class interruptible_mutex {
        std::atomic<std::int8_t> state{0};

        // Shared slow path. `interruptible`: give up (return false) once INTERRUPTED is set.
        // lock() passes false: it still WAKES on interrupt() (the word changes) but re-evaluates and
        // carries on waiting -- the interruption is masked from it and visible only through
        // is_interrupted(), which the caller checks once it owns the mutex.
        bool acquire(bool interruptible) noexcept {
            std::int8_t v = state.load(std::memory_order_relaxed);
            bool slept = false;
            for (;;) {
                if (interruptible && (v & INTERRUPTED)) return false;
                if (!(v & LOCKED)) {
                    // Take the lock, leaving INTERRUPTED as it was. A thread that has slept takes it as
                    // "contended" (WAITERS set): other sleepers it left behind may exist, and its unlock()
                    // is what must wake the next one (see unlock()).
                    if (state.compare_exchange_weak(v, v | LOCKED | (slept ? WAITERS : 0u), std::memory_order_acquire, std::memory_order_relaxed)) return true;
                    continue; // v was refreshed by the failed CAS
                }
                // held: announce that someone sleeps (so unlock() notifies), then sleep until the word changes
                if (!(v & WAITERS)) {
                    if (!state.compare_exchange_weak(v, v | WAITERS, std::memory_order_relaxed, std::memory_order_relaxed)) continue;
                    v |= WAITERS;
                }
                slept = true;
                state.wait(v, std::memory_order_relaxed); // returns when the word != v: unlock(), interrupt() or a spurious wake
                v = state.load(std::memory_order_relaxed);
            }
        }

    public:
        void lock() noexcept {
            std::int8_t expected = 0;
            // uncontended: one CAS, no loop, no flags to preserve
            if (state.compare_exchange_strong(expected, LOCKED, std::memory_order_acquire, std::memory_order_relaxed)) return;
            (void)acquire(false);
        }

        bool try_lock() noexcept {
            std::int8_t v = state.load(std::memory_order_relaxed);
            while (!(v & LOCKED)) {
                if (state.compare_exchange_weak(v, v | LOCKED, std::memory_order_acquire, std::memory_order_relaxed)) return true;
            }
            return false;
        }

        // true: acquired. false: interrupted before (or while) waiting -- the mutex is NOT held.
        [[nodiscard]] bool lock_interruptible() noexcept {
            std::int8_t expected = 0;
            if (state.compare_exchange_strong(expected, LOCKED, std::memory_order_acquire, std::memory_order_relaxed)) return true;
            return acquire(true);
        }

        // Caller must hold the mutex, so LOCKED is known to be set: clearing it is a plain subtract of
        // one, a single `lock xadd` -- fetch_and(~LOCKED) with a used result compiles to a CAS loop.
        void unlock() noexcept {
            const std::uint32_t prev = state.fetch_sub(LOCKED, std::memory_order_release);
            if (prev & WAITERS) {
                // Someone sleeps. Clear the flag before waking one of them: the woken thread re-asserts it
                // when it takes the lock (it has slept), so the next unlock wakes the next sleeper -- and
                // once nobody is left the flag stays clear, instead of every unlock paying for a notify.
                state.fetch_and(~WAITERS, std::memory_order_relaxed);
                state.notify_one();
            }
        }

        // Is the mutex held right now? Act immediately.
        [[nodiscard]] bool locking() const noexcept {
            return (state.load(std::memory_order_relaxed) & LOCKED) != 0;
        }

        // Sticky: every current and future lock_interruptible() fails until reset_interrupt(). Waiters
        // already inside lock() (non-interruptible) are unaffected, and so is the holder.
        void interrupt() noexcept {
            state.fetch_or(INTERRUPTED, std::memory_order_release);
            state.notify_all();
        }
        void reset_interrupt() noexcept { state.fetch_and(~INTERRUPTED, std::memory_order_release); }

        [[nodiscard]] bool is_interrupted() const noexcept {
            return (state.load(std::memory_order_relaxed) & INTERRUPTED) != 0;
        }
    };

    template<InterruptibleMutexTrait Mutex>
    class interruptible_lock_guard {
        Mutex &mtx;
        bool acquired_;
    public:
        explicit interruptible_lock_guard(Mutex &mtx) noexcept : mtx{mtx}, acquired_{mtx.lock_interruptible()} {}
        ~interruptible_lock_guard() {
            if (acquired_) {
                mtx.reset_interrupt();
                mtx.unlock();
            }
        }
        interruptible_lock_guard(const interruptible_lock_guard &) = delete;
        interruptible_lock_guard &operator=(const interruptible_lock_guard &) = delete;

        // Check this before touching what the mutex protects: an interrupted guard holds nothing.
        [[nodiscard]] bool owns_lock() const noexcept { return acquired_; }

        void rollback() const noexcept {
            if(acquired_) {
                mtx.interrupt();
            }
        }

        [[nodiscard]] explicit operator bool() const noexcept {
            return owns_lock();
        }
    };

    template<InterruptibleMutexTrait Mutex>
    class interruptible_lock_guard_throwable {
        Mutex &mutex;
    public:
        explicit interruptible_lock_guard_throwable(Mutex &mtx) : mutex{mtx} {
            // Acquire here and KEEP it
            if (!mutex.lock_interruptible()) {
                throw std::system_error(std::make_error_code(std::errc::operation_canceled), "Mutex acquisition interrupted.");
            }
        }
        void rollback() const {
            mutex.interrupt();
        }
        ~interruptible_lock_guard_throwable() { mutex.reset_interrupt(); mutex.unlock(); }
        interruptible_lock_guard_throwable(const interruptible_lock_guard_throwable &) = delete;
        interruptible_lock_guard_throwable &operator=(const interruptible_lock_guard_throwable &) = delete;
    };
}

#endif
