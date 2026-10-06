#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <semaphore>

namespace FCS::Worker::detail {

    // One sleeper's park/unpark primitive: a binary semaphore plus a `sleeping`
    // flag that makes waking *claim-based*. No mutex, no condition variable --
    // std::binary_semaphore is a futex (Linux) / WaitOnAddress (Windows) under the
    // hood, and it gives us what std::atomic::wait cannot: a timed wait.
    //
    // The claim is what keeps the semaphore's invariant (never more than one
    // pending token -- releasing past max() is undefined) without a lock: a waker
    // only release()s after winning `sleeping.exchange(false)`, and a sleeper that
    // gives up (timeout / late cancel) tries the same exchange; if it *loses*, a
    // waker has claimed it and its release() is on the way, so the sleeper
    // consumes that token instead of leaking it into its next park.
    //
    // One thread parks on a given parker at a time; any thread may unpark().
    template<typename Semaphore = std::binary_semaphore>
    class parker {
    public:
        // Blocks until unpark(), `bound` elapsing, or `cancel()` -- which is
        // evaluated *after* announcing the sleep, and is what closes the
        // missed-wakeup window (see parking_lot). A non-positive bound only
        // announces and retracts. Returns normally in every case; callers
        // re-check their own conditions, as with any condition variable.
        template<typename Cancel>
        void park(std::optional<std::chrono::nanoseconds> bound, Cancel&& cancel) {
            sleeping_.store(true, std::memory_order_seq_cst);
            if (cancel()) { retract(); return; }
            if (!bound) { sem_.acquire(); return; }
            if (*bound > std::chrono::nanoseconds::zero() && sem_.try_acquire_for(*bound)) return;
            retract();
        }

        // True iff this call woke a sleeper.
        bool unpark() noexcept {
            // seq_cst, not relaxed: this load is the waker half of the Dekker pairing with
            // park()'s seq_cst announce (see parking_lot); a relaxed read is formally allowed
            // to miss it (x86 happens to forbid that; ARM does not).
            if (!sleeping_.load(std::memory_order_seq_cst)) return false;
            if (!sleeping_.exchange(false, std::memory_order_seq_cst)) return false;
            sem_.release();
            return true;
        }

        [[nodiscard]] bool sleeping() const noexcept { return sleeping_.load(std::memory_order_relaxed); }

    private:
        void retract() noexcept {
            if (!sleeping_.exchange(false, std::memory_order_seq_cst)) sem_.acquire(); // a waker claimed us: eat its token
        }

        alignas(64) std::atomic_bool sleeping_{false};
        Semaphore sem_{0};
    };

    // A pool's parked workers, woken one or all at a time, lock-free.
    //
    // Replaces `mutex + condition_variable + epoch`. Wakeups are *targeted*: a
    // submit wakes one sleeping worker (the old notify_all roused the whole pool
    // for every task, and every one of them then fought over the same mutex).
    //
    // The protocol is the classic Dekker pairing, all seq_cst:
    //
    //   sleeper:  sleepers++  ;  sleeping = true  ;  re-read epoch (and stop flag)
    //   waker:    epoch++     ;  read sleepers    ;  scan for a `sleeping` worker
    //
    // Whatever the interleaving, either the waker sees the sleeper announced (and
    // wakes it), or the sleeper's re-read sees the waker's epoch bump (and doesn't
    // sleep). `epoch` therefore has to be bumped on *every* notify, even with no
    // sleepers -- skipping that when `sleepers == 0` would reopen the window.
    template<std::size_t MaxWorkers, typename Semaphore = std::binary_semaphore>
    class parking_lot {
    public:
        void configure(std::size_t workers) noexcept { count_ = workers < MaxWorkers ? workers : MaxWorkers; }

        // Sample before looking for work; pass to park().
        [[nodiscard]] std::uint64_t epoch() const noexcept { return epoch_.load(std::memory_order_seq_cst); }
        [[nodiscard]] std::size_t sleepers() const noexcept { return sleepers_.load(std::memory_order_relaxed); }
        [[nodiscard]] bool sleeping(std::size_t id) const noexcept { return id < MaxWorkers && slots_[id].sleeping(); } // diagnostics

        // Parks worker `id` unless the epoch moved since `seen` or `extra_cancel()`
        // (e.g. "pool is stopping") says not to. Returns on wake, timeout or cancel.
        template<typename Cancel>
        void park(std::size_t id, std::uint64_t seen, std::optional<std::chrono::nanoseconds> bound, Cancel&& extra_cancel) {
            sleepers_.fetch_add(1, std::memory_order_seq_cst);
            slots_[id].park(bound, [&] { return epoch_.load(std::memory_order_seq_cst) != seen || extra_cancel(); });
            sleepers_.fetch_sub(1, std::memory_order_seq_cst);
        }

        // Something a worker could act on exists: wake at most one sleeper.
        void notify_one() noexcept {
            epoch_.fetch_add(1, std::memory_order_seq_cst);
            if (sleepers_.load(std::memory_order_seq_cst) == 0) return;
            const auto start = cursor_.fetch_add(1, std::memory_order_relaxed);
            for (std::size_t n = 0; n < count_; ++n) {
                if (slots_[(start + n) % count_].unpark()) return;
            }
        }

        // The pool's shape changed (stop, timer registered, poll hook added...):
        // every sleeper must re-evaluate.
        void notify_all() noexcept {
            epoch_.fetch_add(1, std::memory_order_seq_cst);
            if (sleepers_.load(std::memory_order_seq_cst) == 0) return;
            for (std::size_t i = 0; i < count_; ++i) (void)slots_[i].unpark();
        }

    private:
        alignas(64) std::atomic<std::uint64_t> epoch_{0};
        alignas(64) std::atomic<std::size_t> sleepers_{0};
        std::atomic<std::size_t> cursor_{0};
        std::size_t count_{0};
        std::array<parker<Semaphore>, MaxWorkers> slots_{};
    };

}
