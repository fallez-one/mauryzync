#pragma once

#include "../experimental.hpp"
#include "cpu_relax.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <thread>
#include <utility>

namespace FCS::Worker::detail {

    // Pending enqueue_until() timers: a binary min-heap with a COMPILE-TIME capacity.
    //
    //  * O(log N) register and fire (sift up / sift down) -- no scanning, no growth, no allocation.
    //  * The heap orders 16-byte entries {deadline, slot}; the (large, inline) tasks stay put in a
    //    parallel slot array with a free list, so sifting never moves a task.
    //  * Mutations (register, fire) take a tiny spin lock: the critical section is a handful of
    //    cache-resident comparisons, far shorter than parking a thread would cost. A mutex is not
    //    used. What matters for the idle path is that it never takes the lock at all: the earliest
    //    deadline is mirrored in one atomic (`next_`), which every worker reads when deciding
    //    whether a timer might be due or how long it may sleep.
    //  * Bounded: add() returns added=false when `Capacity` timers are pending, the same refusal a
    //    full admission queue gives.
    template<std::size_t Capacity, typename Task>
    class timer_heap {
        static_assert(Capacity > 0 && Capacity <= 0xFFFFFFFFu, "capacity must fit a 32-bit slot index");

    public:
        static constexpr std::int64_t none = std::numeric_limits<std::int64_t>::max();

        struct add_result {
            bool added{false};
            bool became_earliest{false}; // sleepers computed their wake-up from a later deadline: they must re-evaluate
        };

        timer_heap() noexcept {
            for (std::size_t i = 0; i < Capacity; ++i) free_[i] = static_cast<std::uint32_t>(Capacity - 1 - i);
            free_count_ = Capacity;
        }

        // Any thread. O(log N).
        [[nodiscard]] add_result add(std::chrono::steady_clock::time_point deadline, Task&& action) noexcept {
            const guard lock{*this};
            if (free_count_ == 0) return {};
            const auto slot = free_[--free_count_];
            tasks_[slot] = std::move(action);
            const auto ticks = deadline.time_since_epoch().count();
            std::size_t i = size_++;
            while (i > 0) { // sift up
                const std::size_t parent = (i - 1) / 2;
                if (heap_[parent].deadline <= ticks) break;
                heap_[i] = heap_[parent];
                i = parent;
            }
            heap_[i] = entry{ticks, slot};
            const bool earliest = (i == 0);
            if (earliest) next_.store(ticks, std::memory_order_release);
            return {true, earliest};
        }

        // Earliest pending deadline in steady_clock ticks, or `none`. Lock-free.
        [[nodiscard]] std::int64_t next_deadline() const noexcept { return next_.load(std::memory_order_acquire); }

        // Any thread. Moves the earliest timer with deadline <= now into `out`. O(log N); the
        // common "nothing due" answer is one atomic load and no lock.
        [[nodiscard]] bool fire_one(std::chrono::steady_clock::time_point now, Task& out) noexcept {
            const auto now_ticks = now.time_since_epoch().count();
            if (next_.load(std::memory_order_acquire) > now_ticks) return false;
            const guard lock{*this};
            if (size_ == 0 || heap_[0].deadline > now_ticks) return false; // another worker took it
            const auto slot = heap_[0].slot;
            out = std::move(tasks_[slot]);
            free_[free_count_++] = slot;
            const entry last = heap_[--size_];
            if (size_ > 0) { // sift down
                std::size_t i = 0;
                for (;;) {
                    std::size_t child = 2 * i + 1;
                    if (child >= size_) break;
                    if (child + 1 < size_ && heap_[child + 1].deadline < heap_[child].deadline) ++child;
                    if (last.deadline <= heap_[child].deadline) break;
                    heap_[i] = heap_[child];
                    i = child;
                }
                heap_[i] = last;
            }
            next_.store(size_ == 0 ? none : heap_[0].deadline, std::memory_order_release);
            return true;
        }

        [[nodiscard]] std::size_t size() const noexcept { const guard lock{const_cast<timer_heap&>(*this)}; return size_; }

#if FCS_EXPERIMENTAL_ALWAYS_ON
        // In a clone: the thread that held the spin lock does not exist there. (The pending
        // timers themselves are kept -- the deadlines are steady_clock ticks, which a clone shares.)
        void release_lock_after_clone() noexcept { lock_.store(false, std::memory_order_relaxed); }
#endif

    private:
        struct entry {
            std::int64_t deadline{0};
            std::uint32_t slot{0};
        };

        // Test-and-test-and-set with a brief spin, then yielding if the holder was preempted
        // (a descheduled holder is the only way this lock is ever held for long).
        struct guard {
            explicit guard(timer_heap& heap) noexcept : h(heap) {
                for (unsigned spins = 0;; ++spins) {
                    if (!h.lock_.load(std::memory_order_relaxed) && !h.lock_.exchange(true, std::memory_order_acquire)) return;
                    if (spins < 64) cpu_relax(); else std::this_thread::yield();
                }
            }
            ~guard() { h.lock_.store(false, std::memory_order_release); }
            guard(const guard&) = delete;
            guard& operator=(const guard&) = delete;
            timer_heap& h;
        };

        alignas(64) std::atomic<std::int64_t> next_{none};
        alignas(64) std::atomic_bool lock_{false};
        std::size_t size_{0};
        std::size_t free_count_{0};
        std::array<entry, Capacity> heap_{};
        std::array<std::uint32_t, Capacity> free_{};
        std::array<Task, Capacity> tasks_{};
    };

}
