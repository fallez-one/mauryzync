#pragma once

#include "../execution.hpp"
#include "../experimental.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <thread>

namespace FCS::Worker::detail {

    // Registration table for execution::shared_worker: instead of a
    // dedicated OS thread blocking in a backend's native wait call, a
    // backend hands this a non-blocking, single-pass poll callback and
    // pool_service::worker_loop() invokes it between task batches. Slot
    // claiming uses the same CAS-over-a-fixed-array approach the backends
    // themselves already use for their own registrations (see
    // backends/epoll.inl et al.) rather than a mutex-guarded container.
    //
    // `busy` does double duty: it stops two workers from concurrently
    // entering the same backend's poll callback (a level-triggered
    // multiplexer such as epoll can otherwise hand the same ready fd to two
    // concurrent waiters), and it lets unregister() wait out any invocation
    // already in flight before returning -- so a caller that unregisters
    // right before destroying the backend can't race a poll callback still
    // touching it.
    class poll_registry {
    public:
        using poll_fn = bool (*)(void*) noexcept;

        static constexpr std::size_t capacity = 8;

        // Returns a handle to pass to unregister(), or capacity if the
        // table is full. `reserved` (not `context`) is the actual claim --
        // context only gets published (release) once `fn` is already
        // written, so a concurrent poll_once() that acquire-loads a
        // non-null context is guaranteed to see a valid fn alongside it.
        // Publishing context first and writing fn after (the original
        // shape of this function) raced: another thread could observe the
        // non-null context and call a not-yet-written fn.
        [[nodiscard]] std::size_t register_hook(void* context, poll_fn fn) noexcept {
            for (std::size_t i{}; i < capacity; ++i) {
                bool expected = false;
                if (slots_[i].reserved.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
                    slots_[i].fn = fn;
                    slots_[i].context.store(context, std::memory_order_release);
                    count_.fetch_add(1, std::memory_order_relaxed);
                    return i;
                }
            }
            return capacity;
        }

        void unregister(std::size_t handle) noexcept {
            if (handle >= capacity) return;
            auto& slot = slots_[handle];
            slot.context.store(nullptr, std::memory_order_release);
            // Drain any invocation already past the "context is non-null"
            // check in poll_once() below before we let the caller proceed
            // (typically straight into destroying whatever `context` was).
            while (slot.busy.load(std::memory_order_acquire)) std::this_thread::yield();
            count_.fetch_sub(1, std::memory_order_relaxed);
            // Only now safe to let a future register_hook() reuse this slot
            // and overwrite `fn` -- nothing can still be reading it once
            // context is null and busy has drained.
            slot.reserved.store(false, std::memory_order_release);
        }

        // Invokes registered hooks non-blocking, shaped by `type` -- see
        // execution::task_type. `segment` (default) sweeps every hook in
        // one call, same as always; `individual` advances exactly one,
        // round-robin across calls via cursor_. Either way, returns true
        // if real work got done, so the caller should loop around and
        // re-check its own queues rather than parking.
        [[nodiscard]] bool poll_once(execution::task_type type = execution::task_type::segment) noexcept {
            if (type == execution::task_type::individual) return poll_one();

            bool progressed = false;
            for (auto& slot : slots_) {
                void* context = slot.context.load(std::memory_order_acquire);
                if (!context) continue;

                bool expected = false;
                if (!slot.busy.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) continue;

                // Re-check: context may have been cleared by unregister()
                // between our first load and winning the busy claim.
                if (slot.context.load(std::memory_order_acquire) == context) {
                    if (slot.fn(context)) progressed = true;
                }
                slot.busy.store(false, std::memory_order_release);
            }
            return progressed;
        }

        [[nodiscard]] bool empty() const noexcept { return count_.load(std::memory_order_relaxed) == 0; }

#if FCS_EXPERIMENTAL_ALWAYS_ON
        // In a clone: a hook is still `busy` if the worker polling it was frozen mid-pass.
        // Registrations themselves are kept; the backend owning each decides what to re-arm.
        void clear_busy_after_clone() noexcept { for (auto& hook : slots_) hook.busy.store(false, std::memory_order_relaxed); }
#endif

    private:
        // Advances exactly one hook per call. cursor_ is shared across every
        // caller (soft heuristic, relaxed ops -- a torn round-robin sequence
        // under concurrent callers just means slightly uneven rotation, not
        // a correctness issue) so repeated calls sweep the table over time
        // instead of one caller always starting from slot 0.
        [[nodiscard]] bool poll_one() noexcept {
            for (std::size_t attempts{}; attempts < capacity; ++attempts) {
                const auto i = cursor_.fetch_add(1, std::memory_order_relaxed) % capacity;
                auto& slot = slots_[i];
                void* context = slot.context.load(std::memory_order_acquire);
                if (!context) continue;

                bool expected = false;
                if (!slot.busy.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) continue;

                bool progressed = false;
                if (slot.context.load(std::memory_order_acquire) == context) progressed = slot.fn(context);
                slot.busy.store(false, std::memory_order_release);
                return progressed;
            }
            return false;
        }

        struct slot {
            std::atomic_bool reserved{false};
            poll_fn fn{};
            std::atomic<void*> context{nullptr};
            std::atomic_bool busy{false};
        };

        std::array<slot, capacity> slots_{};
        std::atomic_size_t count_{};
        std::atomic_size_t cursor_{};
    };

}
