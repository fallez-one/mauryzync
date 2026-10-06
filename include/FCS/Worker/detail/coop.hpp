#pragma once

#include "worker_profile.hpp"

#include <atomic>
#include <coroutine>
#include <type_traits>

namespace FCS::Worker::detail {

    // ---- cooperative scheduling of pool-driven coroutines -------------------------------
    //
    // A plain task is a std::function<void()> and cannot be interrupted. A coroutine can: it
    // can *suspend*. So a coroutine driven by pool_service::enqueue_and_poll() may be asked to
    // give its worker back. The stall watchdog already knows when a worker has been stuck in
    // one task past its tolerance; for a coroutine that same flag now means "yield at your next
    // checkpoint" -- the coroutine suspends, and its remainder is re-queued as a new task
    // (possibly in a lower lane) instead of holding the thread. Checkpoints are
    // `co_await FCS::Worker::yield_point()` (cheap: one relaxed load when not asked) or any
    // plain suspension the coroutine already makes.
    //
    // Because a coroutine may therefore resume on a different worker than it last ran on, every
    // resumption goes through the pool: exactly one thread resumes a given coroutine at a time,
    // and the queue push/pop between two resumptions is the happens-before edge. That includes
    // `co_await async_completed_result`, which used to resume the continuation *inline on the
    // completing thread* while the polling thread's loop could be resuming the same handle --
    // see async_completed_result::await_suspend.

    // Stack record of the pool task currently resuming a coroutine on this thread. An awaiter
    // that hands the coroutine to someone else (re-queue, external wait, completion) sets
    // `handed_off` *before* it releases the handle; the driver, once resume() returns, trusts
    // only this local flag and never touches the (possibly already re-owned) coroutine.
    struct step_frame {
        bool handed_off{false};
    };
    inline thread_local step_frame* tls_step_frame = nullptr;

    // What the current pool worker thread exposes to coroutines running on it.
    struct worker_coop_view {
        const std::atomic_bool* stalled{nullptr};
        worker_steal_profile* profile{nullptr};
        const void* owner{nullptr};   // the pool this worker belongs to (a coroutine may be completed by another pool's worker)
        std::size_t id{0};            // this worker's index in that pool
        bool yielded{false};          // a coroutine just yielded on this thread: the worker loop should look at the global queues before its own deque
    };
    inline thread_local worker_coop_view tls_worker_view{};

    // True when the watchdog has flagged the worker this code runs on.
    [[nodiscard]] inline bool yield_requested() noexcept {
        const auto* flag = tls_worker_view.stalled;
        return flag != nullptr && flag->load(std::memory_order_relaxed);
    }

    // Mixed into the promise of pool-drivable coroutines (FCS::async_result). All null when the
    // coroutine is not being driven by a pool -- then every hook below degrades to plain
    // suspend_always behaviour, so standalone use of async_result is unchanged.
    struct coop_base {
        void* ctl{nullptr};
        // Queue the coroutine's next resumption. forced_yield: it is giving its worker up because
        // the watchdog asked -- it goes into that worker's own deque a few items back (see
        // worker_registry::requeue_behind). Otherwise (woken by an external completion) it is an
        // ordinary task. Returns false when nothing could take it -- every queue bounded and
        // full -- and the caller must then NOT give the coroutine up (keep running / resume inline).
        bool (*reschedule)(void* ctl, bool forced_yield) noexcept {nullptr};
        // Resume it right here instead; the last resort when reschedule() returned false.
        void (*resume_inline)(void* ctl) noexcept {nullptr};
        // Reference counting of the control block, for hand-offs that outlive the current step.
        void (*retain)(void* ctl) noexcept {nullptr};
        void (*release)(void* ctl) noexcept {nullptr};
        // Runs once, at the coroutine's final suspend point, on whichever thread finished it.
        void (*complete)(void* ctl) noexcept {nullptr};

        [[nodiscard]] bool driven() const noexcept { return ctl != nullptr; }
    };

    // final_suspend() for pool-drivable coroutines: still suspends (so done()/result() work as
    // before), but first tells the pool the coroutine finished -- from whatever thread ran its
    // last step, which with cooperative yields is no longer necessarily where it started.
    struct coop_final_awaiter {
        [[nodiscard]] bool await_ready() const noexcept { return false; }
        template<typename Promise>
        void await_suspend(std::coroutine_handle<Promise> handle) const noexcept {
            const auto complete = handle.promise().complete;
            void* const ctl = handle.promise().ctl;
            if (complete == nullptr) return;
            if (tls_step_frame != nullptr) tls_step_frame->handed_off = true;
            complete(ctl); // may drop the last reference; the frame is not touched afterwards
        }
        void await_resume() const noexcept {}
    };

    // A one-shot coroutine used as the *continuation* attached to an external awaitable. When
    // the awaited work completes, scheduled_state resumes this handle inline on the completing
    // thread; all it does is re-queue the real coroutine as a pool task (so the real
    // coroutine is never resumed from inside someone else's set_value), then free itself.
    struct resume_trampoline {
        struct promise_type {
            resume_trampoline get_return_object() noexcept { return resume_trampoline{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() const noexcept { return {}; }
            std::suspend_never final_suspend() const noexcept { return {}; } // frame destroys itself
            void return_void() const noexcept {}
            void unhandled_exception() const noexcept {}
        };
        std::coroutine_handle<promise_type> handle;
    };

    inline resume_trampoline trampoline_body(bool (*reschedule)(void*, bool) noexcept, void (*resume_inline)(void*) noexcept,
                                             void (*release)(void*) noexcept, void* ctl) {
        if (reschedule(ctl, false)) release(ctl);   // the queued step took its own reference; drop this one
        else resume_inline(ctl);                    // every queue full: run it here (step consumes this reference)
        co_return;
    }

    // Builds the trampoline for `promise`'s coroutine, holding one reference on its control block.
    inline resume_trampoline make_resume_trampoline(const coop_base& promise) {
        promise.retain(promise.ctl);
        return trampoline_body(promise.reschedule, promise.resume_inline, promise.release, promise.ctl);
    }

}
