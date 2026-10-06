#pragma once

#include "detail/coop.hpp"

#include <coroutine>
#include <type_traits>

namespace FCS::Worker {

    // True when the stall watchdog has flagged the worker this code is running on, i.e. the
    // current task has held it past pool_service::watchdog_mark_stall(). Plain callbacks can
    // poll this to wind down voluntarily; coroutines should prefer yield_point().
    [[nodiscard]] inline bool cooperative_yield_requested() noexcept { return detail::yield_requested(); }

    // `co_await FCS::Worker::yield_point();` -- a cooperative checkpoint for a coroutine driven
    // by pool_service::enqueue_and_poll(). If the watchdog has asked this worker to yield, the
    // coroutine suspends and its continuation goes into that worker's own deque a few tasks
    // back (pool_service::coroutine_yield_depth()), so other work runs first; otherwise it
    // costs one relaxed load and does not suspend. Outside a pool-driven coroutine it never
    // suspends, so it is always safe to leave in.
    //
    // Put one in loops that can run long. Between checkpoints the coroutine is exactly as
    // non-preemptible as any function: it cannot be interrupted, only asked.
    struct yield_point_awaiter {
        [[nodiscard]] bool await_ready() const noexcept { return !detail::yield_requested(); }

        template<typename Promise>
        [[nodiscard]] bool await_suspend(std::coroutine_handle<Promise> handle) const noexcept {
            if constexpr (std::is_base_of_v<detail::coop_base, Promise>) {
                auto& promise = handle.promise();
                auto* const frame = detail::tls_step_frame;
                if (!promise.driven() || frame == nullptr) return false;   // not pool-driven: just keep going
                frame->handed_off = true;                                  // before the handle can be re-owned
                if (promise.reschedule(promise.ctl, /*forced_yield=*/true)) return true; // suspended; queued
                frame->handed_off = false;                                 // queue full: don't strand it, keep running
                return false;
            } else {
                return false;
            }
        }

        void await_resume() const noexcept {}
    };

    [[nodiscard]] inline yield_point_awaiter yield_point() noexcept { return {}; }

}
