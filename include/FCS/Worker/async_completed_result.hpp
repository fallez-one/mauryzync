#pragma once

#include "../expected.hpp"
#include "detail/coop.hpp"
#include "detail/scheduled_result.hpp"
#include "types.hpp"

#include <coroutine>
#include <cstdint>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>

namespace FCS::Worker::detail {

    // What a scheduled_state<completed_payload<R>> actually carries: the
    // callable's result (or why there isn't one -- see FCS::Worker::error)
    // plus which worker thread produced it. `worker_id` is only meaningful
    // when `value` holds a value. `sequence` is known synchronously at
    // enqueue_and_poll()'s call site (assignment order, not completion
    // order) so it lives on async_completed_result itself, not here.
    template<typename R>
    struct completed_payload {
        Expected<R, error> value;
        std::thread::id worker_id{};
    };

}

namespace FCS::Worker {

    // Future returned by pool_service::enqueue_and_poll() -- distinct from
    // scheduled_result<T> (the plain internal future behind enqueue_stream()/
    // enqueue_completion()) in that it also carries scheduling metadata and
    // exposes it as plain accessors, not just through co_await, and never
    // throws: a not-yet-ready read is FCS::Unexpected{error::not_ready},
    // same convention as the rest of this library.
    //
    // Directly co_await-able, reusing scheduled_state's single-continuation
    // handoff: awaiting suspends the caller and whichever worker thread
    // finishes the work resumes it directly from inside set_value(), so the
    // continuation runs on that worker, not necessarily the one that started
    // the await -- migration is the mechanism, not a side effect.
    template<typename T, typename R>
    class async_completed_result {
    public:
        explicit async_completed_result(detail::intrusive_ptr<detail::scheduled_state<detail::completed_payload<R>>> state, std::uint64_t sequence) noexcept
            : state_(std::move(state)), sequence_(sequence) {}

        [[nodiscard]] bool ready() const noexcept { return cached_.has_value() || state_->is_ready(); }

        [[nodiscard]] bool await_ready() const noexcept { return ready(); }
        // Templated on the awaiting coroutine's promise so a coroutine being driven by the pool
        // (enqueue_and_poll) can be handled safely: instead of attaching the coroutine handle
        // itself -- which scheduled_state would resume *inline on whichever thread completes
        // the work*, racing the polling loop and any other resumer -- a driven coroutine
        // attaches a trampoline that re-queues it as a pool task. Everyone else (a plain
        // user coroutine) keeps the original direct hand-off.
        template<typename Promise>
        bool await_suspend(std::coroutine_handle<Promise> handle) noexcept {
            if constexpr (std::is_base_of_v<detail::coop_base, Promise>) {
                auto& promise = handle.promise();
                auto* const frame = detail::tls_step_frame;
                if (promise.driven() && frame != nullptr) {
                    auto trampoline = detail::make_resume_trampoline(promise);   // holds a reference on the control block
                    const auto release = promise.release;
                    void* const ctl = promise.ctl;
                    frame->handed_off = true;                                    // before the handle can be re-owned
                    if (state_->attach(trampoline.handle)) return true;          // suspended; completion will re-queue it
                    frame->handed_off = false;                                   // already ready: keep running, no hand-off
                    trampoline.handle.destroy();
                    release(ctl);
                    return false;
                }
            }
            return state_->attach(handle);
        }
        // Protocol-guaranteed ready by the time this runs -- await_ready()
        // was already true, or a suspended continuation only resumes from
        // inside set_value() -- so there's no not-ready case to report.
        // Still an Expected: the work itself may have failed (see
        // FCS::Worker::error), and that's the caller's to handle, not ours
        // to throw over.
        Expected<R, error> await_resume() { ensure_cached(); return std::move(cached_->value); }

        // user_data()/worker_id(): Unexpected{error::not_ready} before
        // ready() -- via co_await already having run, or a manual wait --
        // otherwise carry whatever the work produced (or its own error).
        // sequence() needs neither: it's fixed at enqueue_and_poll()'s call
        // site. Requires R copy-constructible: unlike await_resume() (which
        // moves out once), these can be called repeatedly.
        [[nodiscard]] Expected<R, error> user_data() {
            if (!ready()) return Unexpected{error::not_ready};
            ensure_cached();
            return cached_->value;
        }
        [[nodiscard]] Expected<std::thread::id, error> worker_id() {
            if (!ready()) return Unexpected{error::not_ready};
            ensure_cached();
            return cached_->worker_id;
        }
        [[nodiscard]] std::uint64_t sequence() const noexcept { return sequence_; }

    private:
        void ensure_cached() { if (!cached_) cached_.emplace(state_->take_value()); }

        detail::intrusive_ptr<detail::scheduled_state<detail::completed_payload<R>>> state_;
        std::optional<detail::completed_payload<R>> cached_{};
        std::uint64_t sequence_;
    };

}
