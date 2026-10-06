#pragma once

#include "../expected.hpp"
#include "types.hpp"
#include "detail/coop.hpp"

#include <coroutine>
#include <exception>
#include <stdexcept>
#include <utility>

namespace FCS {

    template<typename T = void>
    struct async_result {
        struct promise_type : Worker::detail::coop_base {
            T value{};
            std::exception_ptr exception;

            async_result get_return_object() { return async_result{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            // Not plain suspend_always: when a pool is driving this coroutine it is told here that
            // the coroutine finished (see detail/coop.hpp). Otherwise identical.
            Worker::detail::coop_final_awaiter final_suspend() noexcept { return {}; }
            void return_value(T v) { value = std::move(v); }
            void unhandled_exception() { exception = std::current_exception(); }
        };

        using handle_type = std::coroutine_handle<promise_type>;
        handle_type handle;

        explicit async_result(handle_type handle) noexcept : handle(handle) {}
        ~async_result() { if (handle) handle.destroy(); }

        async_result(const async_result&) = delete;
        async_result& operator=(const async_result&) = delete;

        async_result(async_result&& other) noexcept : handle(std::exchange(other.handle, {})) {}
        async_result& operator=(async_result&& other) noexcept {
            if (this == &other) return *this;
            if (handle) handle.destroy();
            handle = std::exchange(other.handle, {});
            return *this;
        }

        bool resume() {
            if (handle && !handle.done()) handle.resume();
            return handle && !handle.done();
        }

        // Rethrows if the coroutine completed via an unhandled exception
        // rather than a normal co_return, instead of silently handing back
        // a default-constructed T as though nothing went wrong. Also
        // guards the handle itself: calling this on an empty (moved-from)
        // or not-yet-finished handle used to dereference a null or
        // half-alive coroutine frame — undefined behavior, not something
        // that reliably crashed loudly. Now it's a catchable
        // std::logic_error naming exactly what was called too soon.
        [[nodiscard]] T result() const {
            if (!handle) throw std::logic_error{"FCS::async_result::result() called on an empty (moved-from) handle"};
            if (!handle.done()) throw std::logic_error{"FCS::async_result::result() called before the coroutine finished"};
            if (handle.promise().exception) std::rethrow_exception(handle.promise().exception);
            return handle.promise().value;
        }
    };

    template<>
    struct async_result<void> {
        struct promise_type : Worker::detail::coop_base {
            std::exception_ptr exception;

            async_result get_return_object() { return async_result{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            Worker::detail::coop_final_awaiter final_suspend() noexcept { return {}; }
            void return_void() {}
            void unhandled_exception() { exception = std::current_exception(); }
        };

        using handle_type = std::coroutine_handle<promise_type>;
        handle_type handle;

        explicit async_result(handle_type handle) noexcept : handle(handle) {}
        ~async_result() { if (handle) handle.destroy(); }

        async_result(const async_result&) = delete;
        async_result& operator=(const async_result&) = delete;

        async_result(async_result&& other) noexcept : handle(std::exchange(other.handle, {})) {}
        async_result& operator=(async_result&& other) noexcept {
            if (this == &other) return *this;
            if (handle) handle.destroy();
            handle = std::exchange(other.handle, {});
            return *this;
        }

        bool resume() {
            if (handle && !handle.done()) handle.resume();
            return handle && !handle.done();
        }

        // See async_result<T>::result() — same rethrow, same handle guards,
        // nothing to hand back.
        void result() const {
            if (!handle) throw std::logic_error{"FCS::async_result<void>::result() called on an empty (moved-from) handle"};
            if (!handle.done()) throw std::logic_error{"FCS::async_result<void>::result() called before the coroutine finished"};
            if (handle.promise().exception) std::rethrow_exception(handle.promise().exception);
        }
    };

    template<typename T>
    struct chunk_stream {
        struct promise_type {
            T current_value{};
            std::exception_ptr exception;

            chunk_stream get_return_object() { return chunk_stream{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }

            template<typename U>
            std::suspend_always yield_value(U&& v) { current_value = std::forward<U>(v); return {}; }

            void return_void() noexcept {}
            void unhandled_exception() { exception = std::current_exception(); }
        };

        std::coroutine_handle<promise_type> handle;

        explicit chunk_stream(std::coroutine_handle<promise_type> handle) noexcept : handle(handle) {}
        ~chunk_stream() { if (handle) handle.destroy(); }

        chunk_stream(const chunk_stream&) = delete;
        chunk_stream& operator=(const chunk_stream&) = delete;

        chunk_stream(chunk_stream&& other) noexcept : handle(std::exchange(other.handle, {})) {}
        chunk_stream& operator=(chunk_stream&& other) noexcept {
            if (this == &other) return *this;
            if (handle) handle.destroy();
            handle = std::exchange(other.handle, {});
            return *this;
        }

        bool move_next() {
            if (handle && !handle.done()) {
                handle.resume();
                // The coroutine ran to its final suspend point via an
                // unhandled exception rather than falling off the end
                // normally — surface it here, at the call that caused it,
                // instead of leaving it sitting unreported in the promise
                // forever.
                if (handle.promise().exception) std::rethrow_exception(std::exchange(handle.promise().exception, nullptr));
            }
            return handle && !handle.done();
        }

        [[nodiscard]] T& value() const {
            if (!handle) throw std::logic_error{"FCS::chunk_stream::value() called on an empty (moved-from) handle"};
            return handle.promise().current_value;
        }
    };

    // Pull-based counterpart to chunk_stream<T>, for a coroutine that
    // co_returns a single value instead of co_yielding several: pull()
    // drives it to completion and hands back the value in one call,
    // error-as-values (FCS::Expected<T, Worker::error>) rather than
    // async_result<T>'s throwing result(). Meant for standalone use --
    // written and consumed within one thread's own code, same as
    // chunk_stream<T>. Never hand a pull_result<T> to another thread to
    // resume/pull: like any coroutine handle, it isn't safe to drive from
    // more than one place. Pool-driven completions that DO need to cross
    // threads go through pool_service::enqueue_and_poll() instead, which
    // accepts a callable returning async_result<T> and pumps it to
    // completion entirely on one worker before handing the result to the
    // properly synchronized async_completed_result<T>.
    template<typename T>
    struct pull_result {
        struct promise_type {
            T value{};
            std::exception_ptr exception;

            pull_result get_return_object() { return pull_result{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }
            void return_value(T v) { value = std::move(v); }
            void unhandled_exception() { exception = std::current_exception(); }
        };

        using handle_type = std::coroutine_handle<promise_type>;
        handle_type handle;

        explicit pull_result(handle_type handle) noexcept : handle(handle) {}
        ~pull_result() { if (handle) handle.destroy(); }

        pull_result(const pull_result&) = delete;
        pull_result& operator=(const pull_result&) = delete;

        pull_result(pull_result&& other) noexcept : handle(std::exchange(other.handle, {})) {}
        pull_result& operator=(pull_result&& other) noexcept {
            if (this == &other) return *this;
            if (handle) handle.destroy();
            handle = std::exchange(other.handle, {});
            return *this;
        }

        // Drives the coroutine to completion, entirely within this call, on
        // this thread -- never partially resumed and left for anyone else
        // to pick back up. Worker::error::not_ready covers an empty
        // (moved-from) handle; Worker::error::callback_threw covers an
        // unhandled exception from the coroutine body.
        [[nodiscard]] Expected<T, Worker::error> pull() {
            if (!handle) return Unexpected{Worker::error::not_ready};
            while (!handle.done()) handle.resume();
            if (handle.promise().exception) return Unexpected{Worker::error::callback_threw};
            return handle.promise().value;
        }
    };

}
