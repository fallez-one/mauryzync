#pragma once

#include "../exceptions.hpp"
#include "../expected.hpp"
#include "types.hpp"
#include "detail/coop.hpp"

#include <coroutine>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <utility>
#include <variant>

namespace FCS {
    struct async_failure {
        using raise_fn = void (*)(const async_failure&);

        Worker::error code{Worker::error::not_ready};   // not_ready: no coroutine, or it has not finished.
                                                        // callback_threw: it ended with an unhandled exception.
        const char* what{""};                           // static text: which call, and what was wrong with it
        raise_fn raise{nullptr};                        // see above; never null on a value the library returns
#if FCS_HAS_EXCEPTIONS
        std::exception_ptr exception{};                 // the coroutine's exception, for code == callback_threw
#endif
    };

    namespace async_detail {   // (not FCS::detail: sibling FCS libraries may own that name)
#if FCS_HAS_EXCEPTIONS
        // Not marked noexcept: throwing is the point.
        [[noreturn]] inline void raise_by_throwing(const async_failure& failure) {
            if (failure.exception) std::rethrow_exception(failure.exception);
            throw std::logic_error{failure.what};
        }
        inline constexpr async_failure::raise_fn default_raise = &raise_by_throwing;
#else
        [[noreturn]] inline void raise_by_aborting(const async_failure& failure) noexcept {
            std::fputs(failure.what, stderr);
            std::fputc('\n', stderr);
            std::abort();
        }
        inline constexpr async_failure::raise_fn default_raise = &raise_by_aborting;
#endif

        [[nodiscard]] inline Unexpected<async_failure> async_misuse(const char* what) noexcept {
            return Unexpected<async_failure>{async_failure{Worker::error::not_ready, what, default_raise}};
        }
    }

    // The old throwing result(), for a caller who wants it back: the value, or raise().
    //   auto v = FCS::unwrap(coro.result());     // throws (or aborts, -fno-exceptions) on failure
    // Spelled out at the call site on purpose -- a library that can be built without exceptions
    // should not decide on its own that a failure is fatal.
    template<typename T>
    [[nodiscard]] T unwrap(Expected<T, async_failure>&& result) {
        if (!result.has_value()) result.error().raise(result.error());
        return std::move(result.value());
    }

    template<typename T = void>
    struct async_result {
        struct promise_type : Worker::detail::coop_base {
            T value{};
#if FCS_HAS_EXCEPTIONS
            std::exception_ptr exception;
#endif

            async_result get_return_object() { return async_result{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            // Not plain suspend_always: when a pool is driving this coroutine it is told here that
            // the coroutine finished (see detail/coop.hpp). Otherwise identical.
            Worker::detail::coop_final_awaiter final_suspend() noexcept { return {}; }
            void return_value(T v) { value = std::move(v); }
            // Defined in every build (some compilers insist on it); with -fno-exceptions it is never called.
            void unhandled_exception() noexcept {
#if FCS_HAS_EXCEPTIONS
                exception = std::current_exception();
#endif
            }
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

        // The coroutine's value, or why there isn't one -- as a value, never as a throw:
        //   empty (moved-from) handle     -> error.code == not_ready
        //   called before it finished     -> error.code == not_ready
        //   ended on an unhandled exception -> error.code == callback_threw
        // A caller who wants the old throwing behaviour calls error().raise(error()), or
        // FCS::unwrap(result()); one who does not simply never does. Builds with -fno-exceptions
        // compile, and never have an exception to report.
        [[nodiscard]] Expected<T, async_failure> result() const noexcept(std::is_nothrow_copy_constructible_v<T>) {
            if (!handle) return async_detail::async_misuse("FCS::async_result::result() called on an empty (moved-from) handle");
            if (!handle.done()) return async_detail::async_misuse("FCS::async_result::result() called before the coroutine finished");
#if FCS_HAS_EXCEPTIONS
            if (handle.promise().exception) {
                return Unexpected<async_failure>{async_failure{Worker::error::callback_threw, "FCS::async_result: the coroutine ended with an unhandled exception", async_detail::default_raise, handle.promise().exception}};
            }
#endif
            return handle.promise().value;
        }
    };

    template<>
    struct async_result<void> {
        struct promise_type : Worker::detail::coop_base {
#if FCS_HAS_EXCEPTIONS
            std::exception_ptr exception;
#endif

            async_result get_return_object() { return async_result{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            Worker::detail::coop_final_awaiter final_suspend() noexcept { return {}; }
            void return_void() {}
            void unhandled_exception() noexcept {
#if FCS_HAS_EXCEPTIONS
                exception = std::current_exception();
#endif
            }
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

        // See async_result<T>::result(): same three outcomes, same raise(). Expected has no void
        // specialisation, so "succeeded, nothing to hand back" is std::monostate.
        [[nodiscard]] Expected<std::monostate, async_failure> result() const noexcept {
            if (!handle) return async_detail::async_misuse("FCS::async_result<void>::result() called on an empty (moved-from) handle");
            if (!handle.done()) return async_detail::async_misuse("FCS::async_result<void>::result() called before the coroutine finished");
#if FCS_HAS_EXCEPTIONS
            if (handle.promise().exception) {
                return Unexpected<async_failure>{async_failure{Worker::error::callback_threw, "FCS::async_result<void>: the coroutine ended with an unhandled exception", async_detail::default_raise, handle.promise().exception}};
            }
#endif
            return std::monostate{};
        }
    };

    template<typename T>
    struct chunk_stream {
        struct promise_type {
            T current_value{};
#if FCS_HAS_EXCEPTIONS
            std::exception_ptr exception;
#endif

            chunk_stream get_return_object() { return chunk_stream{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }

            template<typename U>
            std::suspend_always yield_value(U&& v) { current_value = std::forward<U>(v); return {}; }

            void return_void() noexcept {}
            void unhandled_exception() noexcept {
#if FCS_HAS_EXCEPTIONS
                exception = std::current_exception();
#endif
            }
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
                // forever. (Only exists where exceptions do; chunk_stream keeps its throwing
                // contract in builds that have them -- it is the one coroutine type that has
                // not been moved to Expected.)
#if FCS_HAS_EXCEPTIONS
                if (handle.promise().exception) std::rethrow_exception(std::exchange(handle.promise().exception, nullptr));
#endif
            }
            return handle && !handle.done();
        }

        [[nodiscard]] T& value() const {
            // Precondition: a live handle. Violated: throws std::logic_error where exceptions
            // exist, aborts where they do not.
            if (!handle) {
#if FCS_HAS_EXCEPTIONS
                throw std::logic_error{"FCS::chunk_stream::value() called on an empty (moved-from) handle"};
#else
                async_detail::raise_by_aborting(async_failure{Worker::error::not_ready, "FCS::chunk_stream::value() called on an empty (moved-from) handle", async_detail::default_raise});
#endif
            }
            return handle.promise().current_value;
        }
    };

    // Pull-based counterpart to chunk_stream<T>, for a coroutine that
    // co_returns a single value instead of co_yielding several: pull()
    // drives it to completion and hands back the value in one call,
    // error-as-values (FCS::Expected<T, Worker::error>) -- as async_result<T>::result() now
    // is too, with a richer error (async_failure). Meant for standalone use --
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
#if FCS_HAS_EXCEPTIONS
            std::exception_ptr exception;
#endif

            pull_result get_return_object() { return pull_result{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            std::suspend_always final_suspend() noexcept { return {}; }
            void return_value(T v) { value = std::move(v); }
            void unhandled_exception() noexcept {
#if FCS_HAS_EXCEPTIONS
                exception = std::current_exception();
#endif
            }
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
#if FCS_HAS_EXCEPTIONS
            if (handle.promise().exception) return Unexpected{Worker::error::callback_threw};
#endif
            return handle.promise().value;
        }
    };

}
