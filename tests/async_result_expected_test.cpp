// FCS::async_result<T> / async_result<void>: result() is an FCS::Expected, never a throw.
// The error (FCS::async_failure) carries a `raise` function pointer: the caller decides whether anything is thrown.
//
// Builds both ways: with exceptions (the coroutine's own exception is captured and rethrown on request) and with
// -fno-exceptions (no exception can exist; raise() aborts, which is checked in a child process).
#include <FCS/Worker/workers.hpp>

#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>

#if !defined(_WIN32)
#  include <csignal>
#  include <sys/wait.h>
#  include <unistd.h>
#endif

namespace {
int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

FCS::async_result<int> returns_seven() { co_return 7; }
FCS::async_result<void> returns_nothing() { co_return; }
FCS::async_result<int> suspends_once() { co_await std::suspend_always{}; co_return 3; }

#if FCS_HAS_EXCEPTIONS
FCS::async_result<int> throws_in_body() { throw std::runtime_error("boom"); co_return 0; }
FCS::async_result<void> throws_in_void_body() { throw std::logic_error("void boom"); co_return; }
#endif

int g_custom_calls = 0;
void custom_handler(const FCS::async_failure&) { ++g_custom_calls; }   // returns: the caller's own policy, not a throw
}

int main() {
    static_assert(std::is_same_v<decltype(returns_seven().result()), FCS::Expected<int, FCS::async_failure>>);
    static_assert(std::is_same_v<decltype(returns_nothing().result()), FCS::Expected<std::monostate, FCS::async_failure>>);

    {   std::printf("async_result<int>: value, or a failure as a value\n");
        auto a = returns_seven();
        auto early = a.result();                        // not finished yet: a value, not a throw
        CHECK(!early.has_value());
        CHECK(early.error().code == FCS::Worker::error::not_ready);
        CHECK(early.error().raise != nullptr);
        CHECK(std::strstr(early.error().what, "before the coroutine finished") != nullptr);
        while (a.resume()) {}
        auto done = a.result();
        CHECK(done.has_value() && *done == 7);
        CHECK(FCS::unwrap(a.result()) == 7);

        auto b = suspends_once();
        CHECK(b.resume());                              // parked at the co_await
        CHECK(!b.result().has_value());
        while (b.resume()) {}
        CHECK(b.result().has_value() && *b.result() == 3);

        auto moved_from = returns_seven();
        auto taken = std::move(moved_from);
        auto empty = moved_from.result();               // NOLINT: deliberately inspecting a moved-from handle
        CHECK(!empty.has_value());
        CHECK(empty.error().code == FCS::Worker::error::not_ready);
        CHECK(std::strstr(empty.error().what, "empty (moved-from)") != nullptr);
    }

    {   std::printf("async_result<void>: monostate on success, same failures\n");
        auto v = returns_nothing();
        CHECK(!v.result().has_value());
        while (v.resume()) {}
        CHECK(v.result().has_value());
        auto taken = std::move(v);
        CHECK(!v.result().has_value());                 // NOLINT: moved-from
        CHECK(v.result().error().code == FCS::Worker::error::not_ready);
        CHECK(taken.result().has_value());
    }

    {   std::printf("the caller decides: the pointer is just data\n");
        auto a = returns_seven();
        auto failure = a.result().error();              // unfinished
        failure.raise = &custom_handler;                // substitute a policy of your own ...
        failure.raise(failure);                         // ... and nothing throws, nothing aborts
        CHECK(g_custom_calls == 1);
    }

#if FCS_HAS_EXCEPTIONS
    {   std::printf("exceptions available: the coroutine's own exception rides in the error, thrown only on request\n");
        auto t = throws_in_body();
        while (t.resume()) {}
        auto r = t.result();                            // no throw here, just a value
        CHECK(!r.has_value());
        CHECK(r.error().code == FCS::Worker::error::callback_threw);
        CHECK(r.error().exception != nullptr);
        bool rethrown = false;
        try { r.error().raise(r.error()); } catch (const std::runtime_error& e) { rethrown = std::string{e.what()} == "boom"; }
        CHECK(rethrown);                                // the ORIGINAL exception, original type
        bool unwrapped = false;
        try { (void)FCS::unwrap(t.result()); } catch (const std::runtime_error&) { unwrapped = true; }
        CHECK(unwrapped);

        auto tv = throws_in_void_body();
        while (tv.resume()) {}
        bool void_rethrown = false;
        try { auto e = tv.result(); CHECK(!e.has_value()); e.error().raise(e.error()); } catch (const std::logic_error&) { void_rethrown = true; }
        CHECK(void_rethrown);

        bool misuse = false;                            // misuse raises std::logic_error, like the old throwing result()
        try { auto e = returns_seven().result(); e.error().raise(e.error()); } catch (const std::logic_error&) { misuse = true; }
        CHECK(misuse);
    }
#else
#  if !defined(_WIN32)
    {   std::printf("-fno-exceptions: raise() has nothing to throw -- it aborts (checked in a child process)\n");
        std::fflush(stdout);
        const pid_t child = ::fork();
        if (child == 0) {
            ::close(2);                                 // keep the expected diagnostic out of the log
            auto a = returns_seven();
            auto e = a.result();
            e.error().raise(e.error());
            ::_exit(0);                                 // not reached
        }
        int status = 0;
        (void)::waitpid(child, &status, 0);
        CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
    }
#  endif
#endif

    std::printf(failures ? "async_result_expected: %d FAILED\n" : "async_result_expected: all passed\n", failures);
    return failures ? 1 : 0;
}
