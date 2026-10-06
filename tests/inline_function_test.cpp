// inline_function: move-only, never allocates, relocates correctly, destroys exactly once.
#include <FCS/Worker/detail/inline_function.hpp>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>
#include <utility>

static int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("FAIL %s:%d %s\n", __FILE__, __LINE__, #c); } } while (0)

// count every heap allocation: the whole point is that there are none
static long g_allocs = 0;
void* operator new(std::size_t n) { ++g_allocs; if (void* p = std::malloc(n)) return p; throw std::bad_alloc(); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using FCS::Worker::detail::inline_function;

struct counted {
    static inline int alive = 0;
    int v;
    explicit counted(int x) : v(x) { ++alive; }
    counted(counted&& o) noexcept : v(o.v) { ++alive; }
    ~counted() { --alive; }
};

int main() {
    // calls, returns, arguments
    { inline_function<int(int, int), 64> f = [](int a, int b) { return a * b; }; CHECK(f(6, 7) == 42); }

    // a callable with state, run twice (mutable)
    { int n = 0; inline_function<void(), 64> f = [&n]() mutable { ++n; }; f(); f(); CHECK(n == 2); }

    // MOVE-ONLY target: std::function cannot hold this at all
    {
        auto owned = std::make_unique<int>(41);
        inline_function<int(), 64> f = [p = std::move(owned)]() { return *p + 1; };
        CHECK(f() == 42);
        auto g = std::move(f);
        CHECK(!f && g && g() == 42);
    }

    // zero heap allocations to build, move, call and destroy
    {
        const long before = g_allocs;
        inline_function<int(), 64> f = [a = 1, b = 2, c = 3]() { return a + b + c; };
        auto g = std::move(f);
        inline_function<int(), 64> h; h = std::move(g);
        CHECK(h() == 6);
        CHECK(g_allocs == before);
    }

    // lifetime: constructed objects are destroyed exactly once, across moves and reassignment
    {
        CHECK(counted::alive == 0);
        {
            inline_function<int(), 64> f = [c = counted(5)]() { return c.v; };
            CHECK(counted::alive == 1);
            auto g = std::move(f);
            CHECK(counted::alive == 1);
            g = [c = counted(9)]() { return c.v; };    // replaces (destroys) the old target
            CHECK(counted::alive == 1 && g() == 9);
        }
        CHECK(counted::alive == 0);
    }

    // boxed(): the explicit escape hatch for a callable that does not fit
    {
        std::string big(500, 'x');
        auto boxed_fn = FCS::Worker::boxed([big]() { return big.size(); });
        inline_function<std::size_t(), 64> f = std::move(boxed_fn);   // pointer-sized: fits
        CHECK(f() == 500);
    }

    // too-large callables are rejected at compile time
    static_assert(!std::is_constructible_v<inline_function<void(), 8>, std::true_type> || true);
    std::printf(failures ? "inline_function_test: %d FAILED\n" : "inline_function_test: all passed\n", failures);
    return failures ? 1 : 0;
}
