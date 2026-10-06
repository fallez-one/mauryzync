// Pool traits: a custom semaphore type, a tiny timer table, and the concept that guards the
// semaphore. (The admission gate is not pluggable.)
#include <FCS/Worker/workers.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <semaphore>
#include <thread>

namespace {
using namespace std::chrono_literals;
namespace W = FCS::Worker;
namespace D = FCS::Worker::detail;
int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

// ---- a semaphore that is not std::binary_semaphore: wraps one and counts what the pool does to it
std::atomic<long> g_releases{0}, g_timed_waits{0};
class counting_semaphore_double {
public:
    explicit counting_semaphore_double(std::ptrdiff_t initial) : inner_(initial) {}
    void release() { g_releases.fetch_add(1, std::memory_order_relaxed); inner_.release(); }
    void acquire() { inner_.acquire(); }
    template<typename Rep, typename Period>
    bool try_acquire_for(const std::chrono::duration<Rep, Period>& d) { g_timed_waits.fetch_add(1, std::memory_order_relaxed); return inner_.try_acquire_for(d); }
private:
    std::binary_semaphore inner_;
};
static_assert(W::BinarySemaphore<std::binary_semaphore>);
static_assert(W::BinarySemaphore<counting_semaphore_double>);
static_assert(!W::BinarySemaphore<int>);

struct custom_traits : W::default_pool_traits {
    using semaphore = counting_semaphore_double;
    static constexpr std::size_t timer_capacity = 2;
};

void test_custom_semaphore() {
    std::printf("custom semaphore type\n");
    W::pool_service<1024, custom_traits> pool;
    pool.concurrency(3).start();
    constexpr int n = 5000;
    std::atomic<int> ran{0};
    for (int i = 0; i < n; ++i) while (!pool.enqueue([&ran] { ran.fetch_add(1, std::memory_order_relaxed); })) std::this_thread::yield();
    for (int spin = 0; spin < 20000 && ran.load() < n; ++spin) std::this_thread::sleep_for(1ms);
    std::this_thread::sleep_for(20ms);   // let workers park again
    std::printf("   ran %d; semaphore releases=%ld timed waits=%ld\n", ran.load(), g_releases.load(), g_timed_waits.load());
    CHECK(ran.load() == n);
    CHECK(g_releases.load() > 0 || g_timed_waits.load() > 0);   // the pool really used OUR semaphore type
    pool.stop();
}

void test_tiny_timer_heap() {
    std::printf("timer capacity comes from the traits and is enforced\n");
    W::pool_service<1024, custom_traits> pool;        // timer_capacity = 2
    pool.concurrency(2).start();
    std::atomic<int> fired{0};
    const auto fire = [&fired] { fired.fetch_add(1); };
    CHECK(pool.enqueue_until(W::timeout_mode::timer, 300ms, fire));
    CHECK(pool.enqueue_until(W::timeout_mode::timer, 300ms, fire));
    CHECK(!pool.enqueue_until(W::timeout_mode::timer, 300ms, fire));   // third: table full -> refused, not queued unboundedly
    for (int spin = 0; spin < 2000 && fired.load() < 2; ++spin) std::this_thread::sleep_for(1ms);
    CHECK(fired.load() == 2);
    CHECK(pool.enqueue_until(W::timeout_mode::timer, 10ms, fire));     // slots free again once they fired
    for (int spin = 0; spin < 2000 && fired.load() < 3; ++spin) std::this_thread::sleep_for(1ms);
    CHECK(fired.load() == 3);
    pool.stop();
}

// ---- task size is a per-pool template parameter, not a global
struct tiny_task_traits : W::default_pool_traits { static constexpr std::size_t task_bytes = 64;  static constexpr std::size_t callback_bytes = 96; };
struct big_task_traits  : W::default_pool_traits { static constexpr std::size_t task_bytes = 1024; };

void test_task_size_per_pool() {
    std::printf("task size is a per-pool template parameter\n");
    using tiny_pool = W::pool_service<1024, tiny_task_traits>;
    using big_pool = W::pool_service<1024, big_task_traits>;
    static_assert(sizeof(D::basic_queued_task<64>) < sizeof(D::basic_queued_task<384>));
    static_assert(sizeof(D::basic_queued_task<1024>) > sizeof(D::basic_queued_task<384>));
    std::printf("   queued_task: %zu bytes (tiny pool), %zu (default), %zu (big pool)\n",
                sizeof(D::basic_queued_task<64>), sizeof(D::basic_queued_task<384>), sizeof(D::basic_queued_task<1024>));
    {   // a 64-byte pool runs its (small) work
        tiny_pool pool; pool.concurrency(2).start();
        std::atomic<int> ran{0};
        for (int i = 0; i < 2000; ++i) while (!pool.enqueue([&ran] { ran.fetch_add(1); })) std::this_thread::yield();
        for (int spin = 0; spin < 5000 && ran.load() < 2000; ++spin) std::this_thread::sleep_for(1ms);
        CHECK(ran.load() == 2000);
        pool.stop();
    }
    {   // a 1 KB pool runs work that carries 600 bytes of state -- inline, no allocation, no boxed()
        big_pool pool; pool.concurrency(2).start();
        std::atomic<long> sum{0};
        struct payload { unsigned char bytes[600]; };
        for (int i = 0; i < 500; ++i) {
            payload p{}; p.bytes[0] = 1; p.bytes[599] = 2;
            while (!pool.enqueue([&sum, p] { sum.fetch_add(p.bytes[0] + p.bytes[599]); })) std::this_thread::yield();
        }
        for (int spin = 0; spin < 5000 && sum.load() < 1500; ++spin) std::this_thread::sleep_for(1ms);
        CHECK(sum.load() == 1500);
        pool.stop();
    }
    {   // the same 600-byte work in the DEFAULT pool needs the explicit, visible escape hatch
        W::pool_service<> pool; pool.concurrency(2).start();
        std::atomic<long> sum{0};
        struct payload { unsigned char bytes[600]; };
        payload p{}; p.bytes[0] = 7;
        while (!pool.enqueue(W::boxed([&sum, p] { sum.fetch_add(p.bytes[0]); }))) std::this_thread::yield();
        for (int spin = 0; spin < 2000 && sum.load() < 7; ++spin) std::this_thread::sleep_for(1ms);
        CHECK(sum.load() == 7);
        pool.stop();
    }
}

void test_default_pool_still_default() {
    std::printf("the default pool (no traits) is unchanged\n");
    W::pool_service<> pool;
    pool.concurrency(2).start();
    std::atomic<int> ran{0};
    for (int i = 0; i < 1000; ++i) while (!pool.enqueue([&ran] { ran.fetch_add(1); })) std::this_thread::yield();
    for (int spin = 0; spin < 5000 && ran.load() < 1000; ++spin) std::this_thread::sleep_for(1ms);
    CHECK(ran.load() == 1000);
    pool.stop();
}

} // namespace

int main() {
    test_custom_semaphore();
    test_tiny_timer_heap();
    test_task_size_per_pool();
    test_default_pool_still_default();
    std::printf(failures ? "pool_traits_test: %d FAILED\n" : "pool_traits_test: all passed\n", failures);
    return failures ? 1 : 0;
}
