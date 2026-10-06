// timer_heap: earliest-first, bounded, never early, exactly-once under a concurrent stress (TSan).
#include <FCS/Worker/detail/task.hpp>
#include <FCS/Worker/detail/timer_heap.hpp>

#include <atomic>
#include <memory>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;
using FCS::Worker::detail::task;
template<std::size_t N> using timer_table = FCS::Worker::detail::timer_heap<N, FCS::Worker::detail::task>;
static int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("FAIL %s:%d %s\n", __FILE__, __LINE__, #c); } } while (0)

int main() {
    const auto t0 = clk::now();
    {   // earliest first, only when due, next_deadline tracks the minimum
        timer_table<8> t; std::vector<int> fired;
        CHECK(t.next_deadline() == timer_table<8>::none);
        CHECK(t.add(t0 + 30ms, task{[&] { fired.push_back(30); }}).added);
        auto r = t.add(t0 + 10ms, task{[&] { fired.push_back(10); }});
        CHECK(r.added && r.became_earliest);
        CHECK(!t.add(t0 + 20ms, task{[&] { fired.push_back(20); }}).became_earliest);
        CHECK(t.next_deadline() == (t0 + 10ms).time_since_epoch().count());
        task out;
        CHECK(!t.fire_one(t0 + 5ms, out));                  // nothing due yet
        CHECK(t.fire_one(t0 + 25ms, out)); out();           // 10 first
        CHECK(t.fire_one(t0 + 25ms, out)); out();           // then 20
        CHECK(!t.fire_one(t0 + 25ms, out));                 // 30 not due
        CHECK(t.next_deadline() == (t0 + 30ms).time_since_epoch().count());
        CHECK(t.fire_one(t0 + 31ms, out)); out();
        CHECK(t.next_deadline() == timer_table<8>::none);
        CHECK(fired == (std::vector<int>{10, 20, 30}));
    }
    {   // heap order at scale: 5000 pseudo-random deadlines come out sorted
        // 8192 inline tasks is ~3 MB: far too big for a stack (Windows main threads have ~1 MB), so
        // this is heap-allocated -- as the pool itself does with its bulk storage.
        auto heap = std::make_unique<timer_table<8192>>(); auto& t = *heap; std::vector<long long> order;
        unsigned x = 12345;
        for (int i = 0; i < 5000; ++i) {
            x = x * 1664525u + 1013904223u;
            const auto d = t0 + std::chrono::microseconds(x % 100000);
            CHECK(t.add(d, task{[&order, ticks = d.time_since_epoch().count()] { order.push_back(ticks); }}).added);
        }
        task out;
        while (t.fire_one(t0 + 1s, out)) out();
        CHECK(order.size() == 5000);
        bool sorted = true;
        for (std::size_t i = 1; i < order.size(); ++i) if (order[i] < order[i - 1]) sorted = false;
        CHECK(sorted);
    }
    {   // bounded: refuses when full, recovers after one fires
        timer_table<4> t;
        for (int i = 0; i < 4; ++i) CHECK(t.add(t0 + 1ms, task{[] {}}).added);
        CHECK(!t.add(t0 + 1ms, task{[] {}}).added);
        task out; CHECK(t.fire_one(t0 + 2ms, out));
        CHECK(t.add(t0 + 1ms, task{[] {}}).added);
    }
    {   // concurrent: many adders, many firers -> every timer fires exactly once, none lost
        constexpr int adders = 4, per = 20000, firers = 4;
        timer_table<256> t;
        std::atomic<long> fired{0}, added{0}, early{0};
        std::atomic<bool> done_adding{false};
        std::vector<std::thread> ths;
        for (int a = 0; a < adders; ++a) ths.emplace_back([&] {
            for (int i = 0; i < per; ++i) {
                const auto deadline = clk::now() + std::chrono::microseconds(i % 50);
                // a timer must never run before its own deadline, even while slots are fired and reused under a scanner
                while (!t.add(deadline, task{[&fired, &early, deadline] { if (clk::now() < deadline) early.fetch_add(1); fired.fetch_add(1); }}).added) std::this_thread::yield();
                added.fetch_add(1);
            }
        });
        for (int f = 0; f < firers; ++f) ths.emplace_back([&] {
            task out;
            while (!done_adding.load() || t.next_deadline() != timer_table<256>::none) {
                if (t.fire_one(clk::now(), out)) out(); else std::this_thread::yield();
            }
        });
        for (int a = 0; a < adders; ++a) ths[static_cast<std::size_t>(a)].join();
        done_adding = true;
        for (std::size_t i = adders; i < ths.size(); ++i) ths[i].join();
        std::printf("stress: added=%ld fired=%ld\n", added.load(), fired.load());
        CHECK(added.load() == static_cast<long>(adders) * per);
        CHECK(fired.load() == added.load());
        CHECK(early.load() == 0);
    }
    std::printf(failures ? "timer_heap_test: %d FAILED\n" : "timer_heap_test: all passed\n", failures);
    return failures ? 1 : 0;
}
