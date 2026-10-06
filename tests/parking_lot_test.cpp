// parking_lot / parker: no lost wakeups, no leaked semaphore tokens, timeouts,
// cancel-by-epoch, wake_all. Run under TSan and ASan.
#include <FCS/Worker/detail/parking_lot.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using FCS::Worker::detail::parker;
using FCS::Worker::detail::parking_lot;
static int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("FAIL %s:%d %s\n", __FILE__, __LINE__, #c); } } while (0)
static double ms_since(std::chrono::steady_clock::time_point t) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t).count();
}

int main() {
    // 1. timeout returns; unpark with no sleeper is a no-op; no token leaks into the next park
    {
        parker p;
        auto t = std::chrono::steady_clock::now();
        p.park(20ms, [] { return false; });
        CHECK(ms_since(t) >= 15.0);
        CHECK(!p.unpark());                       // nobody sleeping
        t = std::chrono::steady_clock::now();
        p.park(20ms, [] { return false; });       // a leaked token would return immediately
        CHECK(ms_since(t) >= 15.0);
    }
    // 2. cancel() short-circuits the sleep
    {
        parker p;
        const auto t = std::chrono::steady_clock::now();
        p.park(2000ms, [] { return true; });
        CHECK(ms_since(t) < 500.0);
    }
    // 3. unpark wakes a sleeper promptly (indefinite park)
    {
        parker p; std::atomic<bool> woke{false};
        std::thread th([&] { p.park(std::nullopt, [] { return false; }); woke = true; });
        while (!p.sleeping()) std::this_thread::yield();
        const auto t = std::chrono::steady_clock::now();
        CHECK(p.unpark());
        th.join();
        CHECK(woke.load());
        CHECK(ms_since(t) < 500.0);
    }
    // 4. the epoch closes the missed-wakeup window; notify_all wakes everyone
    {
        parking_lot<8> lot; lot.configure(4);
        const auto seen = lot.epoch();
        lot.notify_one();                         // lands *before* the park
        const auto t = std::chrono::steady_clock::now();
        lot.park(0, seen, 2000ms, [] { return false; });
        CHECK(ms_since(t) < 500.0);               // must not sleep through it

        std::atomic<int> woke{0};
        std::vector<std::thread> ths;
        for (std::size_t i = 0; i < 4; ++i) ths.emplace_back([&, i] { lot.park(i, lot.epoch(), std::nullopt, [] { return false; }); ++woke; });
        while (lot.sleepers() != 4) std::this_thread::yield();
        lot.notify_all();
        for (auto& th : ths) th.join();
        CHECK(woke.load() == 4);
    }
    // 5. stress: producers publish work + notify_one, consumers park when empty.
    //    Every item must be consumed (no lost wakeup => no hang), and afterwards no
    //    parker holds a leaked token (a final park must actually wait).
    {
        constexpr std::size_t consumers = 6, producers = 3;
        constexpr long per_producer = 60000;
        parking_lot<8> lot; lot.configure(consumers);
        std::atomic<long> published{0}, consumed{0};
        std::atomic<bool> done{false};
        std::vector<std::thread> ths;
        for (std::size_t c = 0; c < consumers; ++c) ths.emplace_back([&, c] {
            for (;;) {
                const auto seen = lot.epoch();
                long n = consumed.load();
                bool took = false;
                while (n < published.load()) { if (consumed.compare_exchange_weak(n, n + 1)) { took = true; break; } }
                if (took) continue;
                if (done.load()) return;
                // Mix of indefinite-with-cancel and timed parks exercises both retract paths.
                lot.park(c, seen, (consumed.load() & 1) ? std::optional<std::chrono::nanoseconds>{200us} : std::nullopt,
                         [&] { return done.load() || consumed.load() < published.load(); });
            }
        });
        for (std::size_t p = 0; p < producers; ++p) ths.emplace_back([&] {
            for (long i = 0; i < per_producer; ++i) {
                published.fetch_add(1);
                lot.notify_one();
                if ((i & 1023) == 0) std::this_thread::yield();
            }
        });
        for (std::size_t i = consumers; i < ths.size(); ++i) ths[i].join();
        const auto total = static_cast<long>(producers) * per_producer;
        const auto deadline = std::chrono::steady_clock::now() + 20s;
        while (consumed.load() < total && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(1ms);
        CHECK(consumed.load() == total);
        done.store(true);
        lot.notify_all();
        for (std::size_t c = 0; c < consumers; ++c) ths[c].join();
        // leaked-token probe
        for (std::size_t c = 0; c < consumers; ++c) {
            const auto t = std::chrono::steady_clock::now();
            lot.park(c, lot.epoch(), 15ms, [] { return false; });
            CHECK(ms_since(t) >= 10.0);
        }
        std::printf("stress: %ld items, all consumed\n", consumed.load());
    }
    std::printf(failures ? "parking_lot_test: %d FAILED\n" : "parking_lot_test: all passed\n", failures);
    return failures ? 1 : 0;
}
