// ordered_mutex: FIFO order, mutual exclusion across ticket wrap-around, interrupt() cancelling every
// queued waiter at once, plain lock() waiters surviving it, no lost wake-ups, and the interrupt-vs-hand-over
// race (a waiter whose turn has just come must neither be cancelled into a stuck lock nor enter twice).
#include <FCS/Worker/sync/ordered_mutex.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using FCS::synchronization::ordered_mutex;
static_assert(FCS::synchronization::InterruptibleMutexTrait<ordered_mutex<std::uint8_t>>);
static_assert(sizeof(ordered_mutex<std::uint8_t>) == 4, "uint8_t tickets: the whole mutex is one 32-bit word");
static_assert(sizeof(ordered_mutex<std::uint16_t>) == 8, "uint16_t tickets: the whole mutex is one 64-bit word");
// The storage budget is 1 (state) + 2*N (counters) bytes of information; the checked policy must not add to it.
static_assert(ordered_mutex<std::uint8_t>::information_bytes == 3 && ordered_mutex<std::uint16_t>::information_bytes == 5);
static_assert(sizeof(ordered_mutex<std::uint8_t, FCS::synchronization::capacity_policy::block>) == sizeof(ordered_mutex<std::uint8_t>));
static_assert(sizeof(ordered_mutex<std::uint16_t, FCS::synchronization::capacity_policy::block>) == sizeof(ordered_mutex<std::uint16_t>));
static_assert(ordered_mutex<std::uint8_t>::generation_bits >= 5);
static int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("FAIL %s:%d %s\n", __FILE__, __LINE__, #c); } } while (0)

// kills a hung test instead of letting it hang the CI: a lost wake-up IS a hang
struct watchdog {
    std::atomic<bool> done{false};
    std::thread t{[this] {
        for (int i = 0; i < 600 && !done.load(); ++i) std::this_thread::sleep_for(100ms);
        if (!done.load()) { std::printf("HANG: a waiter was never woken\n"); std::fflush(stdout); std::_Exit(3); }
    }};
    ~watchdog() { done = true; t.join(); }
};

template<typename M>
void run_all(const char* name) {
    std::printf("== %s\n", name);
    {   // try_lock / locking
        M m;
        CHECK(!m.locking());
        CHECK(m.try_lock());
        CHECK(m.locking());
        CHECK(!m.try_lock());
        m.unlock();
        CHECK(!m.locking());
    }
    {   // strict FIFO: threads queue up one at a time and must enter in exactly that order
        M m; m.lock();
        std::vector<int> order; std::vector<std::thread> ts;
        for (int i = 0; i < 6; ++i) {
            ts.emplace_back([&, i] { m.lock(); order.push_back(i); m.unlock(); });
            std::this_thread::sleep_for(40ms);               // ticket order == i
        }
        m.unlock();
        for (auto& t : ts) t.join();
        bool fifo = order.size() == 6;
        for (std::size_t i = 0; i < order.size(); ++i) fifo = fifo && order[i] == static_cast<int>(i);
        CHECK(fifo);
    }
    {   // mutual exclusion + no lost wake-up under heavy contention, tickets wrap many times (uint8_t: every 256)
        watchdog wd;
        M m; long counter = 0; constexpr int threads = 8, per = 30000;
        std::vector<std::thread> ts;
        for (int t = 0; t < threads; ++t) ts.emplace_back([&] { for (int i = 0; i < per; ++i) { m.lock(); ++counter; m.unlock(); } });
        for (auto& t : ts) t.join();
        CHECK(counter == static_cast<long>(threads) * per);
        CHECK(!m.locking());
    }
    {   // interrupt() cancels EVERY queued lock_interruptible() at once, while the holder is still inside
        M m; m.lock();
        std::atomic<int> returned{0}; std::vector<std::thread> ws;
        for (int i = 0; i < 5; ++i) { ws.emplace_back([&] { if (!m.lock_interruptible()) returned.fetch_add(1); }); std::this_thread::sleep_for(25ms); }
        m.interrupt();
        std::this_thread::sleep_for(250ms);
        CHECK(returned.load() == 5);                          // all of them, immediately -- the holder still holds
        CHECK(m.locking());                                   // and the holder is undisturbed
        CHECK(!m.try_lock());
        m.unlock();
        for (auto& t : ws) t.join();
        CHECK(!m.locking());                                  // counters were reset consistently: nothing leaked
        CHECK(m.try_lock()); m.unlock();
        CHECK(!m.lock_interruptible());                       // still interrupted
        m.reset_interrupt();
        CHECK(m.lock_interruptible()); m.unlock();
    }
    {   // plain lock() masks the interrupt: its waiters are not cancelled, they re-queue and still get in
        watchdog wd;
        M m; m.lock();
        std::atomic<int> got{0}; std::vector<std::thread> ws;
        for (int i = 0; i < 4; ++i) { ws.emplace_back([&] { m.lock(); got.fetch_add(1); m.unlock(); }); std::this_thread::sleep_for(25ms); }
        m.interrupt();
        std::this_thread::sleep_for(100ms);
        CHECK(got.load() == 0);                               // still waiting for the holder
        CHECK(m.is_interrupted());                            // the interrupt is visible, just not acted on by lock()
        m.unlock();
        for (auto& t : ws) t.join();
        CHECK(got.load() == 4);
        CHECK(!m.locking());
    }
    {   // an interrupted waiter in the MIDDLE must not break the hand-over for the ones behind it
        watchdog wd;
        M m; m.lock();
        std::atomic<int> got{0}; std::vector<std::thread> ts;
        ts.emplace_back([&] { m.lock(); got.fetch_add(1); m.unlock(); });           // ahead
        std::this_thread::sleep_for(40ms);
        std::atomic<bool> middle_false{false};
        ts.emplace_back([&] { middle_false = !m.lock_interruptible(); });            // middle: will be interrupted
        std::this_thread::sleep_for(40ms);
        ts.emplace_back([&] { m.lock(); got.fetch_add(1); m.unlock(); });           // behind
        std::this_thread::sleep_for(40ms);
        m.interrupt();
        std::this_thread::sleep_for(60ms);
        m.unlock();
        for (auto& t : ts) t.join();
        CHECK(middle_false.load());
        CHECK(got.load() == 2);
        CHECK(!m.locking());
    }
    {   // the abandon / hand-over race, hammered: interrupts toggling while others lock; must stay exclusive and never deadlock
        watchdog wd;
        M m; std::atomic<bool> stop{false}; long inside = 0, max_inside = 0; std::atomic<long> entries{0};
        std::vector<std::thread> ts;
        for (int t = 0; t < 6; ++t) ts.emplace_back([&, t] {
            while (!stop.load()) {
                const bool ok = (t % 3 == 0) ? (m.lock(), true) : m.lock_interruptible();
                if (!ok) { std::this_thread::yield(); continue; }
                ++inside; if (inside > max_inside) max_inside = inside;
                entries.fetch_add(1);
                --inside;
                m.unlock();
            }
        });
        for (int i = 0; i < 400; ++i) { m.interrupt(); std::this_thread::sleep_for(150us); m.reset_interrupt(); std::this_thread::sleep_for(150us); }
        stop = true;
        for (auto& t : ts) t.join();
        CHECK(max_inside <= 1);
        CHECK(entries.load() > 0);
        CHECK(!m.locking());
        CHECK(m.try_lock()); m.unlock();
    }
}

// capacity_policy::block: 400 threads on a uint8_t ticket (255 numbers). Without the policy this aliases ticket == serving.
void test_blocking_capacity() {
    std::printf("== ordered_mutex<uint8_t, block>: 400 threads on 255 tickets\n");
    using M = FCS::synchronization::ordered_mutex<std::uint8_t, FCS::synchronization::capacity_policy::block>;
    static_assert(FCS::synchronization::InterruptibleMutexTrait<M>);
    watchdog wd;
    M m; long counter = 0; int inside = 0; std::atomic<int> violations{0};
    constexpr int threads = 400, per = 200;
    std::vector<std::thread> ts;
    for (int t = 0; t < threads; ++t) ts.emplace_back([&, t] {
        for (int i = 0; i < per; ++i) {
            if (t % 3 == 0) { if (!m.lock_interruptible()) continue; } else m.lock();
            if (++inside != 1) violations.fetch_add(1);
            ++counter;
            if (--inside != 0) violations.fetch_add(1);
            m.unlock();
        }
    });
    std::thread interrupter([&] { for (int i = 0; i < 50; ++i) { m.interrupt(); std::this_thread::sleep_for(2ms); m.reset_interrupt(); std::this_thread::sleep_for(2ms); } });
    interrupter.join();
    for (auto& t : ts) t.join();
    CHECK(violations.load() == 0);
    CHECK(counter > 0);
    CHECK(!m.locking());
    CHECK(m.try_lock()); m.unlock();
}

int main() {
    run_all<ordered_mutex<std::uint8_t>>("ordered_mutex<uint8_t>  (one 32-bit word)");
    run_all<ordered_mutex<std::uint16_t>>("ordered_mutex<uint16_t> (one 64-bit word)");
    run_all<ordered_mutex<std::uint8_t, FCS::synchronization::capacity_policy::block>>("ordered_mutex<uint8_t, block>");
    test_blocking_capacity();
    std::printf(failures ? "ordered_mutex_test: %d FAILED\n" : "ordered_mutex_test: all passed\n", failures);
    return failures ? 1 : 0;
}
