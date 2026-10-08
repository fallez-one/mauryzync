// interruptible_mutex: the three bugs a first version had (see the header comment), plus plain
// mutual-exclusion stress and interruption of blocked acquirers. Run under TSan.
#include <FCS/Worker/sync/interruptible_mutex.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using FCS::synchronization::interruptible_lock_guard;
using FCS::synchronization::interruptible_lock_guard_throwable;
using FCS::synchronization::interruptible_mutex;
static int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("FAIL %s:%d %s\n", __FILE__, __LINE__, #c); } } while (0)

int main() {
    {   // 1. interrupt() while held must not let anybody else in
        interruptible_mutex m;
        m.lock();
        m.interrupt();
        std::atomic<bool> got_in{false};
        std::thread b([&] { m.lock(); got_in = true; m.unlock(); });
        std::this_thread::sleep_for(80ms);
        CHECK(!got_in.load());
        m.unlock();
        b.join();
        CHECK(got_in.load());                         // ...and it does get in once the holder leaves
    }
    {   // 2. the throwable guard really holds the mutex
        interruptible_mutex m;
        {
            interruptible_lock_guard_throwable<interruptible_mutex> g{m};
            CHECK(!m.try_lock());
        }
        CHECK(m.try_lock());
        m.unlock();
    }
    {   // 3. the interruption is sticky: unlock() does not erase it; reset_interrupt() does
        interruptible_mutex m;
        m.lock(); m.interrupt(); m.unlock();
        CHECK(m.is_interrupted());
        CHECK(!m.lock_interruptible());
        {
            interruptible_lock_guard<interruptible_mutex> g{m};
            CHECK(!g.owns_lock());
        }
        bool threw = false;
        try { interruptible_lock_guard_throwable<interruptible_mutex> g{m}; } catch (const std::system_error&) { threw = true; }
        CHECK(threw);
        CHECK(m.try_lock()); m.unlock();              // an interrupted guard released nothing it did not hold
        m.reset_interrupt();
        CHECK(!m.is_interrupted());
        CHECK(m.lock_interruptible()); m.unlock();
    }
    {   // 4. interrupt() wakes acquirers blocked in lock_interruptible()
        interruptible_mutex m;
        m.lock();
        std::atomic<int> returned_false{0};
        std::vector<std::thread> waiters;
        for (int i = 0; i < 4; ++i) waiters.emplace_back([&] { if (!m.lock_interruptible()) returned_false.fetch_add(1); });
        std::this_thread::sleep_for(60ms);
        m.interrupt();
        for (auto& t : waiters) t.join();
        CHECK(returned_false.load() == 4);
        m.unlock();
    }
    {   // 4b. lock() wakes on interrupt() but masks it: it keeps waiting, still acquires, and the
        //     interruption is visible only through is_interrupted()
        interruptible_mutex m;
        m.lock();
        std::atomic<bool> acquired{false}, saw_interrupt{false};
        std::thread w([&] { m.lock(); acquired = true; saw_interrupt = m.is_interrupted(); m.unlock(); });
        std::this_thread::sleep_for(60ms);
        m.interrupt();
        std::this_thread::sleep_for(60ms);
        CHECK(!acquired.load());                      // interrupt() did not make lock() give up or barge in
        m.unlock();
        w.join();
        CHECK(acquired.load() && saw_interrupt.load());
    }
    {   // 4c. locking() reflects the held state (and ignores the interrupt flag)
        interruptible_mutex m;
        CHECK(!m.locking());
        m.lock();
        CHECK(m.locking());
        m.interrupt();
        CHECK(m.locking());                           // still held
        m.unlock();
        CHECK(!m.locking());                          // released, though still interrupted
        CHECK(m.is_interrupted());
    }
    {   // 4d. no lost wakeup across a contention episode: many sleepers, all get through
        interruptible_mutex m;
        m.lock();
        std::atomic<int> through{0};
        std::vector<std::thread> ts;
        for (int i = 0; i < 8; ++i) ts.emplace_back([&] { m.lock(); through.fetch_add(1); m.unlock(); });
        std::this_thread::sleep_for(80ms);
        m.unlock();
        for (auto& t : ts) t.join();
        CHECK(through.load() == 8);
        CHECK(!m.locking());
    }
    {   // 5. mutual exclusion under contention (non-atomic counter: TSan + the final count prove it)
        interruptible_mutex m;
        long counter = 0;
        constexpr int threads = 6, per = 20000;
        std::vector<std::thread> ts;
        for (int t = 0; t < threads; ++t) ts.emplace_back([&, t] {
            for (int i = 0; i < per; ++i) {
                if (t % 2) { m.lock(); ++counter; m.unlock(); }
                else { interruptible_lock_guard<interruptible_mutex> g{m}; if (g.owns_lock()) ++counter; }
            }
        });
        for (auto& t : ts) t.join();
        CHECK(counter == static_cast<long>(threads) * per);
    }
    {   // 6. interrupting in the middle of contention: the holder is never disturbed, nobody deadlocks
        interruptible_mutex m;
        long inside = 0, max_inside = 0;
        std::atomic<bool> stop{false};
        std::vector<std::thread> ts;
        for (int t = 0; t < 4; ++t) ts.emplace_back([&] {
            while (!stop.load()) {
                if (!m.lock_interruptible()) { std::this_thread::yield(); continue; }
                ++inside; if (inside > max_inside) max_inside = inside;
                std::this_thread::yield();
                --inside;
                m.unlock();
            }
        });
        for (int i = 0; i < 200; ++i) { m.interrupt(); std::this_thread::sleep_for(100us); m.reset_interrupt(); std::this_thread::sleep_for(100us); }
        stop = true;
        for (auto& t : ts) t.join();
        CHECK(max_inside <= 1);
    }
    std::printf(failures ? "interruptible_mutex_test: %d FAILED\n" : "interruptible_mutex_test: all passed\n", failures);
    return failures ? 1 : 0;
}
