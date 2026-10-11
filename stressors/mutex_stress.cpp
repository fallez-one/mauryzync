// Mutex stressor: thread safety is the entire point of a mutex, so this hammers the library's
// two interruptible mutexes -- and std::mutex as a throughput baseline -- far past what a unit test does.
//
// Phases, per mutex type:
//   exclusion   N threads (default 2x hardware threads: oversubscribed on purpose, so lock holders get
//               descheduled mid-section) take the lock flat out. Invariants: never two threads inside;
//               the protected plain counter equals the sum of every thread's own entry count.
//   storm       the same, with an interrupter toggling interrupt()/reset_interrupt() at random, half the
//               threads using lock_interruptible() (some cancelled), half plain lock(). Same invariants,
//               plus: afterwards the mutex is idle (not locking(), try_lock() works) -- a leaked or doubled
//               turn would show here -- and every cancelled call is accounted for.
//   fairness    (ticket mutex) per-thread entry counts under full contention: nobody may starve; the spread is
//               reported (a FIFO lock should be nearly flat; std::mutex is shown for contrast).
//   cancel-all  a holder keeps the lock, K waiters queue in lock_interruptible(), interrupt() must release ALL
//               of them promptly while the holder is still inside -- repeated for the whole phase.
//
// Then the worst-case tier ("hell"), because a mutex that survives friendly contention proves little:
//   burst       all threads released at the same instant, thousands of rounds (worst case for ticket assignment)
//   nasty       every thread picks a random behaviour each iteration -- lock / lock_interruptible / try_lock spin /
//               holds the lock across a sleep or a yield burst -- while FOUR interrupters hammer interrupt() and
//               reset_interrupt() concurrently, one of them with no pause at all
//   pinned      the nasty mix with every thread pinned to ONE core: the holder is preempted mid-section constantly
//               and, for a FIFO lock, the next ticket holder must be scheduled before anyone can proceed (convoy)
//   churn       short-lived threads created and destroyed continuously, many exiting while queued or cancelled
//   capacity    ordered_mutex<uint8_t> with 250 threads: one more would break its 255-outstanding-ticket contract
//
// A hang detector aborts with a diagnostic (exit code 3) if no thread makes progress for `hang_seconds`: a
// lost wake-up is a hang. Exit code 1 = an invariant was violated, 0 = clean.
//
// Usage: mutex_stress [threads=2*hardware] [seconds_per_phase=2] [hang_seconds=30]
#include <FCS/Worker/sync/interruptible_mutex.hpp>
#include <FCS/Worker/sync/ordered_mutex.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <barrier>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#elif defined(__linux__)
#  include <pthread.h>
#  include <sched.h>
#endif

namespace {

using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

std::atomic<std::uint64_t> g_progress{0};            // bumped by every worker on every entry: the hang detector watches it
std::atomic<const char*> g_where{"startup"};
std::atomic<std::uint64_t> g_violations{0};

void violation(const char* what) {
    if (g_violations.fetch_add(1) < 10) std::fprintf(stderr, "  VIOLATION [%s]: %s\n", g_where.load(), what);
}

template<typename M> constexpr bool has_interrupt = requires(M& m) { m.interrupt(); m.reset_interrupt(); { m.lock_interruptible() } -> std::convertible_to<bool>; };

template<typename F>
void run_threads(std::size_t n, F&& body) {
    std::vector<std::thread> ts;
    ts.reserve(n);
    for (std::size_t i = 0; i < n; ++i) ts.emplace_back([&body, i] { body(i); });
    for (auto& t : ts) t.join();
}

// ---------------------------------------------------------------- exclusion
template<typename M>
void phase_exclusion(const char* name, std::size_t threads, std::chrono::milliseconds duration) {
    g_where = "exclusion";
    M m;
    long counter = 0;                                // plain on purpose: only the mutex protects it
    int inside = 0;
    std::atomic<bool> stop{false};
    std::vector<std::uint64_t> entries(threads, 0);
    const auto t0 = clk::now();
    std::thread timer([&] { std::this_thread::sleep_for(duration); stop = true; });
    run_threads(threads, [&](std::size_t id) {
        std::uint64_t mine = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            m.lock();
            if (++inside != 1) violation("two threads inside the critical section");
            ++counter;
            if ((counter & 0x3FF) == 0) std::this_thread::yield();   // let the holder get descheduled now and then
            if (--inside != 0) violation("critical section entered while a thread was leaving");
            m.unlock();
            ++mine;
            g_progress.fetch_add(1, std::memory_order_relaxed);
        }
        entries[id] = mine;
    });
    timer.join();
    const double secs = std::chrono::duration<double>(clk::now() - t0).count();
    std::uint64_t total = 0;
    for (auto e : entries) total += e;
    if (static_cast<std::uint64_t>(counter) != total) violation("protected counter != sum of per-thread entries (lost update)");
    std::printf("  %-26s exclusion : %10.0f lock/s  (%llu entries)\n", name, static_cast<double>(total) / secs, static_cast<unsigned long long>(total));
}

// ---------------------------------------------------------------- fairness
template<typename M>
void phase_fairness(const char* name, std::size_t threads, std::chrono::milliseconds duration) {
    g_where = "fairness";
    M m;
    std::atomic<bool> stop{false};
    std::vector<std::uint64_t> entries(threads, 0);
    std::thread timer([&] { std::this_thread::sleep_for(duration); stop = true; });
    run_threads(threads, [&](std::size_t id) {
        std::uint64_t mine = 0;
        while (!stop.load(std::memory_order_relaxed)) { m.lock(); ++mine; m.unlock(); g_progress.fetch_add(1, std::memory_order_relaxed); }
        entries[id] = mine;
    });
    timer.join();
    const auto [mn, mx] = std::minmax_element(entries.begin(), entries.end());
    if (*mn == 0) violation("a thread never got the lock (starvation)");
    std::printf("  %-26s fairness  : per-thread entries min=%llu max=%llu (max/min %.1fx)\n", name,
                static_cast<unsigned long long>(*mn), static_cast<unsigned long long>(*mx), *mn ? static_cast<double>(*mx) / static_cast<double>(*mn) : 0.0);
}

// ---------------------------------------------------------------- storm
template<typename M>
void phase_storm(const char* name, std::size_t threads, std::chrono::milliseconds duration) {
    g_where = "storm";
    M m;
    long counter = 0;
    int inside = 0;
    std::atomic<bool> stop{false};
    std::vector<std::uint64_t> entered(threads, 0), cancelled(threads, 0);
    std::thread timer([&] { std::this_thread::sleep_for(duration); stop = true; });
    std::thread interrupter([&] {
        std::mt19937 rng(12345);
        while (!stop.load(std::memory_order_relaxed)) {
            m.interrupt();
            std::this_thread::sleep_for(std::chrono::microseconds(20 + rng() % 400));
            m.reset_interrupt();
            std::this_thread::sleep_for(std::chrono::microseconds(20 + rng() % 400));
        }
    });
    run_threads(threads, [&](std::size_t id) {
        std::uint64_t in = 0, out = 0;
        const bool interruptible = (id % 2 == 0);
        while (!stop.load(std::memory_order_relaxed)) {
            bool got;
            if (interruptible) got = m.lock_interruptible(); else { m.lock(); got = true; }
            if (!got) { ++out; std::this_thread::yield(); continue; }
            if (++inside != 1) violation("two threads inside the critical section (during interrupts)");
            ++counter;
            if ((counter & 0xFF) == 0) std::this_thread::yield();
            if (--inside != 0) violation("critical section entered while a thread was leaving (during interrupts)");
            m.unlock();
            ++in;
            g_progress.fetch_add(1, std::memory_order_relaxed);
        }
        entered[id] = in; cancelled[id] = out;
    });
    interrupter.join();
    timer.join();
    std::uint64_t total = 0, total_cancelled = 0;
    for (std::size_t i = 0; i < threads; ++i) { total += entered[i]; total_cancelled += cancelled[i]; }
    if (static_cast<std::uint64_t>(counter) != total) violation("storm: protected counter != sum of entries");
    m.reset_interrupt();
    if (m.locking()) violation("storm: mutex still reports locking() with every thread gone (leaked turn)");
    if (!m.try_lock()) violation("storm: try_lock() failed on an idle mutex (leaked turn)");
    else { m.unlock(); if (m.locking()) violation("storm: unlock() after try_lock() left it locking()"); }
    std::printf("  %-26s storm     : %10llu entries, %llu cancelled lock_interruptible()\n", name,
                static_cast<unsigned long long>(total), static_cast<unsigned long long>(total_cancelled));
}

// ---------------------------------------------------------------- cancel-all
template<typename M>
void phase_cancel_all(const char* name, std::chrono::milliseconds duration) {
    g_where = "cancel-all";
    M m;
    constexpr int waiters = 6;
    std::uint64_t rounds = 0, slow_rounds = 0;
    const auto end = clk::now() + duration;
    while (clk::now() < end) {
        m.lock();                                                  // the holder, "stuck"
        std::atomic<int> released{0}, started{0};
        std::vector<std::thread> ws;
        for (int i = 0; i < waiters; ++i) ws.emplace_back([&] { started.fetch_add(1); if (!m.lock_interruptible()) released.fetch_add(1); });
        while (started.load() < waiters) std::this_thread::yield();
        std::this_thread::sleep_for(2ms);                          // let them reach the queue
        m.interrupt();
        const auto t = clk::now();
        while (released.load() < waiters && clk::now() - t < 1s) std::this_thread::sleep_for(100us);
        if (released.load() != waiters) { ++slow_rounds; violation("interrupt() did not release every queued lock_interruptible() while the holder was inside"); }
        if (!m.locking()) violation("holder lost the lock to an interrupt");
        m.unlock();
        for (auto& w : ws) w.join();
        if (m.locking()) violation("cancel-all: mutex still locking() after everyone left");
        m.reset_interrupt();
        ++rounds;
        g_progress.fetch_add(1, std::memory_order_relaxed);
    }
    std::printf("  %-26s cancel-all: %llu rounds, every queued waiter released promptly in %llu of them\n", name,
                static_cast<unsigned long long>(rounds), static_cast<unsigned long long>(rounds - slow_rounds));
}


void pin_to_first_cpu() {
#if defined(_WIN32)
    ::SetThreadAffinityMask(::GetCurrentThread(), 1);
#elif defined(__linux__)
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(0, &set);
    ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
#endif
}

// The invariant checks every nasty phase shares: critical-section exclusion plus the plain protected counter.
struct guarded {
    long counter = 0;
    int inside = 0;
    void enter() { if (++inside != 1) violation("two threads inside the critical section"); ++counter; }
    void leave() { if (--inside != 0) violation("critical section entered while a thread was leaving"); }
};

template<typename M>
bool idle_checks(M& m, const char* what) {
    bool ok = true;
    if constexpr (has_interrupt<M>) m.reset_interrupt();
    if constexpr (requires { m.locking(); }) { if (m.locking()) { violation(what); ok = false; } }
    if (!m.try_lock()) { violation(what); ok = false; } else m.unlock();
    return ok;
}

// ---------------------------------------------------------------- burst
template<typename M>
void phase_burst(const char* name, std::size_t threads, std::chrono::milliseconds duration) {
    g_where = "burst";
    M m; guarded g;
    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> entries{0};
    // The decision to stop must be made ONCE per round: if each thread read `stop` on its own, some would see
    // it set and leave while the rest wait at the next barrier forever (a hang in the harness, not the mutex).
    // The barrier's completion step runs exactly once, after the last arrival and before anyone is released.
    std::atomic<bool> stopping{false};
    std::barrier start_line(static_cast<std::ptrdiff_t>(threads), [&]() noexcept { stopping.store(stop.load()); });
    std::barrier end_line(static_cast<std::ptrdiff_t>(threads));
    std::uint64_t rounds = 0;
    std::thread timer([&] { std::this_thread::sleep_for(duration); stop = true; });
    run_threads(threads, [&](std::size_t id) {
        for (;;) {
            start_line.arrive_and_wait();                 // everybody released at the same instant
            if (stopping.load()) return;                  // same answer for every thread this round
            m.lock(); g.enter(); g.leave(); m.unlock();
            entries.fetch_add(1, std::memory_order_relaxed);
            g_progress.fetch_add(1, std::memory_order_relaxed);
            end_line.arrive_and_wait();
            if (id == 0) ++rounds;
        }
    });
    timer.join();
    if (static_cast<std::uint64_t>(g.counter) != entries.load()) violation("burst: lost update");
    idle_checks(m, "burst: mutex not idle afterwards");
    std::printf("  %-26s burst     : %llu simultaneous-arrival rounds\n", name, static_cast<unsigned long long>(rounds));
}

// ---------------------------------------------------------------- nasty / pinned
template<typename M>
void phase_nasty(const char* name, std::size_t threads, std::chrono::milliseconds duration, bool pinned) {
    g_where = pinned ? "pinned" : "nasty";
    M m; guarded g;
    std::atomic<bool> stop{false};
    std::vector<std::uint64_t> entered(threads, 0), cancelled(threads, 0), plain_entries(threads, 0);
    std::thread timer([&] { std::this_thread::sleep_for(duration); stop = true; });

    std::vector<std::thread> interrupters;
    if constexpr (has_interrupt<M>) {
        for (int k = 0; k < 4; ++k) interrupters.emplace_back([&, k] {
            if (pinned) pin_to_first_cpu();
            std::mt19937 rng(777u + static_cast<unsigned>(k));
            while (!stop.load(std::memory_order_relaxed)) {
                m.interrupt();
                if (k != 0) std::this_thread::sleep_for(std::chrono::microseconds(rng() % 300));   // k==0: no pause at all
                m.reset_interrupt();
                if (k != 0) std::this_thread::sleep_for(std::chrono::microseconds(rng() % 300));
                if (k == 0) std::this_thread::yield();
            }
        });
    }
    run_threads(threads, [&](std::size_t id) {
        if (pinned) pin_to_first_cpu();
        std::mt19937 rng(1000u + static_cast<unsigned>(id));
        std::uint64_t in = 0, out = 0, plain = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            const unsigned pick = rng() % 100;
            bool got = false;
            bool was_plain = false;
            if (pick < 8) { got = m.try_lock(); if (!got) { std::this_thread::yield(); continue; } }                  // barging attempt
            else if (pick < 45) { if constexpr (has_interrupt<M>) got = m.lock_interruptible(); else { m.lock(); got = true; } }
            else { m.lock(); got = true; was_plain = true; }
            if (!got) { ++out; std::this_thread::yield(); continue; }
            g.enter();
            const unsigned hold = rng() % 1000;
            if (hold < 3) std::this_thread::sleep_for(std::chrono::microseconds(200 + rng() % 1500));   // holder asleep INSIDE the section
            else if (hold < 40) { for (int y = 0; y < 8; ++y) std::this_thread::yield(); }               // holder preempted repeatedly
            g.leave();
            m.unlock();
            ++in; if (was_plain) ++plain;
            g_progress.fetch_add(1, std::memory_order_relaxed);
        }
        entered[id] = in; cancelled[id] = out; plain_entries[id] = plain;
    });
    for (auto& t : interrupters) t.join();
    timer.join();
    std::uint64_t total = 0, total_cancelled = 0;
    for (std::size_t i = 0; i < threads; ++i) { total += entered[i]; total_cancelled += cancelled[i]; }
    if (static_cast<std::uint64_t>(g.counter) != total) violation("nasty: protected counter != sum of entries");
    idle_checks(m, "nasty: mutex not idle afterwards (leaked or doubled turn)");
    // plain lock() masks interrupts, so under an interrupt storm it must still make progress
    std::uint64_t starved = 0;
    for (std::size_t i = 0; i < threads; ++i) if (plain_entries[i] == 0 && entered[i] > 0) ++starved;
    std::printf("  %-26s %-9s : %10llu entries, %llu cancelled%s\n", name, pinned ? "pinned" : "nasty", static_cast<unsigned long long>(total),
                static_cast<unsigned long long>(total_cancelled), starved ? "  (!) some threads got in only via try_lock/interruptible" : "");
}

// ---------------------------------------------------------------- churn
template<typename M>
void phase_churn(const char* name, std::chrono::milliseconds duration) {
    g_where = "churn";
    M m; guarded g;
    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> entries{0}, spawned{0};
    std::thread timer([&] { std::this_thread::sleep_for(duration); stop = true; });
    std::thread interrupter([&] {
        if constexpr (has_interrupt<M>) {
            while (!stop.load(std::memory_order_relaxed)) { m.interrupt(); std::this_thread::sleep_for(std::chrono::microseconds(150)); m.reset_interrupt(); std::this_thread::sleep_for(std::chrono::microseconds(150)); }
        }
    });
    while (!stop.load()) {
        std::vector<std::thread> wave;
        for (int i = 0; i < 24; ++i) wave.emplace_back([&, i] {
            for (int k = 0; k < 20; ++k) {
                bool got = true;
                if (i % 2 == 0) { if constexpr (has_interrupt<M>) got = m.lock_interruptible(); else m.lock(); } else m.lock();
                if (!got) continue;                          // cancelled: the thread then simply exits, possibly still "in" the queue's past
                g.enter(); g.leave(); m.unlock();
                entries.fetch_add(1, std::memory_order_relaxed);
                g_progress.fetch_add(1, std::memory_order_relaxed);
            }
        });
        spawned += wave.size();
        for (auto& t : wave) t.join();
    }
    interrupter.join();
    timer.join();
    if (static_cast<std::uint64_t>(g.counter) != entries.load()) violation("churn: lost update");
    idle_checks(m, "churn: mutex not idle afterwards");
    std::printf("  %-26s churn     : %llu threads spawned and joined, %llu entries\n", name, static_cast<unsigned long long>(spawned.load()), static_cast<unsigned long long>(entries.load()));
}

template<typename M>
void run_mutex(const char* name, std::size_t threads, std::chrono::milliseconds phase) {
    std::printf("[%s]\n", name);
    phase_exclusion<M>(name, threads, phase);
    phase_fairness<M>(name, threads, phase);
    if constexpr (has_interrupt<M>) {
        phase_storm<M>(name, threads, phase);
        phase_cancel_all<M>(name, phase);
    }
    std::printf("  -- worst case --\n");
    phase_burst<M>(name, threads, phase);
    phase_nasty<M>(name, threads, phase, false);
    phase_nasty<M>(name, threads, phase, true);
    phase_churn<M>(name, phase);
}

} // namespace

int main(int argc, char** argv) {
    const unsigned hw = std::max(1u, std::thread::hardware_concurrency());
    const std::size_t threads = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : std::min<std::size_t>(2u * hw, 64u); // < 255: uint8_t ticket contract
    const auto phase = std::chrono::milliseconds(static_cast<long long>((argc > 2 ? std::atof(argv[2]) : 2.0) * 1000));
    const double hang_seconds = argc > 3 ? std::atof(argv[3]) : 30.0;
    if (threads >= 255) { std::fprintf(stderr, "threads must be < 255 (ordered_mutex<uint8_t> holds at most 255 outstanding tickets)\n"); return 2; }

    std::printf("FCS-Workers mutex_stress: threads=%zu, %.1fs per phase, hang limit %.0fs\n", threads, std::chrono::duration<double>(phase).count(), hang_seconds);

    std::thread([hang_seconds] {                                  // hang detector: a lost wake-up is a hang
        std::uint64_t last = g_progress.load();
        auto since = clk::now();
        for (;;) {
            std::this_thread::sleep_for(500ms);
            const auto now = g_progress.load();
            if (now != last) { last = now; since = clk::now(); continue; }
            if (std::chrono::duration<double>(clk::now() - since).count() > hang_seconds) {
                std::fprintf(stderr, "\n*** HANG: no thread made progress for %.0fs during phase \"%s\" (lost wake-up or deadlock)\n", hang_seconds, g_where.load());
                std::fflush(stderr);
                std::_Exit(3);
            }
        }
    }).detach();

    run_mutex<std::mutex>("std::mutex (baseline)", threads, phase);
    run_mutex<FCS::synchronization::interruptible_mutex>("interruptible_mutex", threads, phase);
    run_mutex<FCS::synchronization::ordered_mutex<std::uint8_t>>("ordered_mutex<uint8_t>", threads, phase);
    run_mutex<FCS::synchronization::ordered_mutex<std::uint16_t>>("ordered_mutex<uint16_t>", threads, phase);

    // capacity_policy::block: MORE threads than the 255 tickets a uint8_t has -- correct only because they wait for a free ticket
    using checked_u8 = FCS::synchronization::ordered_mutex<std::uint8_t, FCS::synchronization::capacity_policy::block>;
    std::printf("[ordered_mutex<uint8_t, block> past capacity]\n");
    phase_exclusion<checked_u8>("ordered_mutex<u8,block> x400", 400, phase);
    phase_nasty<checked_u8>("ordered_mutex<u8,block> x400", 400, phase, false);

    // ordered_mutex<uint8_t> at the edge of its contract: 250 threads, so up to 250 outstanding tickets (limit 254)
    std::printf("[ordered_mutex<uint8_t> at capacity]\n");
    phase_exclusion<FCS::synchronization::ordered_mutex<std::uint8_t>>("ordered_mutex<uint8_t> x250", 250, phase);
    phase_nasty<FCS::synchronization::ordered_mutex<std::uint8_t>>("ordered_mutex<uint8_t> x250", 250, phase, false);

    if (g_violations.load() != 0) { std::printf("\nmutex_stress: %llu INVARIANT VIOLATIONS\n", static_cast<unsigned long long>(g_violations.load())); return 1; }
    std::printf("\nmutex_stress: all phases clean\n");
    return 0;
}
