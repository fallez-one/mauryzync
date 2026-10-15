// Worker CPU affinity knob: off by default; pinned only when asked.
#include <FCS/Worker/workers.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

#if defined(__linux__)
#  include <sched.h>
#endif

namespace {
using namespace std::chrono_literals;
namespace W = FCS::Worker;
int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

// What the calling thread is allowed to run on (Linux only; -1 elsewhere).
int allowed_cpus() {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (::sched_getaffinity(0, sizeof set, &set) != 0) return -1;   // 0 = the calling thread
    return CPU_COUNT(&set);
#else
    return -1;
#endif
}

// Runs `n` tasks on a started pool and returns, per task, how many CPUs its worker may use.
template<typename Setup>
std::vector<int> observe(std::size_t workers, Setup&& setup, std::size_t* failed = nullptr) {
    W::pool_service<> pool;
    pool.concurrency(workers).execution_policy(W::execution::dedicated_poller);
    setup(pool);
    pool.start();
    std::vector<int> seen(workers * 8, 0);
    std::atomic<std::size_t> done{0};
    for (std::size_t i = 0; i < seen.size(); ++i) CHECK(pool.enqueue([&, i] { seen[i] = allowed_cpus(); std::this_thread::sleep_for(2ms); done.fetch_add(1); }));
    while (done.load() < seen.size()) std::this_thread::sleep_for(1ms);
    if (failed) *failed = pool.worker_affinity_failures();
    pool.stop();
    return seen;
}
}

int main() {
    const int mine = allowed_cpus();
    std::printf("main thread may use %d CPU(s)\n", mine);

    {   // default: nothing is touched
        std::size_t failed = 99;
        const auto seen = observe(3, [](auto&) {}, &failed);
        for (const int n : seen) CHECK(n == mine);
        CHECK(failed == 0);
        std::printf("default: workers keep the process mask -- ok\n");
    }
    {   // explicit list
        std::size_t failed = 99;
        const auto seen = observe(3, [](auto& p) { p.worker_affinity({0}); }, &failed);
        if (mine >= 0) for (const int n : seen) CHECK(n == 1);
        CHECK(failed == 0);
        std::printf("worker_affinity({0}): every worker pinned to one CPU -- ok\n");
    }
    {   // spread
        std::size_t failed = 99;
        const auto seen = observe(3, [](auto& p) { p.worker_affinity_spread(); }, &failed);
        if (mine >= 0) for (const int n : seen) CHECK(n == 1);
        CHECK(failed == 0);
        std::printf("worker_affinity_spread(): every worker pinned to one CPU -- ok\n");
    }
    {   // a CPU that does not exist: counted, the worker stays unpinned, the pool works
        std::size_t failed = 0;
        const auto seen = observe(2, [](auto& p) { p.worker_affinity({100000}); }, &failed);
        for (const int n : seen) CHECK(n == mine);
        CHECK(failed == 2);
        std::printf("worker_affinity({100000}): refused, counted (%zu), pool unaffected -- ok\n", failed);
    }
    {   // set and cleared again
        std::size_t failed = 99;
        const auto seen = observe(2, [](auto& p) { p.worker_affinity({0}); p.worker_affinity({}); }, &failed);
        for (const int n : seen) CHECK(n == mine);
        CHECK(failed == 0);
        std::printf("worker_affinity({}) clears it -- ok\n");
    }
    {   // too late: ignored once running
        W::pool_service<> pool;
        pool.concurrency(2).start();
        pool.worker_affinity({0});
        std::atomic<int> n{-2};
        CHECK(pool.enqueue([&] { n = allowed_cpus(); }));
        while (n.load() == -2) std::this_thread::sleep_for(1ms);
        CHECK(n.load() == mine);
        pool.stop();
        std::printf("worker_affinity() after start() is ignored -- ok\n");
    }
    std::printf(failures ? "worker_affinity: %d FAILED\n" : "worker_affinity: all passed\n", failures);
    return failures ? 1 : 0;
}
