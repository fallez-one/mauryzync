// Wakeup benchmark: how fast does a submit reach a *sleeping* worker, and what
// does a submit cost the producer?
//
//  * latency : one task at a time, pool otherwise idle (all workers parked) --
//              submit-to-start latency, median / p99 / max.
//  * burst   : M producers each submit N trivial tasks while workers drain them;
//              reports tasks/s (stresses the submit-side wake path).
//
// Usage: wake_bench [workers=4] [latency_samples=3000] [producers=2] [tasks_per_producer=200000]
// Build the same source against two versions of the library to A/B them.
#include <FCS/Worker/workers.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using clock_type = std::chrono::steady_clock;

int main(int argc, char** argv) {
    const std::size_t workers = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 4;
    const std::size_t samples = argc > 2 ? std::strtoul(argv[2], nullptr, 10) : 3000;
    const std::size_t producers = argc > 3 ? std::strtoul(argv[3], nullptr, 10) : 2;
    const std::size_t per_producer = argc > 4 ? std::strtoul(argv[4], nullptr, 10) : 200000;

    FCS::Worker::pool_service<> pool;
    pool.concurrency(workers).start();

    // ---- latency
    std::vector<double> lat; lat.reserve(samples);
    for (std::size_t i = 0; i < samples; ++i) {
        std::this_thread::sleep_for(std::chrono::microseconds(300)); // let everyone park
        std::atomic<std::int64_t> started{0};
        const auto t0 = clock_type::now();
        while (!pool.enqueue([&started] { started.store(clock_type::now().time_since_epoch().count(), std::memory_order_release); })) std::this_thread::yield();
        while (started.load(std::memory_order_acquire) == 0) std::this_thread::yield();
        lat.push_back(std::chrono::duration<double, std::micro>(clock_type::time_point{clock_type::duration{started.load()}} - t0).count());
    }
    std::sort(lat.begin(), lat.end());
    std::printf("wake_bench workers=%zu\n", workers);
    std::printf("  submit->start latency (us): p50=%.1f p90=%.1f p99=%.1f max=%.1f\n",
                lat[lat.size() / 2], lat[lat.size() * 9 / 10], lat[lat.size() * 99 / 100], lat.back());

    // ---- burst throughput
    std::atomic<std::uint64_t> done{0};
    const auto t0 = clock_type::now();
    std::vector<std::thread> ps;
    for (std::size_t p = 0; p < producers; ++p) ps.emplace_back([&] {
        for (std::size_t i = 0; i < per_producer; ++i)
            while (!pool.enqueue([&done] { done.fetch_add(1, std::memory_order_relaxed); })) std::this_thread::yield();
    });
    for (auto& t : ps) t.join();
    const auto total = producers * per_producer;
    while (done.load(std::memory_order_acquire) < total) std::this_thread::yield();
    const auto secs = std::chrono::duration<double>(clock_type::now() - t0).count();
    std::printf("  burst: %zu tasks from %zu producers in %.3fs = %.0f tasks/s\n", total, producers, secs, static_cast<double>(total) / secs);
    pool.stop();
    return 0;
}
