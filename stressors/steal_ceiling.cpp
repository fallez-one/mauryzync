// Work-redistribution benchmark for steal() -- isolates the idle-estimate cost.
//
// One producer repeatedly submits a burst of tiny tasks; they funnel into a few
// workers' local queues and the rest of the pool must redistribute them by
// stealing. With many mostly-idle workers every one of them is a thief at once,
// which is exactly where the old per-thief O(threads) scan was paid N times
// over for the same answer.
//
// Usage: steal_bench [workers=16] [bursts=2000] [burst_size=512] [ttl=default|0|N]
// Prints scans vs cache hits (deterministic effect of the cache) and wall time.
// NOTE: wall-time differences only show up with >= `workers` hardware threads;
// on fewer cores threads time-slice, so compare the scan counts instead.

#include <FCS/Worker/workers.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

int main(int argc, char** argv) {
    const std::size_t workers = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 16;
    const std::size_t bursts = argc > 2 ? std::strtoul(argv[2], nullptr, 10) : 2000;
    const std::size_t burst_size = argc > 3 ? std::strtoul(argv[3], nullptr, 10) : 512;
    const bool ttl_given = argc > 4 && std::strcmp(argv[4], "default") != 0;
    const std::size_t ttl = ttl_given ? std::strtoul(argv[4], nullptr, 10) : 0;

    FCS::Worker::pool_service<> pool;
    pool.concurrency(workers);
    if (ttl_given) pool.idle_cache_ttl(ttl);
    pool.start();

    std::atomic<std::uint64_t> done{0};
    std::uint64_t expected = 0;
    const auto t0 = std::chrono::steady_clock::now();
    for (std::size_t b = 0; b < bursts; ++b) {
        for (std::size_t i = 0; i < burst_size; ++i) {
            while (!pool.enqueue([&done] {
                volatile unsigned x = 0;
                for (int k = 0; k < 200; ++k) x = x + static_cast<unsigned>(k);
                done.fetch_add(1, std::memory_order_relaxed);
            })) std::this_thread::yield();
            ++expected;
        }
        // Let the pool go (mostly) idle between bursts so thieves pile in together.
        while (done.load(std::memory_order_acquire) < expected) std::this_thread::yield();
    }
    const auto secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    std::uint64_t attempts = 0, hits = 0, scans = 0, stale = 0, stolen = 0;
    for (std::size_t id = 0; id < pool.worker_count(); ++id) {
        const auto p = pool.worker_profile(id);
        attempts += p.outgoing_attempts; hits += p.idle_cache_hits; scans += p.idle_scans; stale += p.idle_stale_reuses; stolen += p.outgoing_stolen;
    }
    pool.stop();
    const auto lookups = hits + scans + stale;
    std::printf("steal_bench workers=%zu bursts=%zu burst=%zu ttl=%s\n", workers, bursts, burst_size, ttl_given ? argv[4] : "default");
    std::printf("  time=%.3fs  tasks/s=%.0f  stolen=%llu\n", secs, static_cast<double>(expected) / secs, static_cast<unsigned long long>(stolen));
    std::printf("  idle-estimate lookups=%llu: scans=%llu (%.1f%%) cache_hits=%llu stale_reuses=%llu\n",
                static_cast<unsigned long long>(lookups), static_cast<unsigned long long>(scans),
                lookups ? 100.0 * static_cast<double>(scans) / static_cast<double>(lookups) : 0.0,
                static_cast<unsigned long long>(hits), static_cast<unsigned long long>(stale));
    std::printf("  full-array loads avoided: ~%llu\n", static_cast<unsigned long long>((lookups - scans) * workers));
    (void)attempts;
    return 0;
}
