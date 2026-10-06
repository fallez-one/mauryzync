// idle_estimate: exact TTL accounting, single refresher per window, invalidate(),
// and ttl==0 disabling. Run under TSan.
#include <FCS/Worker/detail/idle_estimate.hpp>

#include <atomic>
#include <cstdio>
#include <thread>
#include <vector>

using FCS::Worker::detail::idle_estimate;
static int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("FAIL %s:%d %s\n", __FILE__, __LINE__, #c); } } while (0)

int main() {
    // 1. single thread: 1 scan then exactly ttl cached reads, then a rescan
    {
        idle_estimate e; e.configure(5);
        int scans = 0; std::size_t value = 3;
        auto scan = [&] { ++scans; return value; };
        auto r = e.get(scan);                       CHECK(r.from == idle_estimate::source::scanned && r.idle == 3);
        value = 9;                                  // changes are invisible until the TTL runs out
        for (int i = 0; i < 5; ++i) { r = e.get(scan); CHECK(r.from == idle_estimate::source::cached && r.idle == 3); }
        r = e.get(scan);                            CHECK(r.from == idle_estimate::source::scanned && r.idle == 9);
        CHECK(scans == 2);
    }
    // 2. invalidate forces a rescan; invalidating a stale cache is a no-op
    {
        idle_estimate e; e.configure(100);
        int scans = 0; auto scan = [&] { ++scans; return std::size_t{2}; };
        (void)e.get(scan); (void)e.get(scan);       CHECK(scans == 1);
        e.invalidate(); e.invalidate();
        (void)e.get(scan);                          CHECK(scans == 2);
    }
    // 3. ttl 0 = always scan
    {
        idle_estimate e; e.configure(0);
        int scans = 0; auto scan = [&] { ++scans; return std::size_t{1}; };
        for (int i = 0; i < 10; ++i) (void)e.get(scan);
        CHECK(scans == 10);
    }
    // 4. concurrency: total scans bounded by (#gets / ttl) + threads' worth of races, never one per get,
    //    and a cached value is never invented (all readings are values the scan produced).
    {
        constexpr int threads = 8, per = 200000, ttl = 16;
        idle_estimate e; e.configure(ttl);
        std::atomic<long> scans{0}, bad{0}, cached{0};
        std::atomic<std::size_t> truth{4};
        auto scan = [&] { scans.fetch_add(1, std::memory_order_relaxed); return truth.load(std::memory_order_relaxed); };
        std::vector<std::thread> ts;
        for (int t = 0; t < threads; ++t) ts.emplace_back([&, t] {
            for (int i = 0; i < per; ++i) {
                if (t == 0 && i % 1000 == 0) { truth.store(static_cast<std::size_t>(1 + (i / 1000) % 7)); }
                if (t == 1 && i % 5000 == 0) e.invalidate();
                const auto r = e.get(scan);
                if (r.idle < 1 || r.idle > 7) bad.fetch_add(1);
                if (r.from == idle_estimate::source::cached) cached.fetch_add(1);
            }
        });
        for (auto& t : ts) t.join();
        const long gets = static_cast<long>(threads) * per;
        std::printf("gets=%ld scans=%ld (%.2f%% of gets, bound ~%.2f%%) cached=%ld\n", gets, scans.load(),
                    100.0 * static_cast<double>(scans.load()) / static_cast<double>(gets), 100.0 / ttl, cached.load());
        CHECK(bad.load() == 0);
        CHECK(scans.load() < gets / 4);            // far below one-scan-per-get
    }
    std::printf(failures ? "idle_estimate_test: %d FAILED\n" : "idle_estimate_test: all passed\n", failures);
    return failures ? 1 : 0;
}
