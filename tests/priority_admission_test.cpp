// Normal-priority work must not be locked out at admission by a high-priority flood.
//
//  Part 1 (deterministic): the admission gate alone. High is refused `reserve` slots short
//  of full; normal still gets in right up to full; reserve=0 reproduces the old behaviour.
//  Part 2 (end to end): 16 flooders submit high priority flat out while a probe submits one
//  normal task every 2 ms. The probe's admission rate is asserted (it used to be ~6%).
#include <FCS/Worker/workers.hpp>
#include <FCS/Worker/detail/admission_gate.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace {
namespace W = FCS::Worker;
namespace D = FCS::Worker::detail;
using namespace std::chrono_literals;
int failures = 0;
#define CHECK(c) do { if (!(c)) { ++failures; std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

D::queued_task task_of(D::task_priority priority) {
    D::queued_task t{W::workload::Fast, W::source_kind::synthetic, D::task{[] {}}};
    t.priority = priority;
    return t;
}

void part1_gate() {
    std::printf("part 1: admission gate\n");
    using gate_t = D::admission_gate<256, 256>;
    {
        gate_t gate; D::scheduler_metrics metrics;
        gate.configure(/*full=*/128, /*available=*/16, /*candidates=*/32);
        gate.reserve_for_normal(16);
        CHECK(gate.high_priority_limit() == 112);
        int high_in = 0;
        while (gate.submit(task_of(D::task_priority::high), metrics) && high_in < 1000) ++high_in;
        CHECK(high_in == 112);                                   // high stops 16 short of full
        int normal_in = 0;
        while (gate.submit(task_of(D::task_priority::normal), metrics) && normal_in < 1000) ++normal_in;
        CHECK(normal_in == 16);                                  // the reserve is exactly what normal gets
        CHECK(gate.admission_state() == W::queue_state::full);
    }
    {
        gate_t gate; D::scheduler_metrics metrics;               // default (auto) reserve = max(1, full/16)
        gate.configure(128, 16, 32);
        CHECK(gate.high_priority_limit() == 120);
    }
    {
        gate_t gate; D::scheduler_metrics metrics;               // reserve 0 = old behaviour
        gate.configure(128, 16, 32);
        gate.reserve_for_normal(0);
        int high_in = 0;
        while (gate.submit(task_of(D::task_priority::high), metrics) && high_in < 1000) ++high_in;
        CHECK(high_in == 128);
    }
    {
        gate_t gate; D::scheduler_metrics metrics;               // an absurd reserve must not lock high out entirely
        gate.configure(128, 16, 32);
        gate.reserve_for_normal(100000);
        CHECK(gate.high_priority_limit() >= 1);
        CHECK(gate.submit(task_of(D::task_priority::high), metrics));
    }
}

double probe_admission_rate(std::size_t reserve_or_auto, bool use_auto) {
    W::pool_service<> pool;
    pool.concurrency(4);
    if (!use_auto) pool.normal_admission_reserve(reserve_or_auto);
    pool.start();
    std::atomic_bool flood{true};
    std::vector<std::thread> flooders;
    for (int f = 0; f < 16; ++f) flooders.emplace_back([&] {
        while (flood.load(std::memory_order_relaxed))
            (void)pool.enqueue_priority(D::task_priority::high, W::source_kind::synthetic, [] { volatile int x = 0; for (int i = 0; i < 200; ++i) x = x + i; });
    });
    std::this_thread::sleep_for(100ms); // let the flood reach steady state
    int attempted = 0, admitted = 0;
    const auto end = std::chrono::steady_clock::now() + 1500ms;
    while (std::chrono::steady_clock::now() < end) {
        ++attempted;
        if (pool.enqueue([] {})) ++admitted;
        std::this_thread::sleep_for(2ms);
    }
    flood = false;
    for (auto& t : flooders) t.join();
    pool.stop();
    return attempted ? static_cast<double>(admitted) / attempted : 0.0;
}

void part2_pool() {
    std::printf("part 2: end to end under a 16-thread high-priority flood\n");
    const double with_reserve = probe_admission_rate(0, true);
    const double without = probe_admission_rate(0, false);
    std::printf("  normal probe admitted: %.1f%% with the reserve, %.1f%% without (control)\n", with_reserve * 100, without * 100);
    CHECK(with_reserve >= 0.90);
}

} // namespace

int main() {
    part1_gate();
    part2_pool();
    std::printf(failures ? "priority_admission_test: %d FAILED\n" : "priority_admission_test: all passed\n", failures);
    return failures ? 1 : 0;
}
