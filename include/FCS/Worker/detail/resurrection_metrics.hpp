#pragma once

#include <atomic>
#include <cstdint>

namespace FCS::Worker::detail {

    // Telemetry of the total-stall poll / runtime shed (pool_service::resurrection_stats()).
    // Plain snapshot type: always available, all zero when FCS_EXPERIMENTAL_ALWAYS_ON is off.
    // Counters live in the pool, so a clone INHERITS its parent's totals (copy-on-write) and
    // `generation` says how many clones deep this process is: 0 = the original.
    struct resurrection_snapshot {
        std::uint64_t generation{};            // clones between the original process and this one
        std::uint64_t polls_opened{};          // total stalls that triggered a proof-of-life poll
        std::uint64_t polls_cleared{};         // ... that ended because enough workers showed life (false alarms)
        std::uint64_t polls_extended{};        // times the watchdog itself overslept (paused / starved) and the window was extended to match
        std::uint64_t sheds_started{};         // polls that failed: point of no return reached
        std::uint64_t sheds_aborted{};         // ... whose clone failed, so the shed was undone
        std::uint64_t resurrections{};         // ... whose clone succeeded (counted in the clone)
        std::uint64_t tasks_evacuated{};       // moved into the batch, all sheds
        std::uint64_t tasks_reinjected{};      // put back into queues (clone: after the hook; abort: after the failure)
        std::uint64_t tasks_refused{};         // had nowhere to go on re-injection: lost
        std::uint64_t tasks_unreachable{};     // gauges still counted them after the sweep (frozen producer, full row): probably lost
        std::uint64_t submits_refused{};       // enqueue() calls refused because a shed was running
        std::uint64_t last_poll_ns{};          // how long the last poll waited
        std::uint64_t last_evacuation_ns{};    // how long the last sweep took (flag up -> batch full)
        std::uint64_t last_shed_ns{};          // flag up -> clone answered (parent) / -> workers started (clone)
    };

    class resurrection_metrics {
    public:
        std::atomic<std::uint64_t> generation{}, polls_opened{}, polls_cleared{}, polls_extended{}, sheds_started{}, sheds_aborted{}, resurrections{},
            tasks_evacuated{}, tasks_reinjected{}, tasks_refused{}, tasks_unreachable{}, submits_refused{},
            last_poll_ns{}, last_evacuation_ns{}, last_shed_ns{};

        [[nodiscard]] resurrection_snapshot snapshot() const noexcept {
            const auto get = [](const std::atomic<std::uint64_t>& v) { return v.load(std::memory_order_relaxed); };
            return {get(generation), get(polls_opened), get(polls_cleared), get(polls_extended), get(sheds_started), get(sheds_aborted), get(resurrections),
                    get(tasks_evacuated), get(tasks_reinjected), get(tasks_refused), get(tasks_unreachable), get(submits_refused),
                    get(last_poll_ns), get(last_evacuation_ns), get(last_shed_ns)};
        }
    };

}
