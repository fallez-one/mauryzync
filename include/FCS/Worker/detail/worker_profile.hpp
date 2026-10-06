#pragma once

#include <array>
#include <atomic>
#include <cstdint>

namespace FCS::Worker::detail {

    // Plain-value snapshot of a worker's steal telemetry, safe to copy/read from
    // any thread once taken.
    struct worker_steal_snapshot {
        // Someone else tried to steal from this worker.
        std::uint64_t incoming_attempts{};
        // Items actually taken from this worker by peers (batch + single).
        std::uint64_t incoming_stolen{};
        // How many of those transfers were batch claims vs. single-item steals.
        std::uint64_t incoming_batches{};
        std::uint64_t incoming_singles{};

        // This worker tried to steal from a peer.
        std::uint64_t outgoing_attempts{};
        // Items this worker actually took from peers (batch + single).
        std::uint64_t outgoing_stolen{};
        // How many of those transfers were batch claims vs. single-item steals.
        std::uint64_t outgoing_batches{};
        std::uint64_t outgoing_singles{};

        // How this worker obtained its idle-peer estimate for sizing steals
        // (see idle_estimate.hpp): reused a cached value, rescanned all workers,
        // or reused the previous value because another thief was mid-refresh.
        std::uint64_t idle_cache_hits{};
        std::uint64_t idle_scans{};
        std::uint64_t idle_stale_reuses{};

        // Stall watchdog (see worker_registry::scan_stalls): how many times the
        // watchdog flagged *this* worker as stuck in one task, and how many items
        // this worker, as a thief, took from stalled victims below the normal
        // steal threshold.
        std::uint64_t times_stalled{};
        std::uint64_t hostage_stolen{};

        // Cooperative coroutines (see detail/coop.hpp), counted on the worker the coroutine
        // gave up: how many times a pool-driven coroutine yielded because the watchdog flagged
        // this worker, and how many of those could not stay in this worker's own deque (it was
        // full, or the coroutine belongs to another pool) and went through the global queue.
        std::uint64_t coop_yields{};
        std::uint64_t coop_global_requeues{};
    };

    // Per-worker steal counters. One instance lives inside each worker's local
    // slot; a thief updates the *victim's* incoming_* counters and its *own*
    // outgoing_* counters for the same event, so the two sides of the ledger stay
    // consistent (outgoing_stolen summed across all workers == incoming_stolen
    // summed across all workers, modulo the read not being a single atomic
    // snapshot across workers — same best-effort spirit as the rest of the
    // scheduler's counters).
    class worker_steal_profile {
    public:
        // Peers a worker can probe; probes_ is indexed by victim id (pool max_workers == 64).
        static constexpr std::size_t max_peers = 64;

        // Thief side of a probe: counted in the THIEF's own table, one slot per victim, by
        // the thief alone (plain load + store, no read-modify-write). The victim's
        // "incoming attempts" is the sum of every thief's slot for it, taken only when a
        // snapshot is read. This used to be a fetch_add on a counter inside the *victim*,
        // so every probe by every idle worker was a contended atomic RMW on a line shared
        // with all the other thieves -- the idle path's dominant cache-coherence cost.
        void record_probe_of(std::size_t victim) noexcept {
            auto& slot = probes_[victim];
            slot.store(slot.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
        }
        [[nodiscard]] std::uint64_t probes_of(std::size_t victim) const noexcept { return probes_[victim].load(std::memory_order_relaxed); }
        void record_incoming_batch(std::uint64_t count) noexcept {
            incoming_stolen_.fetch_add(count, std::memory_order_relaxed);
            incoming_batches_.fetch_add(1, std::memory_order_relaxed);
        }
        void record_incoming_single() noexcept {
            incoming_stolen_.fetch_add(1, std::memory_order_relaxed);
            incoming_singles_.fetch_add(1, std::memory_order_relaxed);
        }

        void record_outgoing_attempt() noexcept { outgoing_attempts_.fetch_add(1, std::memory_order_relaxed); }
        void record_outgoing_batch(std::uint64_t count) noexcept {
            outgoing_stolen_.fetch_add(count, std::memory_order_relaxed);
            outgoing_batches_.fetch_add(1, std::memory_order_relaxed);
        }
        void record_outgoing_single() noexcept {
            outgoing_stolen_.fetch_add(1, std::memory_order_relaxed);
            outgoing_singles_.fetch_add(1, std::memory_order_relaxed);
        }

        // Written by the owning worker only (it is always the thief here), so
        // plain per-worker counters: no cache line is shared with other thieves.
        void record_idle_cache_hit() noexcept { idle_cache_hits_.fetch_add(1, std::memory_order_relaxed); }
        void record_idle_scan() noexcept { idle_scans_.fetch_add(1, std::memory_order_relaxed); }
        void record_idle_stale_reuse() noexcept { idle_stale_reuses_.fetch_add(1, std::memory_order_relaxed); }

        // Written by the watchdog thread (victim side) / by the thief (thief side).
        void record_coop_yield() noexcept { coop_yields_.fetch_add(1, std::memory_order_relaxed); }
        void record_coop_global_requeue() noexcept { coop_global_requeues_.fetch_add(1, std::memory_order_relaxed); }
        void record_stalled() noexcept { times_stalled_.fetch_add(1, std::memory_order_relaxed); }
        void record_hostage_stolen(std::uint64_t count) noexcept { hostage_stolen_.fetch_add(count, std::memory_order_relaxed); }

        [[nodiscard]] worker_steal_snapshot snapshot() const noexcept {
            return worker_steal_snapshot{
                0, // incoming_attempts: aggregated across thieves by worker_registry::profile()
                incoming_stolen_.load(std::memory_order_relaxed),
                incoming_batches_.load(std::memory_order_relaxed),
                incoming_singles_.load(std::memory_order_relaxed),
                outgoing_attempts_.load(std::memory_order_relaxed),
                outgoing_stolen_.load(std::memory_order_relaxed),
                outgoing_batches_.load(std::memory_order_relaxed),
                outgoing_singles_.load(std::memory_order_relaxed),
                idle_cache_hits_.load(std::memory_order_relaxed),
                idle_scans_.load(std::memory_order_relaxed),
                idle_stale_reuses_.load(std::memory_order_relaxed),
                times_stalled_.load(std::memory_order_relaxed),
                hostage_stolen_.load(std::memory_order_relaxed),
                coop_yields_.load(std::memory_order_relaxed),
                coop_global_requeues_.load(std::memory_order_relaxed),
            };
        }

    private:
        alignas(64) std::atomic_uint64_t incoming_stolen_{}, incoming_batches_{}, incoming_singles_{};
        alignas(64) std::array<std::atomic_uint64_t, max_peers> probes_{};
        alignas(64) std::atomic_uint64_t outgoing_attempts_{}, outgoing_stolen_{}, outgoing_batches_{}, outgoing_singles_{};
        alignas(64) std::atomic_uint64_t idle_cache_hits_{}, idle_scans_{}, idle_stale_reuses_{};
        alignas(64) std::atomic_uint64_t times_stalled_{}, hostage_stolen_{};
        alignas(64) std::atomic_uint64_t coop_yields_{}, coop_global_requeues_{};
    };

}
