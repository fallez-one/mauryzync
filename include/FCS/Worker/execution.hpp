#pragma once

#include <cstdint>

namespace FCS::Worker::execution {

    // Controls how a backend with a native multiplexer (epoll, io_uring,
    // IOCP) gets its readiness/completion polling done. This used to be an
    // unconditional implementation detail -- every such backend spun up one
    // blocking OS poller thread the instant start_backend() ran, with no way
    // to see it coming or opt out. It no longer does that by default; a
    // backend now asks its pool_service which policy is in effect (see
    // pool_service::execution_policy()) before deciding how to poll.
    enum class policy : std::uint8_t {
        // One dedicated OS thread blocks in the backend's native wait call
        // (epoll_wait/io_uring_enter/GetQueuedCompletionStatus) with an
        // effectively infinite timeout and drives on_ready/on_completion
        // straight off that thread. Lowest latency, but costs one full OS
        // thread per backend instance for its entire lifetime -- this is
        // the old always-on behavior, now something you ask for explicitly.
        dedicated_poller,

        // No extra OS thread. The backend registers a non-blocking,
        // single-pass poll hook with its pool_service; ordinary pool
        // workers run it (see pool_service::worker_loop()) as one more
        // thing to try between picking up queued tasks, whenever one of
        // them isn't already handling it. Keeps the process at exactly
        // `concurrency()` threads at the cost of some added latency under
        // very light load, since a hook only runs when some worker happens
        // to loop around to it.
        shared_worker,
    };

    inline constexpr policy dedicated_poller = policy::dedicated_poller;
    inline constexpr policy shared_worker = policy::shared_worker;

    // How a shared_worker-policy worker's poll_once() call processes
    // registered hooks (see poll_registry) -- meaningless for
    // dedicated_poller, which already blocks in one specific backend's
    // native wait call on its own committed OS thread instead.
    enum class task_type : std::uint8_t {
        // One poll_once() call sweeps every registered hook (the original,
        // still-default behavior) -- fewer calls needed to cover them all,
        // but one hook with a lot of ready work can make a single call take
        // longer, delaying the others registered alongside it.
        segment,
        // One poll_once() call advances exactly one hook, round-robin
        // across calls -- finer-grained fairness between hooks, at the
        // cost of needing more calls to cover them all.
        individual,
    };

    inline constexpr task_type segment = task_type::segment;
    inline constexpr task_type individual = task_type::individual;

}

// Compile-time default for pool_service::execution_policy(), independent of
// which (if any) FCS_WORKER_BACKEND_* is selected -- pool_service itself
// works with no I/O backend at all (see benchmarks/bench_pool.cpp), so this
// intentionally doesn't route through config.hpp's exactly-one-backend
// check. Overridable per-build (e.g.
// -DFCS_WORKER_DEFAULT_EXECUTION_POLICY=FCS_WORKER_EXECUTION_DEDICATED_POLLER
// to keep every pool_service defaulting to the old always-a-thread
// behavior); service.execution_policy(...) still overrides it per instance
// at runtime regardless of this default.
#define FCS_WORKER_EXECUTION_DEDICATED_POLLER 0
#define FCS_WORKER_EXECUTION_SHARED_WORKER 1

#if !defined(FCS_WORKER_DEFAULT_EXECUTION_POLICY)
#  define FCS_WORKER_DEFAULT_EXECUTION_POLICY FCS_WORKER_EXECUTION_SHARED_WORKER
#endif
