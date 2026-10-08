#pragma once

#include "../types.hpp"

#include "inline_function.hpp"

#include <chrono>

namespace FCS::Worker::detail {

    // Inline, move-only, never allocates (see inline_function.hpp). The capacity is a template
    // parameter -- a pool picks it with traits::task_bytes -- so one pool can run 64-byte
    // tasks and another 4 KB ones, each with queue slots sized exactly for its own work.
    template<std::size_t Bytes>
    using task_for = inline_function<void(), Bytes>;
    using task = task_for<default_task_bytes>;

    // Scheduling priority within the fast lane only -- slow_ and the
    // cushion (admission_gate) are untouched by this. `high` is meant for
    // latency-sensitive completions (e.g. enqueue_until() timeouts); see
    // priority_mpmc_queue.hpp for how it's enforced without starving normal.
    enum class task_priority : unsigned char { normal, high };

    template<std::size_t Bytes>
    struct basic_queued_task {
        // small scalars first, the task last: scheduling decisions read these without ever
        // pulling the (large) task body into cache
        workload lane{};
        source_kind source{};
        task_priority priority{task_priority::normal};
        task_for<Bytes> invoke;

        basic_queued_task() noexcept = default;
        // Parameter order is the historical one (lane, source, task, priority) -- it is only the
        // member order that was rearranged for cache locality.
        basic_queued_task(workload l, source_kind s, task_for<Bytes> t, task_priority p = task_priority::normal) noexcept
            : lane(l), source(s), priority(p), invoke(std::move(t)) {}
    };
    using queued_task = basic_queued_task<default_task_bytes>;

}
