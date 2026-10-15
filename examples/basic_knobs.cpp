/**
    * Copyright (c) 2026, Fallen One
    * SPDX-License-Identifier: BSD-3-Clause
    * See the LICENSE file in the project root for license information.

    Exploring knobs FCS-Workers offer, this covers only knobs you likely care the most.
    For full knobs, consult include/FCS/Worker/pool_service.hpp
*/
#include <FCS/Worker/workers.hpp>
#include <chrono>
namespace worker = FCS::Worker;

using namespace std::chrono_literals;

int main() {
    worker::pool_service<> pool;
    pool
        .concurrency(std::thread::hardware_concurrency() - 1)
        /*
            concurrency, as the name suggests, allows you to set how many
            thread counts are available for workers on this pool object.
         */
         .execution_policy(worker::execution::dedicated_poller)
         /*
            execution_policy is where you control trade-offs between I/O responsiveness
            and compute throughput. There are 2 overloads for execution_policy,
            one: just the execution policy. How it behaves:
                - dedicated_poller dedicate an entire OS thread to continuously poll on the kernel
                    best sleeping behavior (no wakeups to check I/O poller overhead).
                - shared_worker no dedicated poller is created, and I/O poll is a duty of the workers itself
                    best used if I/O is not critical and pure throughput. For shared_worker, execution_policy has an overload
            two: execution policy plus task granularity
                - dedicated_poller unaffected
                - shared_worker task granularity type (segment or individual? available in worker::execution::task_type)
                    segment: I/O is going to be polled after every N segments of tasks is executed
                    individual: I/O is going to be polled after N tasks is executed

                    between_num_threads: how many threads are allowed to poll. Zero means 1. Other workers won't poll at once
                                         set closer to your CPU thread count, and it will busy poll together.  
         */
         .burst_backpressure()
         /*
            burst_backpressure, the built-in admission gate, internally called Cushion FIFO.
            Controls how many tasks are allowed to be put into this queue before FCS-Workers explicitly reject the task.
            It uses hysteresis model to prevent flapping, which has CAS overhead.
            Parameters: full (default (queue_capacity * 2) - 1), available (default queue_capacity / 4),
            candidates (default 80)
            Full: How many entries allowed before admission reject tasks and closes.
            Available: Minimum entries allowed before admission opens the gate.
            Candidates: Scanning budget on the admission queue (internally called cushion FIFO) for work lanes classification.
         */
         .watchdog_interval(5ms)
         /*
            watchdog_interval controls the built-in supervisor of FCS-Workers to detect stalled threads.
            It uses userspace calculation and cannot make a difference between true wedged and descheduled.
            Set the interval for watchdog to wake up every N nanoseconds to scan for threads.
            Set them to 0ns to disable the built-in watchdog.
            Lower values = faster detection, lots of spurious stall.
            Higher values = slower detection, least chances of spurious stall.
            By default it's set to 5ms.
         */;
}