# mauryzync
Header-only fun C++ dependency-free asynchronous runtime.

Internally called "FCS-Workers", is an asynchronous runtime written in C++20 backed with kernel I/O multiplexer. Then why "mauryzync"? I kept thinking about a bird named Mauri in a comic I read, zync is just me rhyming it.

Current support:
 - IOCP
 - epoll
 - io_uring

# What this is
An asynchronous runtime I make because I like solving systems problem at practical level. The aim is, "what if we have a bounded asynchronous runtime that spirally goes down to even work itself (arbitrary function)".

It is deliberately low-level, opinionated machinery, unopinionated policy. You build something on top of it.

What is "FCS"? "Fun C++ Server"

# What this is not
- Replacement for libuv/libevent/asio
- Competitor to tokio, seastar
- Drop-in replacement to your existing production
- Winning leaderboards for async runtime (I only provide stressors)

# Disclaimer
Some of the code were AI generated (directed by me), and some of kernel-specific codes were written by me.

You can use AI to contribute, so long you understand what you are proposing and code.

# Examples
see `src/example.cpp`. For full API (including configurations), consult `include/FCS/Workers/pool_service.hpp`

## Overview
```cpp
#include <FCS/Workers/workers.hpp>
#include <iostream>
namespace worker = FCS::Worker;

struct event_tag{};

// pull-based coroutine, you drive it (utilities, you can't pass this to the worker as it is non-thread safe)
FCS::pull_result<std::uint64_t> work() {
    co_return 10;
}

// chunked (same as above, but chunks)
FCS::chunk_stream<std::uint64_t> work1() {
    co_yield 10;
    co_yield 30;
    co_yield 40;
    co_return;
}

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(0);
    worker::pool_service service;
    /* service
        .concurrency(std::thread::hardware_concurrency())
        .burst_backpressure()
        .execution_policy(worker::execution::shared_worker); // default. Workers take turn polling I/O. For low-latency: use worker::execution::dedicated_poller.
    */
    service.start();
    // Fire-and-forget
    auto accepted = service.enqueue([](int a, int b) { std::cout << (a + b) << '\n'; }); // for lane control: enqueue(workload::Fast / workload::Slow, cb). Fine-grained: enqueue_priority();
    if(!accepted) {
        // Rejected, do something.
    }
    auto subscription_handle = service.subscribe<event_tag>([](std::uint64_t id) {
        // Do something with ID.
    });
    service.post<event_tag>(1);

    // Future
    auto fut = service.enqueue_and_poll(FCS::Worker::workload::Fast, [iterations]() -> FCS::async_result<std::uint64_t> {
        // For checkpoint:
        // co_await worker::yield_point();
        // Non-coroutine:
        // worker::cooperative_yield_requested()
        // Even though it's bounded, this work is not dropped.
        co_return 10 + 30; // Or FCS::Unexpected{error}
    });

    while(!fut.ready()) std::this_thread::yield(); // Or better mechanism (it's awaitable if you're running this on coroutine)

    auto my_data = fut.user_data();
    if(!my_data) {
        /* handle pool error.
            not_ready
            queue_full
            callback_threw
            see "types.hpp"
        */
    }
    std::cout << *my_data << " Worker ID: " << fut.worker_id() << " In Sequence: " << fut.sequence() << '\n';
    // Timers
    auto timer_accepted = service.enqueue_until(worker::timeout_mode::timer, 50ms, []() { std::cout << "Matured! For precision: it's nanoseconds by default.\n"; });
    // WARNING: timeout_mode::timer MODE IS INSTRUSIVE. IT CAN SWAP PLACES WITH YOUR OTHER TASKS ON THE WORKER'S LOCAL QUEUE. For non-critical timeout: prefer lane-based firing mode instead. (worker::timeout_mode::fast_lane, worker::timeout_mode::slow_lane)

    // Or bound the timer to existing event:
    timer_accepted = service.enqueue_until<event_tag>(worker::timeout_mode::fast_lane, 50ms, /*callback unused*/ []{}, std::uint64_t{1}, /* Args here */);
    // subscription_handle.cancel();


    // telemetries
    // service.scheduler_metadata();
    // service.worker_profile(worker id)
    // service.debug_dump(); // by default: stderr
    service.stop(); // Stop just kills the runtime, you can restart it. Some configs can be made on the fly (hot-swappable).
    return 0;
}
```
There are tuning mechanism available for you to configure, see `include/FCS/Worker/pool_service.hpp`

# OS and architecture tested
- Android ARM64 (via Termux)
- Raspberry Pi 5
- Windows (i3-9100F, i3-3240)
- Linux

# License
The project is using a permissive BSD 3-Clause license, free of charge. Do not attribute me for derivates (attribution). See `LICENSE.`
