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
#include <FCS/Worker/workers.hpp>
namespace worker = FCS::Worker;

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(0);

    // ASan will trigger stack overflow due to FCS-Workers's heavy usage on stack allocation. Explicitly allocates on heap to avoid the error.
    std::unique_ptr<worker::pool_service<>> service_ptr = std::make_unique<FCS::Worker::pool_service<>>();
    worker::pool_service<>& service = *service_ptr; // pool_service do not allow copying. Borrow via lvalue instead.
    service.start();
    // Fire-and-forget
    auto accepted = service.enqueue([](int a, int b) { std::cout << (a + b) << '\n'; }, 30, 40); // for lane control: enqueue(workload::Fast / workload::Slow, cb). Fine-grained: enqueue_priority();
    if(!accepted) {
        // Rejected, do something.
    }

    // Future
    auto fut = service.enqueue_and_poll(FCS::Worker::workload::Fast, []() -> FCS::async_result<std::uint64_t> {
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
    std::cout << *my_data << " Worker ID: " << *fut.worker_id() << " In Sequence: " << fut.sequence() << '\n';

    // Under the hood, enqueue relies on inline_function. enqueue and practically almost anything on FCS-Workers
    // is using inline_function.
    {
        std::cout << std::boolalpha;
        static constexpr std::size_t CapturableSizeForFunc1 = 8;
        func_namespace::inline_function<int(), CapturableSizeForFunc1> hey_test = [] {
            return 1 + 2;
        };
        std::cout << "hey_test(): " << (hey_test() == 3) << '\n';
        // By default, the pool uses default_callback_bytes, which is 192 bytes.
        // If you capture a variable too large (pointers included), it will throw a compile time error
        // rather than silently allocating to heap.
        // To opt-in using heap allocation (which makes the capture variable a heap), use:
        func_namespace::inline_function<int(), CapturableSizeForFunc1> boxed = worker::boxed([] {
            return 1 + 3;
        });
        std::cout << "boxed(): " << (boxed() == 4) << '\n';
        std::cout << std::noboolalpha;
    }

    // telemetries
    // service.scheduler_metadata();
    // service.worker_profile(worker id)
    // service.debug_dump(); // by default: stderr
    service.stop(); // Stop just kills the runtime (waiting for worker threads to join), you can restart it. Some configs can be made on the fly (hot-swappable).
    return 0;
}
```
There are tuning mechanism available for you to configure, see `include/FCS/Worker/pool_service.hpp`
More detailed API usage in `examples/example.cpp`!

# Tested compiler support
- clang (LLVM/clang) version 21.1.8
- g++ (GCC) 15.3.0
- cl (MSVC) 19.51.36257

# OS and architecture tested
- Android ARM64 (via Termux)
- Raspberry Pi 5
- Windows (i3-9100F, i3-3240)
- Linux

# License
The project is using a permissive BSD 3-Clause license, free of charge. Do not attribute me for derivates (attribution). See `LICENSE.`