#include <FCS/Worker/workers.hpp>
#include <FCS/Worker/sync/interruptible_mutex.hpp>
#include <FCS/Worker/sync/ordered_mutex.hpp>
#include <iostream>
#include <chrono>
#include <mutex>
namespace worker = FCS::Worker;
namespace sync = FCS::synchronization;
namespace func_namespace = FCS::Worker::detail;

using namespace std::chrono_literals;

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

    // The backbone of FCS-Workers is inline_function
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

    // ASan will trigger stack overflow due to FCS-Workers's heavy usage on stack allocation. Explicitly allocates on heap to avoid the error.
    std::unique_ptr<worker::pool_service<>> service_ptr = std::make_unique<FCS::Worker::pool_service<>>();
    worker::pool_service<>& service = *service_ptr; // pool_service do not allow copying. Borrow via lvalue instead.
    /* service
        .concurrency(std::thread::hardware_concurrency())
        .burst_backpressure()
        .execution_policy(worker::execution::shared_worker); // default. Workers take turn polling I/O. For low-latency: use worker::execution::dedicated_poller.
    */
    service.start();
    // Fire-and-forget
    auto accepted = service.enqueue([](int a, int b) { std::cout << (a + b) << '\n'; }, 30, 40); // for lane control: enqueue(workload::Fast / workload::Slow, cb). Fine-grained: enqueue_priority();
    if(!accepted) {
        // Rejected, do something.
    }
    auto subscription_handle = service.subscribe<event_tag>([](std::uint64_t id) {
        // Do something with ID.
    });
    service.post<event_tag>(1);

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
    // Timers
    auto timer_accepted = service.enqueue_until(worker::timeout_mode::timer, 50ms, [] { std::cout << "Matured! For precision: it's nanoseconds by default.\n"; });
    // WARNING: timeout_mode::timer MODE IS INSTRUSIVE. IT CAN SWAP PLACES WITH YOUR OTHER TASKS ON THE WORKER'S LOCAL QUEUE. For non-critical timeout: prefer lane-based firing mode instead. (worker::timeout_mode::fast_lane, worker::timeout_mode::slow_lane)

    // Or bound the timer to existing event:
    timer_accepted = service.enqueue_until<event_tag>(worker::timeout_mode::event_post, 50ms, [](std::uint64_t){}, std::uint64_t{1} /* Args here based on subscription template */);
    // subscription_handle.cancel();

    // the single-threaded coroutine:
    auto work_stream = work1();
    (void)work_stream.move_next();

    // telemetries
    // service.scheduler_metadata();
    // service.worker_profile(worker id)
    // service.debug_dump(); // by default: stderr
    service.stop(); // Stop just kills the runtime (waiting for worker threads to join), you can restart it. Some configs can be made on the fly (hot-swappable).

    // I also provide an interruptible mutex if you prefer:
    {
        // Variant one: unfair, barging is allowed
        sync::interruptible_mutex mtx;
        mtx.lock(); // this is irrecoverable deadlock. Standard mutex guarantee.
        mtx.unlock();
        if(mtx.lock_interruptible()) {
            // acquired, do something...
            mtx.unlock();
        }
        // STL std::lock_guard works
        {
            std::lock_guard<sync::interruptible_mutex> guard {mtx};
            // do something
        }
        // Or the interruptible
        {
            sync::interruptible_lock_guard guard{ mtx };
            guard.rollback(); // interrupt all
            std::this_thread::sleep_for(5ms); // let waiters wrap up first
            mtx.reset_interrupt();
            // Throwing variant: sync::interruptible_lock_guard_throwable
        }
    }
    {
        // Or, if you want fairness:
        sync::small_ordered_mutex mtx; // Alias for sync::ordered_mutex<std::uint8_t>
        // The same thing, really you would expect from mutex.
    }
    return 0;
}