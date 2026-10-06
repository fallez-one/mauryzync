#include <FCS/Worker/workers.hpp>

#include <atomic>
#include <cstdint>
#include <iostream>

struct tick {};
struct other_tick {};

FCS::chunk_stream<int> stream_generator() {
    co_yield 1;
}

FCS::async_result<int> completion_generator() {
    co_return 7;
}

int main() {
    FCS::Worker::pool_service service;
    // execution::shared_worker (the default) polls the backend's native
    // multiplexer non-blockingly from ordinary pool workers -- no dedicated
    // OS thread. Ask for execution::dedicated_poller instead if you want a
    // thread permanently blocked in the kernel wait call for lower latency.
    service.concurrency(2).burst_backpressure().execution_policy(FCS::Worker::execution::shared_worker).start();

    std::atomic_uint64_t observed{};
    std::atomic_uint64_t other_observed{};
    const auto subscribed = service.subscribe<tick>([&](std::uint64_t value) {
        observed.fetch_add(value, std::memory_order_relaxed);
    });
    const auto other_subscribed = service.subscribe<other_tick>([&](std::uint64_t value) {
        other_observed.fetch_add(value, std::memory_order_relaxed);
    });

    FCS::Worker::eventlooper loop{service};
    const auto fast_accepted = service.enqueue(FCS::Worker::workload::Fast, [](int left, int right) { std::cout << left + right << '\n'; }, 20, 22);
    const auto slow_accepted = service.enqueue(FCS::Worker::workload::Slow, [] { /* bounded slow lane */ });
    const auto posted = service.post<tick>(std::uint64_t{1});
    const auto other_posted = loop.post<other_tick>(std::uint64_t{2});
    // chunk_stream<T>/pull_result<T> (coroutine.hpp) are standalone, pull-
    // based "normal coroutines" -- written and consumed on one thread, no
    // pool involvement. Used directly, not through the pool:
    auto stream = stream_generator();
    (void)stream.move_next();
    // For a coroutine whose *result* needs to reach another thread (e.g.
    // awaited from outside), enqueue_and_poll() auto-polls an
    // async_result<T>-returning callback on a worker instead:
    auto completion = service.enqueue_and_poll(FCS::Worker::workload::Fast, [] { return completion_generator(); });
    (void)subscribed;
    (void)other_subscribed;
    (void)fast_accepted;
    (void)slow_accepted;
    (void)posted;
    (void)other_posted;
    (void)completion;

    loop.start();
    service.stop();
    std::cout << observed.load() << ' ' << other_observed.load() << '\n';
}