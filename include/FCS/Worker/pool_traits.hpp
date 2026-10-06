#pragma once

#include "detail/inline_function.hpp"

#include <chrono>
#include <concepts>
#include <cstddef>
#include <semaphore>
#include <type_traits>
#include <utility>

namespace FCS::Worker {

    // What a worker parks on. Needs only these three operations (initial count 0, at most one
    // pending token -- the library never releases an already-released semaphore) -- so
    // std::binary_semaphore, a futex wrapper, an event object, or a test double all qualify.
    template<typename S>
    concept BinarySemaphore = std::is_constructible_v<S, std::ptrdiff_t> && requires(S s, std::chrono::nanoseconds d) {
        s.release();
        s.acquire();
        { s.try_acquire_for(d) } -> std::convertible_to<bool>;
    };

    // The pool's pluggable types and compile-time sizes, in one place; every piece has a safe,
    // bounded default. (The admission gate / cushion FIFO is deliberately NOT here: it stays the
    // library's own concrete detail::admission_gate.)
    struct default_pool_traits {
        // The parking primitive (see BinarySemaphore).
        using semaphore = std::binary_semaphore;

        // Pending enqueue_until() timers (a bounded min-heap, see detail::timer_heap); registering
        // one more than this is refused.
        static constexpr std::size_t timer_capacity = 1024;

        // Bytes of inline storage in every scheduler task / long-lived callback of this pool. A
        // callable larger than this does not compile (use FCS::Worker::boxed(), or a pool with a
        // bigger size) -- nothing ever allocates behind your back. Every queue slot is sized for
        // task_bytes, so a pool that only runs small work should say so: 64 gives slots about a
        // sixth the size. The default holds a posted completion<256>.
        static constexpr std::size_t task_bytes = detail::default_task_bytes;
        static constexpr std::size_t callback_bytes = detail::default_callback_bytes;
    };

}
