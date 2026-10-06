#pragma once

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <utility>

namespace FCS::Worker {

    enum class workload : unsigned char { Fast, Slow };

    // Error-as-values for this library's own fallible paths (FCS's
    // convention -- see FCS::Expected/FCS::Unexpected in fcs/expected.hpp).
    // Not exhaustive; extend as more paths get converted off exceptions.
    enum class error : unsigned char {
        not_ready,      // async_completed_result accessed before it was ready
        queue_full,     // enqueue()/submit() was rejected by backpressure
        callback_threw, // enqueue_and_poll's callback threw -- the exception itself isn't preserved, only the fact that one happened
    };

    // Dispatch mode for pool_service::enqueue_until()'s fired callback:
    //   timer      -- bypasses the cushion, injected straight into the fast
    //                 lane at high priority (see priority_mpmc_queue.hpp).
    //                 Always wins ordering; a burst of these can starve
    //                 fast/slow -- a deliberate tradeoff for deadline work.
    //   event_post -- delivered inline on the firing thread via post_from<Tag>
    //                 (Tag is enqueue_until's explicit template parameter)
    //                 rather than queued as a task.
    //   fast_lane / slow_lane -- queued normally through the cushion, same
    //                 as enqueue(workload::Fast / Slow, ...).
    enum class timeout_mode : unsigned char { timer = 0, event_post = 1, fast_lane = 2, slow_lane = 3 };

    // Checking cadence for pool_service::age_starvation()'s anti-starvation
    // aging (see detail/priority_mpmc_queue.hpp): `task` re-evaluates on
    // every fast-lane try_pop; `segment` only at each fairness-window
    // boundary (coarser, cheaper).
    enum class age_clamp : unsigned char { task, segment };
    // network: connection-oriented (TCP); datagram: connectionless (UDP),
    // whose Source additionally captures a sender address per read.
    enum class source_kind : unsigned char { synthetic, io, network, datagram };
    enum class queue_state : unsigned char { empty, available, occupied, full };

    // A move-only, type-erased cancellation token returned by every subscribe() call.
    // Cancelling is idempotent and safe to call from the owning thread or on destruction.
    class subscription {
    public:
        using cancel_fn = bool (*)(void*, std::size_t) noexcept;

        subscription() noexcept = default;
        subscription(void* owner, cancel_fn cancel, std::size_t slot) noexcept;
        subscription(const subscription&) = delete;
        subscription& operator=(const subscription&) = delete;
        subscription(subscription&& other) noexcept;
        subscription& operator=(subscription&& other) noexcept;
        ~subscription();

        [[nodiscard]] bool cancel() noexcept;

        // For a *channel* subscription (what pool_service::subscribe<Tag>()
        // returns) whose owner is going away on its own -- a backend whose
        // source reached EOF/error and has already post()ed that final
        // completion. Like cancel() it stops any *future* post() from reaching
        // the callback, but it does NOT suppress deliveries already queued:
        // cancel() marks the subscriber inactive, so a final `closed`/`error`
        // completion still sitting in the pool's queue would be silently
        // dropped the moment its registration was freed. Not meaningful on a
        // backend registration handle (their cancel functions don't expect it).
        [[nodiscard]] bool retire() noexcept;
        static constexpr std::size_t retire_flag = std::size_t{1} << (sizeof(std::size_t) * 8 - 1);

        [[nodiscard]] bool active() const noexcept;
        explicit operator bool() const noexcept;

    private:
        void* owner_{};
        cancel_fn cancel_{};
        std::size_t slot_{};
    };

    template<std::size_t Size>
    struct read_result {
        std::array<std::byte, Size> buffer{};
        std::size_t size{};

        [[nodiscard]] std::span<std::byte> bytes() noexcept { return {buffer.data(), size}; }
        [[nodiscard]] std::span<const std::byte> bytes() const noexcept { return {buffer.data(), size}; }
    };

    template<std::size_t Size, typename Reader, typename Source>
    concept reader_callable = requires(Reader reader, Source source) {
        { std::invoke(reader, source) } -> std::same_as<read_result<Size>>;
    };

    // Write-side counterpart of reader_callable. A Writer is invoked with a
    // mutable Sink (readiness_sink for epoll/kqueue, completed_sink for
    // io_uring/IOCP) and returns how many bytes it pushed through it.
    template<typename Writer, typename Sink>
    concept writer_callable = requires(Writer writer, Sink sink) {
        { std::invoke(writer, sink) } -> std::convertible_to<std::size_t>;
    };

    // Tags a native handle with the traffic class an I/O multiplexer backend should treat
    // it as. Backends fall back to their own classifier when kind is left as its default.
    template<typename Native>
    struct source_ref {
        Native native;
        source_kind kind{source_kind::io};
    };

    struct scheduler_snapshot {
        std::uint64_t submitted{};
        std::uint64_t completed{};
        std::uint64_t active{};
        std::uint64_t rejected{};
        std::uint64_t synthetic_submitted{};
        std::uint64_t io_submitted{};
        std::uint64_t network_submitted{};
        std::uint64_t cushion_depth{};
        std::uint64_t mpmc_depth{};
        std::uint64_t local_depth{};
        std::uint64_t refill_claims{};
        std::uint64_t drain_claims{};
        std::uint64_t steals{};
        queue_state cushion{queue_state::empty};
        queue_state mpmc{queue_state::empty};
        queue_state admission{queue_state::available};
    };

}

#include "types.inl"
