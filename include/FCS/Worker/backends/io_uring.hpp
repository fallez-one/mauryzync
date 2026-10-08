#pragma once

#include "../completion.hpp"
#include "../eventloop.hpp"
#include "../execution.hpp"
#include "detail/io_classifier.hpp"
#include "detail/io_uring_ring.hpp"
#include "detail/mpsc_stack.hpp"
#include "detail/slot_table.hpp"
#include "detail/source_types.hpp"

#ifndef FCS_UNSUPPORTED_FALLBACK_EPOLL

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

namespace FCS::Worker::backend::io_uring {

    // Names from the backend-neutral detail namespace; `detail` alone means
    // io_uring::detail (the ring) inside this namespace.
    namespace common = ::FCS::Worker::backend::detail;

    // Completion-driven backend with the same public surface as epoll::backend:
    // register_source() (with recv_option Options) and register_sink() (full
    // duplex on one fd = two independent registrations).
    //
    //  * Reads are genuinely proactive: every source keeps one buffer-backed
    //    IORING_OP_READ/RECV/RECVMSG submitted at all times; Reader is handed a
    //    common::completed_source over bytes that are already in the buffer.
    //  * Writes have no data to submit ahead of time (the Writer produces it),
    //    so a sink is a one-shot IORING_OP_POLL_ADD(POLLOUT) re-armed after
    //    every firing -- level-triggered, exactly like epoll's EPOLLOUT -- and
    //    Writer is handed the very same common::readiness_sink epoll gives it.
    //
    // Threading model (contrast with the old mutex-per-submit design):
    //  * One *driver* at a time -- the dedicated poller thread, or whichever
    //    pool worker holds this backend's poll hook -- owns the ring and every
    //    registration's memory. Only the driver submits, reaps, and frees.
    //  * Any thread may register/cancel. They never touch the ring or a
    //    registration that's already published: registration claims a slot in
    //    `slots_state_` (lock-free), publishes the registration and pushes an
    //    `arm` command onto `cmds_` (lock-free MPSC); cancellation flips the
    //    slot's state word and pushes a `cancel` command. The driver turns
    //    commands into SQEs. A sleeping dedicated poller is woken through an
    //    eventfd only when it announced it was about to sleep.
    //  * No mutex anywhere. A registration's memory is freed only after the
    //    kernel delivered its terminal completion, so the kernel can never
    //    write into a buffer that's been freed -- including at shutdown, which
    //    cancels and drains everything before anything is deleted.
    template<typename Pool = pool_service<>>
    class backend final : public generic_eventlooper<backend<Pool>, int, Pool> {
    public:
        explicit backend(Pool& service);
        ~backend();

        backend(const backend&) = delete;
        backend& operator=(const backend&) = delete;

        void start_backend();
        void stop_backend() noexcept;

        // On (the default) rearms discovered while reaping a batch of
        // completions are flushed to the kernel in one io_uring_enter() that
        // also waits for the next completions. Off submits each one
        // immediately (one syscall apiece); only useful for comparison.
        void enable_batch(bool enabled) noexcept { batch_.store(enabled, std::memory_order_relaxed); }

        // 0 while the ring is usable (or not yet started); otherwise the errno
        // io_uring_setup() failed with (EPERM = blocked by seccomp/sysctl,
        // ENOSYS = no kernel support). Registrations made while this is
        // non-zero are accepted but will never fire -- check it (or the
        // configure-time probe, cmake/ProbeIoUring.cmake) instead of hoping.
        [[nodiscard]] int setup_error() const noexcept { return setup_error_.load(std::memory_order_relaxed); }

        template<typename Tag, std::size_t Size, common::recv_option Options = common::recv_option::none,
                 typename Reader, typename Source, typename Callback>
        [[nodiscard]] subscription register_source(Source&& source, Reader&& reader, Callback&& callback);

        template<typename Tag, std::size_t Size, typename Writer, typename Sink, typename Callback>
        [[nodiscard]] subscription register_sink(Sink&& sink, Writer&& writer, Callback&& callback);

    private:
        static constexpr std::size_t capacity = 1024;
        using table = common::slot_table<capacity>;

        enum class direction : std::uint8_t { read, write };
        enum class action : std::uint8_t { keep, retire };

        // Everything in here other than `h` is driver-owned once published.
        struct registration {
            table::handle h{};
            int fd{-1};
            source_kind kind{source_kind::io};
            direction dir{direction::read};
            int recv_flags{};          // MSG_* for RECV/RECVMSG (read direction only)
            bool in_flight{};          // an SQE for this registration is outstanding
            bool cancel_sent{};
            std::size_t buffer_size{};
            std::unique_ptr<std::byte[]> buffer;
            // Only populated for kind == datagram: RECVMSG delivers payload
            // into `buffer` through iov and the sender into peer_addr.
            std::unique_ptr<sockaddr_storage> peer_addr;
            std::unique_ptr<struct iovec> iov;
            std::unique_ptr<struct msghdr> msg;
            std::chrono::steady_clock::time_point submitted_at{};
            ::FCS::Worker::detail::callback_function<action(registration&, std::int32_t)> on_event;
            subscription channel_subscription;

            [[nodiscard]] std::uint64_t user_data() const noexcept {
                return (std::uint64_t{h.generation} << 32) | static_cast<std::uint64_t>(h.index);
            }
        };

        struct command {
            enum class kind : std::uint8_t { arm, cancel };
            command* next{};
            kind what{};
            table::handle h{};
        };

        // Real registrations use (generation << 32 | index) with index < capacity,
        // so an all-ones low word can never collide with one.
        static constexpr std::uint64_t wake_user_data = ~std::uint64_t{0};
        static constexpr std::uint64_t ack_user_data = ~std::uint64_t{0} - 1;

        // A handle unpacked from a subscription only carries as many
        // generation bits as fit in a std::size_t next to the index.
        [[nodiscard]] static constexpr bool same_generation(std::uint32_t a, std::uint32_t b) noexcept {
            return (a & table::generation_mask) == (b & table::generation_mask);
        }

        // --- any thread ---
        [[nodiscard]] static bool cancel_slot(void* owner, std::size_t packed) noexcept;
        [[nodiscard]] bool publish(registration* registration_ptr) noexcept;
        void post_command(command::kind what, table::handle h) noexcept;
        void wake() noexcept;
        void wake_if_sleeping() noexcept;
        static void prepare_datagram_control(registration& registration_ref);

        // --- driver only ---
        void run_loop();                                          // execution::dedicated_poller
        [[nodiscard]] static bool poll_once(void* owner) noexcept; // execution::shared_worker
        [[nodiscard]] bool service_commands() noexcept;
        void sweep_cancelled() noexcept;
        [[nodiscard]] unsigned reap(unsigned max);
        void handle_cqe(std::uint64_t user_data, std::int32_t res);
        [[nodiscard]] io_uring_sqe* acquire_sqe() noexcept;
        [[nodiscard]] bool queue_operation(registration& registration_ref) noexcept;
        void queue_cancel_of(std::uint64_t target) noexcept;
        void arm_wakeup() noexcept;
        void finish(registration* registration_ptr, bool deliver_pending = false) noexcept;
        void shutdown_ring() noexcept;
        void discard_unarmed() noexcept;

        detail::ring ring_;
        int wakeup_fd_{-1};
        std::thread poller_;
        std::atomic_bool polling_{};
        std::atomic_bool sleeping_{};      // dedicated poller only: "I'm about to block in io_uring_enter"
        std::atomic_bool batch_{true};
        std::atomic_bool sweep_needed_{};  // a cancel command couldn't be allocated; rescan for cancelled slots
        std::atomic_int setup_error_{0};
        std::size_t poll_hook_{};

        // Driver-only (no synchronization needed beyond the hand-off between
        // successive drivers, which poll_registry's busy flag already provides).
        std::size_t inflight_{};   // SQEs queued whose CQE hasn't been reaped yet
        bool wake_armed_{};
        bool woke_{};

        table slots_state_;
        std::array<std::atomic<registration*>, capacity> slots_{};
        common::mpsc_stack<command> cmds_;
    };

    using eventlooper = backend<pool_service<>>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;

}

#include "io_uring.inl"

#else

#include "epoll.hpp"

namespace FCS::Worker::backend::io_uring {
    // This Linux's uapi headers have no io_uring support at all (see
    // detail/io_uring_ring.hpp) — fall back to the epoll backend wholesale
    // rather than failing the build.
    template<typename Pool = pool_service<>>
    using backend = FCS::Worker::backend::epoll::backend<Pool>;
    using eventlooper = backend<pool_service<>>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;
}

#endif
