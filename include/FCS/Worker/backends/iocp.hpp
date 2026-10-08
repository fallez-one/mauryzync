#pragma once

#include "../completion.hpp"
#include "../eventloop.hpp"
#include "../execution.hpp"
#include "detail/io_classifier.hpp"
#include "detail/slot_table.hpp"
#include "detail/source_types.hpp"

#ifdef _WIN32

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

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
// winsock2.h must be included before windows.h (see detail/io_classifier.hpp) so
// that registering Winsock sockets (SOCKET, WSARecv, ...) below compiles cleanly.
#include <winsock2.h>
#include <windows.h>

namespace FCS::Worker::backend::iocp {

    namespace common = ::FCS::Worker::backend::detail;

    // Completion-driven backend with the same public surface as the epoll and
    // io_uring backends: register_source() (with recv_option Options) and
    // register_sink() (full duplex on one socket = two independent
    // registrations).
    //
    //  * Reads are genuinely proactive: each source keeps one buffer-backed
    //    overlapped ReadFile/WSARecv/WSARecvFrom outstanding at all times;
    //    Reader is handed a common::completed_source over bytes that are
    //    already in the buffer. Winsock sockets are armed with WSARecv /
    //    WSARecvFrom (datagram, to capture the sender) -- ReadFile is not
    //    guaranteed to work on a SOCKET from a non-IFS provider.
    //  * Writes have no data to submit ahead of time (the Writer produces
    //    it), so a sink is a zero-byte overlapped WSASend, re-armed after
    //    every firing, whose completion means "the socket will take a send";
    //    Writer then gets the same common::readiness_sink epoll gives it and
    //    performs the real send(). Sockets only (source_kind::network /
    //    datagram); a file/pipe HANDLE sink is refused (empty subscription).
    //    LIMIT: Winsock completes a zero-byte send without waiting for buffer
    //    space, so unlike EPOLLOUT this does not sleep while the socket is
    //    full -- a Writer that keeps being invoked against a full buffer
    //    spins through completions instead of blocking.
    //
    // Threading model (the same one the io_uring backend uses; no mutex):
    //  * One *driver* at a time -- the dedicated poller thread, or whichever
    //    pool worker holds this backend's poll hook -- owns every
    //    registration's memory and is the only thread that arms, reaps,
    //    cancels and frees. poll_registry's `busy` flag hands the role over
    //    with acquire/release.
    //  * Any thread may register/cancel, but never touches a published
    //    registration: registering claims a slot in a lock-free slot_table
    //    and posts an `arm` command; cancelling flips the slot's state word
    //    and posts a `cancel` command. Commands travel through the
    //    completion port itself (PostQueuedCompletionStatus), which is both
    //    the lock-free queue and the wake-up -- no eventfd analogue needed.
    //  * A registration is freed only after the kernel delivered its terminal
    //    completion, so no OVERLAPPED/buffer is ever freed under pending I/O --
    //    including at shutdown, which cancels and drains before deleting.
    //
    // Socket requirement: sockets must be overlapped (socket() always is;
    // WSASocket() needs WSA_FLAG_OVERLAPPED, and accept() inherits the
    // listener's mode). On a non-overlapped socket Windows runs WSARecv/WSASend
    // as blocking calls that never complete through the port -- the driver would
    // stall inside arm(). It cannot be detected portably, so it is a caller contract.
    //
    // Association caveat: a handle can be associated with a completion port
    // only once. Registering the second direction of a full-duplex socket
    // therefore sees ERROR_INVALID_PARAMETER from CreateIoCompletionPort and
    // treats it as "already associated with this backend's port".
    template<typename Pool = pool_service<>>
    class backend final : public generic_eventlooper<backend<Pool>, HANDLE, Pool> {
    public:
        explicit backend(Pool& service);
        ~backend();

        backend(const backend&) = delete;
        backend& operator=(const backend&) = delete;

        void start_backend();
        void stop_backend() noexcept;

        // 0 while the port is usable; otherwise the Win32 error that
        // creating it failed with. Registrations made then never fire.
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

        struct registration;

        // The OVERLAPPED the kernel owns while an operation is in flight.
        // Deriving (rather than CONTAINING_RECORD) lets the completion packet's
        // OVERLAPPED* be turned straight back into its registration.
        struct op_block : OVERLAPPED {
            registration* owner{};
        };

        struct registration {
            table::handle h{};
            HANDLE handle{};
            SOCKET socket{INVALID_SOCKET};
            source_kind kind{source_kind::io};
            direction dir{direction::read};
            int recv_options{};            // MSG_* for WSARecv/WSARecvFrom (read, socket kinds)
            bool in_flight{};              // driver-only: an operation is outstanding
            bool cancel_sent{};
            op_block op{};
            WSABUF wsa_buffer{};
            DWORD wsa_flags{};
            std::unique_ptr<std::byte[]> buffer;
            std::size_t buffer_size{};
            // Only meaningful for kind == datagram: WSARecvFrom fills these in.
            sockaddr_storage peer_addr{};
            INT peer_len{sizeof(sockaddr_storage)};
            std::chrono::steady_clock::time_point submitted_at{};
            // transferred bytes / completion ok / Win32 error -> what to do next.
            ::FCS::Worker::detail::callback_function<action(registration&, DWORD, bool, DWORD)> on_event;
            subscription channel_subscription;
        };

        struct command : OVERLAPPED {
            enum class kind : std::uint8_t { arm, cancel };
            kind what{};
            table::handle h{};
        };

        // Completion keys. Real I/O completions carry key 0 and are
        // recognised by their OVERLAPPED; these two never collide with it.
        static constexpr ULONG_PTR key_stop = static_cast<ULONG_PTR>(-1);
        static constexpr ULONG_PTR key_command = static_cast<ULONG_PTR>(-2);
        static constexpr ULONG batch = 64;

        [[nodiscard]] static constexpr bool same_generation(std::uint32_t a, std::uint32_t b) noexcept {
            return (a & table::generation_mask) == (b & table::generation_mask);
        }

        // --- any thread ---
        [[nodiscard]] static bool cancel_slot(void* owner, std::size_t packed) noexcept;
        [[nodiscard]] bool associate(HANDLE native) noexcept;
        [[nodiscard]] bool publish(registration* registration_ptr) noexcept;
        void post_command(command::kind what, table::handle h) noexcept;

        // --- driver only ---
        void run_loop();                                          // execution::dedicated_poller
        [[nodiscard]] static bool poll_once(void* owner) noexcept; // execution::shared_worker
        [[nodiscard]] bool handle_entry(const OVERLAPPED_ENTRY& entry) noexcept;
        void handle_command(command* cmd) noexcept;
        void handle_completion(op_block* op, DWORD transferred, LONG status) noexcept;
        void sweep_cancelled() noexcept;
        [[nodiscard]] bool arm(registration& registration_ref) noexcept;
        void request_io_cancel(registration& registration_ref) noexcept;
        void finish(registration* registration_ptr, bool deliver_pending = false) noexcept;
        void shutdown_port() noexcept;
        void discard_unarmed() noexcept;

        HANDLE port_{};
        std::thread poller_;
        std::atomic_bool polling_{};
        std::atomic_bool sweep_needed_{};  // a cancel command couldn't be allocated/posted; rescan for cancelled slots
        std::atomic_size_t pending_commands_{};
        std::atomic_int setup_error_{0};
        std::size_t poll_hook_{};

        // Driver-only.
        std::size_t inflight_{};  // operations started whose completion hasn't been reaped yet
        bool stop_seen_{};

        table slots_state_;
        std::array<std::atomic<registration*>, capacity> slots_{};
    };

    using eventlooper = backend<pool_service<>>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;

}

#include "iocp.inl"

#else

namespace FCS::Worker::backend::iocp {
    template<typename Pool = pool_service<>>
    class backend final : public generic_eventlooper<backend<Pool>, void*, Pool> {
    public:
        using generic_eventlooper<backend<Pool>, void*, Pool>::generic_eventlooper;
    };
    using eventlooper = backend<pool_service<>>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;
}

#endif
