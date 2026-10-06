#pragma once

#include "../../types.hpp"

#include <cstddef>
#include <cstring>
#include <optional>
#include <span>
#include <system_error>

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace FCS::Worker::backend::detail {

    // Maps a platform error into a portable std::error_code, leaving the exact
    // native value available separately (completion::raw_status) for anyone
    // who needs it.
#if defined(_WIN32)
    [[nodiscard]] inline std::error_code from_native_error(DWORD code) noexcept {
        return {static_cast<int>(code), std::system_category()};
    }
#else
    [[nodiscard]] inline std::error_code from_native_error(int code) noexcept {
        return {code, std::generic_category()};
    }
#endif

    // Every Source type (readiness_source below, completed_source) offers
    // the same two-method surface: read() and sender(). sender() is
    // populated only when the registration's source_kind is `datagram`;
    // everything else (network, io, synthetic) always answers nullopt,
    // which is the sane default a Reader gets by just not asking. A
    // Reader that does need it (basic_recvfrom-style) calls source.read()
    // then source.sender() -- no backend-specific branching, no
    // native_handle()/raw recvfrom() of its own required.

    // Recv-side capabilities, XOR/OR-able bit flags rather than a type list --
    // combine with |, e.g. recv_option::peek | recv_option::wait_all.
    enum class recv_option : unsigned {
        none        = 0,
        peek        = 1u << 0, // MSG_PEEK
        wait_all    = 1u << 1, // MSG_WAITALL
        out_of_band = 1u << 2, // MSG_OOB
        truncate    = 1u << 3, // MSG_TRUNC
    };
    constexpr recv_option operator|(recv_option a, recv_option b) noexcept { return static_cast<recv_option>(static_cast<unsigned>(a) | static_cast<unsigned>(b)); }
    constexpr recv_option operator&(recv_option a, recv_option b) noexcept { return static_cast<recv_option>(static_cast<unsigned>(a) & static_cast<unsigned>(b)); }
    [[nodiscard]] constexpr bool has_recv_option(recv_option set, recv_option flag) noexcept { return (set & flag) != recv_option::none; }

    // Maps recv_option onto MSG_* flags for a socket read (kind() ==
    // network/datagram). A plain fd/HANDLE (kind() == io) has no flags concept
    // at all, so the read ignores Options entirely regardless of what's set --
    // that's the "idempotent on unsupported" behavior: no error, no effect.
    // Bits a platform has no equivalent for (MSG_TRUNC on Windows) are dropped.
    [[nodiscard]] constexpr int to_native_recv_flags(recv_option options) noexcept {
        int flags = 0;
        if (has_recv_option(options, recv_option::peek)) flags |= MSG_PEEK;
#if defined(MSG_WAITALL)
        if (has_recv_option(options, recv_option::wait_all)) flags |= MSG_WAITALL;
#endif
        if (has_recv_option(options, recv_option::out_of_band)) flags |= MSG_OOB;
#if !defined(_WIN32)
        if (has_recv_option(options, recv_option::truncate)) flags |= MSG_TRUNC;
#endif
        return flags;
    }

#if !defined(_WIN32)
    // Presented to Reader by epoll/kqueue: readiness only, so read() performs
    // the real synchronous syscall right when Reader calls it -- recvfrom for
    // datagram sources (capturing the sender), recv for other sockets, read
    // for everything else. Tracks the last error itself so a caller can
    // distinguish "0 bytes because EOF" from "0 bytes because the call
    // failed" without inspecting errno separately.
    //
    // `Options`: recv_option flags applied on network/datagram sources only;
    // an io-kind source (plain read()) ignores them, and any bit outside the
    // four defined above is simply never set on the native flags word --
    // both are no-ops rather than errors, satisfying "idempotent on
    // unsupported" without a runtime branch per call.
    template<recv_option Options = recv_option::none>
    class readiness_source {
    public:
        readiness_source(int fd, source_kind kind) noexcept : fd_(fd), kind_(kind) {}

        std::size_t read(std::span<std::byte> into) noexcept {
            errno = 0;
            ssize_t n;
            static constexpr int flags = to_native_recv_flags(Options);
            if (kind_ == source_kind::datagram) {
                sockaddr_storage addr{};
                socklen_t addr_len = sizeof(addr);
                n = ::recvfrom(fd_, into.data(), into.size(), flags, reinterpret_cast<sockaddr*>(&addr), &addr_len);
                if (n >= 0) { sender_ = addr; have_sender_ = true; }
            } else if (kind_ == source_kind::network) {
                n = ::recv(fd_, into.data(), into.size(), flags);
            } else {
                n = ::read(fd_, into.data(), into.size());
            }
            if (n < 0) { error_ = errno; return 0; }
            error_ = 0;
            return static_cast<std::size_t>(n);
        }

        // The sender captured by the most recent read(), if kind() is
        // `datagram` and that read succeeded; nullopt otherwise.
        [[nodiscard]] std::optional<sockaddr_storage> sender() const noexcept {
            if (!have_sender_) return std::nullopt;
            return sender_;
        }

        [[nodiscard]] source_kind kind() const noexcept { return kind_; }
        [[nodiscard]] bool failed() const noexcept { return error_ != 0; }
        [[nodiscard]] int raw_error() const noexcept { return error_; }

        // Escape hatch for a Reader that needs the fd for something read()
        // and sender() don't cover. Only meaningful on this readiness-based
        // path -- io_uring/IOCP's completed_source hands Reader
        // already-received bytes with no live fd behind them by that point.
        [[nodiscard]] int native_handle() const noexcept { return fd_; }

    private:
        int fd_;
        source_kind kind_;
        int error_{};
        sockaddr_storage sender_{};
        bool have_sender_{};
    };

    // Write-side counterpart of readiness_source: write() performs the real
    // syscall on call -- sendto for a datagram sink with target() set, send
    // for other sockets, write otherwise. Tracks the last error itself.
    class readiness_sink {
    public:
        readiness_sink(int fd, source_kind kind) noexcept : fd_(fd), kind_(kind) {}

        std::size_t write(std::span<const std::byte> data) noexcept {
            errno = 0;
            ssize_t n;
            if (kind_ == source_kind::datagram && have_target_) {
                n = ::sendto(fd_, data.data(), data.size(), 0, reinterpret_cast<const sockaddr*>(&target_), target_len_);
            } else if (kind_ == source_kind::network || kind_ == source_kind::datagram) {
                n = ::send(fd_, data.data(), data.size(), 0);
            } else {
                n = ::write(fd_, data.data(), data.size());
            }
            if (n < 0) { error_ = errno; return 0; }
            error_ = 0;
            return static_cast<std::size_t>(n);
        }

        // Destination for the next write(), on a datagram sink only --
        // network/io sinks ignore it (send()/write() have no such notion).
        void target(const sockaddr_storage& addr, socklen_t len) noexcept { target_ = addr; target_len_ = len; have_target_ = true; }

        [[nodiscard]] source_kind kind() const noexcept { return kind_; }
        [[nodiscard]] bool failed() const noexcept { return error_ != 0; }
        [[nodiscard]] int raw_error() const noexcept { return error_; }

        // Same escape hatch as readiness_source::native_handle() -- only
        // meaningful on this readiness-based path.
        [[nodiscard]] int native_handle() const noexcept { return fd_; }

    private:
        int fd_;
        source_kind kind_;
        int error_{};
        sockaddr_storage target_{};
        socklen_t target_len_{};
        bool have_target_{};
    };
#endif

#if defined(_WIN32)
    // Windows counterpart of the POSIX readiness_sink above, for the IOCP
    // backend's write registrations: the backend only learns "this socket can
    // take a send" (a zero-byte overlapped WSASend completing), and the Writer
    // then performs the real, synchronous send() right when it calls write().
    // Sockets only -- a plain file/pipe HANDLE has no such readiness signal.
    class readiness_sink {
    public:
        readiness_sink(SOCKET socket, source_kind kind) noexcept : socket_(socket), kind_(kind) {}

        std::size_t write(std::span<const std::byte> data) noexcept {
            error_ = 0;
            int n;
            const int len = static_cast<int>(data.size() > 0x7FFFFFFFu ? 0x7FFFFFFFu : data.size());
            if (kind_ == source_kind::datagram && have_target_) {
                n = ::sendto(socket_, reinterpret_cast<const char*>(data.data()), len, 0, reinterpret_cast<const sockaddr*>(&target_), target_len_);
            } else {
                n = ::send(socket_, reinterpret_cast<const char*>(data.data()), len, 0);
            }
            if (n == SOCKET_ERROR) { error_ = ::WSAGetLastError(); return 0; }
            return static_cast<std::size_t>(n);
        }

        // Destination for the next write(), on a datagram sink only.
        void target(const sockaddr_storage& addr, int len) noexcept { target_ = addr; target_len_ = len; have_target_ = true; }

        [[nodiscard]] source_kind kind() const noexcept { return kind_; }
        [[nodiscard]] bool failed() const noexcept { return error_ != 0; }
        [[nodiscard]] int raw_error() const noexcept { return error_; }
        [[nodiscard]] SOCKET native_handle() const noexcept { return socket_; }

    private:
        SOCKET socket_;
        source_kind kind_;
        int error_{};
        sockaddr_storage target_{};
        int target_len_{};
        bool have_target_{};
    };
#endif

    // Presented to Reader by io_uring/IOCP: the OS already delivered the
    // bytes into a buffer the registration owns by the time Reader runs, so
    // read() just hands them back -- a bounded copy into the caller's buffer,
    // no syscall. Reader is still always invoked; this is only ever
    // constructed and called on the success path (nothing meaningful to hand
    // back on a failed completion -- the backend reports that via
    // completion::status/error directly instead of calling Reader at all).
    //
    // `sender`: non-null only for a datagram-kind registration whose
    // backend actually captured one (IORING_OP_RECVMSG / WSARecvFrom, not
    // the plain read/WSARecv used for everything else) -- see io_uring.inl
    // and iocp.inl. Copied once at construction; sender() then just hands
    // back that copy, same shape as readiness_source's.
    class completed_source {
    public:
        completed_source(const std::byte* data, std::size_t size, const sockaddr_storage* sender = nullptr) noexcept
            : data_(data), size_(size) {
            if (sender) { sender_ = *sender; have_sender_ = true; }
        }

        std::size_t read(std::span<std::byte> into) const noexcept {
            const auto n = size_ < into.size() ? size_ : into.size();
            if (n) std::memcpy(into.data(), data_, n);
            return n;
        }

        [[nodiscard]] std::optional<sockaddr_storage> sender() const noexcept {
            if (!have_sender_) return std::nullopt;
            return sender_;
        }

        [[nodiscard]] std::size_t available() const noexcept { return size_; }

    private:
        const std::byte* data_;
        std::size_t size_;
        sockaddr_storage sender_{};
        bool have_sender_{};
    };

    // Write-side counterpart of completed_source: the backend already
    // submitted the buffer, so write() just hands back the byte count the
    // OS already reported. Reserved for io_uring/IOCP's write-completion
    // path (not yet wired up); shaped like readiness_sink so one Writer
    // works against either.
    class completed_sink {
    public:
        explicit completed_sink(std::size_t bytes_written) noexcept : bytes_written_(bytes_written) {}

        std::size_t write(std::span<const std::byte>) const noexcept { return bytes_written_; }

        [[nodiscard]] std::size_t bytes_written() const noexcept { return bytes_written_; }

    private:
        std::size_t bytes_written_;
    };

}
