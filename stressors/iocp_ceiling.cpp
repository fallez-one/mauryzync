// Raw Winsock/IOCP ceiling: how many small completed WSARecv's per second can this
// machine do with NO thread pool, NO std::function, NO channel -- just
//   WSARecv -> GetQueuedCompletionStatusEx -> WSARecv ...
// That number is the upper bound any IOCP-based backend can reach on this box; compare
// FCS's IOCP backend against it (fcs_workers_backend_bench uses the same TCP loopback
// pairs on Windows) instead of against epoll on a different OS.
//
// Usage: iocp_ceiling [pairs=32] [seconds=5] [skip=0|1] [chunk=256] [mode=recv|send0]
//   mode=recv  (default) completed WSARecv's against always-stocked sockets: the read path.
//   mode=send0 zero-byte overlapped WSASend's: exactly what the FCS IOCP backend uses as
//              its "socket is writable" signal for register_sink(). Always completes
//              immediately, so it shows what that path costs with and without skip.
//   skip=1 sets FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: a WSARecv that completes
//   immediately (data was already waiting) is handled inline instead of also
//   round-tripping a packet through the port. Run both and compare -- that delta is
//   what a skip-on-success IOCP backend could gain on this machine.
//
// Windows only (builds with MSVC and MinGW).
#ifndef _WIN32
#include <cstdio>
#include <cstring>
int main() { std::puts("iocp_ceiling is Windows-only"); return 0; }
#else

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#include <windows.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

#ifndef FILE_SKIP_COMPLETION_PORT_ON_SUCCESS
#define FILE_SKIP_COMPLETION_PORT_ON_SUCCESS 0x1
#endif
#ifndef FILE_SKIP_SET_EVENT_ON_HANDLE
#define FILE_SKIP_SET_EVENT_ON_HANDLE 0x2
#endif

namespace {

struct conn {
    OVERLAPPED ov{};          // first member: OVERLAPPED* == conn*
    SOCKET rx{INVALID_SOCKET};
    SOCKET tx{INVALID_SOCKET};
    std::unique_ptr<char[]> buf;
    WSABUF wsabuf{};
};

bool make_pair(conn& c, std::size_t chunk) {
    SOCKET listener = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    c.tx = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    if (listener == INVALID_SOCKET || c.tx == INVALID_SOCKET) return false;
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(listener, 1) != 0) return false;
    int len = sizeof(addr);
    ::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len);
    if (::connect(c.tx, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
    c.rx = ::accept(listener, nullptr, nullptr);
    ::closesocket(listener);
    if (c.rx == INVALID_SOCKET) return false;
    BOOL one = TRUE;
    ::setsockopt(c.tx, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), sizeof(one));
    // Non-blocking sender: the feeder must never park inside send() on a full buffer,
    // or it can't notice `stop` once the reader quits draining (and join() hangs).
    u_long nonblocking = 1;
    ::ioctlsocket(c.tx, static_cast<long>(FIONBIO), &nonblocking);
    c.buf = std::make_unique<char[]>(chunk);
    c.wsabuf.buf = c.buf.get();
    c.wsabuf.len = static_cast<ULONG>(chunk);
    return true;
}

// Returns true if the operation completed synchronously (only reported when skip mode is on).
// false: pending (a packet will arrive) -- or failed, in which case `failed` is set.
bool send0_mode = false;

bool arm(conn& c, bool& failed) {
    failed = false;
    std::memset(&c.ov, 0, sizeof(c.ov));
    DWORD flags = 0, got = 0;
    WSABUF empty{0, nullptr};
    const int rc = send0_mode ? ::WSASend(c.rx, &empty, 1, &got, 0, &c.ov, nullptr)
                              : ::WSARecv(c.rx, &c.wsabuf, 1, &got, &flags, &c.ov, nullptr);
    if (rc == 0) return true;
    if (::WSAGetLastError() == WSA_IO_PENDING) return false;
    failed = true;
    return false;
}

} // namespace

int main(int argc, char** argv) {
    const std::size_t pairs = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 32;
    const double seconds = argc > 2 ? std::atof(argv[2]) : 5.0;
    const bool skip = argc > 3 && std::atoi(argv[3]) != 0;
    const std::size_t chunk = argc > 4 ? std::strtoul(argv[4], nullptr, 10) : 256;
    send0_mode = argc > 5 && std::strcmp(argv[5], "send0") == 0;

    WSADATA wsa{};
    if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return 2;
    HANDLE port = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 1);

    std::vector<std::unique_ptr<conn>> conns;
    for (std::size_t i = 0; i < pairs; ++i) {
        auto c = std::make_unique<conn>();
        if (!make_pair(*c, chunk)) { std::printf("socket setup failed (%d)\n", ::WSAGetLastError()); return 1; }
        ::CreateIoCompletionPort(reinterpret_cast<HANDLE>(c->rx), port, 0, 0);
        if (skip && !::SetFileCompletionNotificationModes(reinterpret_cast<HANDLE>(c->rx),
                                                          FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE)) {
            std::printf("SetFileCompletionNotificationModes failed (%lu): this provider can't skip; run with skip=0\n", ::GetLastError());
            return 1;
        }
        conns.push_back(std::move(c));
    }

    // Feeder keeps every socket stocked so a recv always finds data waiting (the
    // case that matters: what does an *immediately satisfiable* read cost?).
    std::atomic<bool> stop{false};
    std::thread feeder([&] {
        if (send0_mode) return; // zero-byte sends need no data
        std::vector<char> payload(chunk * 16, 'x');
        while (!stop.load(std::memory_order_relaxed)) {
            bool sent_any = false;
            for (auto& c : conns) {
                // WSAEWOULDBLOCK (buffer full) is the normal steady state, not an error.
                if (::send(c->tx, payload.data(), static_cast<int>(payload.size()), 0) > 0) sent_any = true;
            }
            if (!sent_any) ::Sleep(1); // everything is full: let the reader drain
            else ::Sleep(0);
        }
    });

    std::uint64_t ops = 0, inline_ops = 0, packets = 0;
    constexpr std::size_t inline_cap = 16; // fairness: at most this many back-to-back inline completions per conn per packet
    std::vector<OVERLAPPED_ENTRY> entries(64);

    constexpr ULONG_PTR key_continue = 1; // our own "keep draining this connection" packet

    // Re-arm `c`. Without skip every WSARecv yields exactly one packet, so a single arm
    // suffices. With skip, a WSARecv that completes immediately produces NO packet, so
    // it is consumed here and re-armed until one goes pending (a packet will then come);
    // after `inline_cap` in a row the connection is handed back through a manual
    // packet so one busy socket can't starve the others.
    auto pump = [&](conn& c) {
        bool failed = false;
        for (std::size_t n = 0; n < inline_cap; ++n) {
            const bool completed_inline = arm(c, failed);
            if (failed) return;
            if (!(skip && completed_inline)) return; // pending (packet coming) or no-skip (packet coming anyway)
            ++ops; ++inline_ops;
        }
        ::PostQueuedCompletionStatus(port, 0, key_continue, &c.ov);
    };

    for (auto& c : conns) pump(*c);

    const auto t0 = std::chrono::steady_clock::now();
    const auto end = t0 + std::chrono::duration<double>(seconds);
    while (std::chrono::steady_clock::now() < end) {
        ULONG count = 0;
        if (!::GetQueuedCompletionStatusEx(port, entries.data(), static_cast<ULONG>(entries.size()), &count, 50, FALSE)) continue;
        for (ULONG i = 0; i < count; ++i) {
            if (!entries[i].lpOverlapped) continue;
            ++packets;
            if (entries[i].lpCompletionKey != key_continue) ++ops; // a real recv completion; continuation packets are bookkeeping
            pump(*reinterpret_cast<conn*>(entries[i].lpOverlapped));
        }
    }
    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    stop = true;
    feeder.join();

    std::printf("iocp_ceiling pairs=%zu chunk=%zu skip_on_success=%d mode=%s\n", pairs, chunk, skip ? 1 : 0, send0_mode ? "send0" : "recv");
    std::printf("  %s completions/s : %.0f\n", send0_mode ? "send0" : "recv", static_cast<double>(ops) / elapsed);
    std::printf("  of which inline    : %.1f%%  (packets through the port: %llu)\n",
                ops ? 100.0 * static_cast<double>(inline_ops) / static_cast<double>(ops) : 0.0, static_cast<unsigned long long>(packets));
    // Deliberately no teardown of in-flight overlapped I/O: the process exits.
    return 0;
}
#endif
