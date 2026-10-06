// Backend drain benchmark -- isolates the per-event cost of one I/O backend.
//
// Unlike pool_stressor (adversarial, many phases, background compute), this does
// one thing: fill N socketpairs with pre-written data, then time how long the
// backend takes to deliver every byte to its callbacks, with no concurrent
// writer competing for the CPU. Because the writer is idle during the timed
// region, events/s and CPU-us/event are a clean A/B between epoll, io_uring
// and IOCP (build the same source against each backend).
//
// Usage: backend_bench [pairs=32] [rounds=20] [policy=shared|dedicated] [workers=4]
//
// Runs on POSIX and Windows. Both use the same transport -- TCP over loopback --
// so epoll/io_uring/IOCP numbers are directly comparable in shape (absolute numbers
// still differ by OS: Windows' network stack and syscalls cost more). The reader
// is generic over the source type, so the identical code runs on every backend.

#include <FCS/Worker/workers.hpp>

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <windows.h>
using sock_t = SOCKET;
#else
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <arpa/inet.h>
#  include <fcntl.h>
#  include <sys/resource.h>
#  include <sys/socket.h>
#  include <unistd.h>
using sock_t = int;
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr std::size_t chunk = 256;
struct bench_tag {};

struct chunk_reader {
    template<typename Source>
    FCS::Worker::read_result<chunk> operator()(Source& source) const {
        FCS::Worker::read_result<chunk> r{};
        r.size = source.read({r.buffer.data(), r.buffer.size()});
        return r;
    }
};

struct cpu_times { double user_s{}, sys_s{}; long vol{}, invol{}; };

cpu_times sample() {
#if defined(_WIN32)
    FILETIME created, exited, kernel, user;
    ::GetProcessTimes(::GetCurrentProcess(), &created, &exited, &kernel, &user);
    auto ft = [](const FILETIME& f) { return static_cast<double>((static_cast<std::uint64_t>(f.dwHighDateTime) << 32) | f.dwLowDateTime) * 1e-7; };
    return {ft(user), ft(kernel), 0, 0}; // context-switch counts: not exposed this cheaply on Windows
#else
    rusage ru{};
    ::getrusage(RUSAGE_SELF, &ru);
    auto tv = [](const timeval& t) { return static_cast<double>(t.tv_sec) + static_cast<double>(t.tv_usec) * 1e-6; };
    return {tv(ru.ru_utime), tv(ru.ru_stime), ru.ru_nvcsw, ru.ru_nivcsw};
#endif
}

void close_sock(sock_t s) {
#if defined(_WIN32)
    ::closesocket(s);
#else
    ::close(s);
#endif
}

// TCP over loopback on every OS (same transport everywhere). Both ends overlapped on
// Windows: IOCP can't complete I/O on a non-overlapped socket.
bool make_pair(sock_t& rx, sock_t& tx) {
#if defined(_WIN32)
    const sock_t listener = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    tx = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    const sock_t bad = INVALID_SOCKET;
    int len = sizeof(sockaddr_in);
#else
    const sock_t listener = ::socket(AF_INET, SOCK_STREAM, 0);
    tx = ::socket(AF_INET, SOCK_STREAM, 0);
    const sock_t bad = -1;
    socklen_t len = sizeof(sockaddr_in);
#endif
    if (listener == bad || tx == bad) return false;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(listener, 1) != 0) return false;
    ::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len);
    if (::connect(tx, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
    rx = ::accept(listener, nullptr, nullptr);
    close_sock(listener);
    if (rx == bad) return false;
    const int one = 1;
    ::setsockopt(tx, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), sizeof(one));
    int bufsz = 1 << 20;
    ::setsockopt(tx, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&bufsz), sizeof(bufsz));
#if !defined(_WIN32)
    // epoll/io_uring read with plain read()/recv on a blocking fd; keep it blocking-safe
    // by making the *reader* end non-blocking like the stressor does.
    ::fcntl(rx, F_SETFL, ::fcntl(rx, F_GETFL, 0) | O_NONBLOCK);
#endif
    return true;
}

} // namespace

int main(int argc, char** argv) {
    const std::size_t pairs = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 32;
    const std::size_t rounds = argc > 2 ? std::strtoul(argv[2], nullptr, 10) : 20;
    const std::string policy_name = argc > 3 ? argv[3] : "shared";
    const std::size_t workers = argc > 4 ? std::strtoul(argv[4], nullptr, 10) : 4;
    const bool dedicated = policy_name == "dedicated";
    constexpr std::size_t chunks_per_fill = 64; // per socket per round

    FCS::Worker::pool_service service;
    service.concurrency(workers).execution_policy(dedicated ? FCS::Worker::execution::dedicated_poller : FCS::Worker::execution::shared_worker).start();
    FCS::Worker::eventlooper loop{service};

    std::atomic<std::uint64_t> bytes_read{0}, events{0};
#if defined(_WIN32)
    WSADATA wsa{};
    if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return 1;
#endif
    std::vector<std::array<sock_t, 2>> fds(pairs);
    std::vector<FCS::Worker::subscription> subs;
    for (auto& p : fds) {
        if (!make_pair(p[0], p[1])) { std::fprintf(stderr, "loopback pair setup failed\n"); return 1; }
    }
    loop.start();
    for (auto& p : fds) {
        subs.push_back(loop.subscribe<bench_tag, chunk>(
            FCS::Worker::source_ref<sock_t>{p[0], FCS::Worker::source_kind::network}, chunk_reader{},
            [&](FCS::Worker::completion<chunk> c) {
                if (c.status == FCS::Worker::completion_status::ok) {
                    bytes_read.fetch_add(c.result.size, std::memory_order_relaxed);
                    events.fetch_add(1, std::memory_order_relaxed);
                }
            }));
    }

    std::vector<char> payload(chunk * chunks_per_fill, 'x');
    double drain_seconds = 0, cpu_seconds = 0;
    long vol = 0, invol = 0;
    std::uint64_t total_events = 0, total_bytes = 0;

    for (std::size_t r = 0; r < rounds; ++r) {
        // Untimed: settle, then fill every socket.
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        const auto target_bytes = bytes_read.load() + pairs * payload.size();
        const auto events_before = events.load();
        const auto cpu0 = sample();
        const auto t0 = std::chrono::steady_clock::now();
        // The fill itself is inside the window only for the send syscalls of the
        // main thread; data is consumed concurrently, which is the realistic case.
        for (auto& p : fds) {
            std::size_t off = 0;
            while (off < payload.size()) {
                const auto n = ::send(p[1], payload.data() + off, static_cast<int>(payload.size() - off), 0);
                if (n > 0) off += static_cast<std::size_t>(n); else std::this_thread::yield();
            }
        }
        while (bytes_read.load(std::memory_order_acquire) < target_bytes) std::this_thread::sleep_for(std::chrono::microseconds(200));
        const auto t1 = std::chrono::steady_clock::now();
        const auto cpu1 = sample();

        drain_seconds += std::chrono::duration<double>(t1 - t0).count();
        cpu_seconds += (cpu1.user_s + cpu1.sys_s) - (cpu0.user_s + cpu0.sys_s);
        vol += cpu1.vol - cpu0.vol;
        invol += cpu1.invol - cpu0.invol;
        total_events += events.load() - events_before;
        total_bytes += pairs * payload.size();
    }

    for (auto& s : subs) (void)s.cancel();
    loop.stop();
    std::uint64_t cache_hits = 0, scans = 0, stale = 0, hostage = 0, stalled = 0;
    for (std::size_t id = 0; id < service.worker_count(); ++id) {
        const auto p = service.worker_profile(id);
        cache_hits += p.idle_cache_hits; scans += p.idle_scans; stale += p.idle_stale_reuses; hostage += p.hostage_stolen; stalled += p.times_stalled;
    }
    for (auto& p : fds) { close_sock(p[0]); close_sock(p[1]); }

    std::printf("backend_bench policy=%s pairs=%zu rounds=%zu workers=%zu\n", policy_name.c_str(), pairs, rounds, workers);
    std::printf("  avg bytes/event: %.1f   (events/s is only comparable between runs with a similar value here: a consumer that\n"
                "                   outruns the feeder does many tiny reads, which inflates events/s without doing more work)\n",
                static_cast<double>(bytes_read.load()) / static_cast<double>(total_events ? total_events : 1));
    std::printf("  events        : %llu (%.0f events/s)\n", static_cast<unsigned long long>(total_events), static_cast<double>(total_events) / drain_seconds);
    std::printf("  throughput    : %.1f MB/s\n", static_cast<double>(total_bytes) / drain_seconds / 1e6);
    std::printf("  cpu per event : %.2f us (user+sys, whole process)\n", cpu_seconds / static_cast<double>(total_events) * 1e6);
#if !defined(_WIN32)
    std::printf("  ctx switches  : %ld voluntary, %ld involuntary (%.3f per event)\n", vol, invol, static_cast<double>(vol + invol) / static_cast<double>(total_events));
#endif
    std::printf("  steal estimate: %llu lookups (%llu cached, %llu scans, %llu stale-reuse); watchdog: %llu stalls, %llu hostage items\n",
                static_cast<unsigned long long>(cache_hits + scans + stale), static_cast<unsigned long long>(cache_hits),
                static_cast<unsigned long long>(scans), static_cast<unsigned long long>(stale),
                static_cast<unsigned long long>(stalled), static_cast<unsigned long long>(hostage));
    return 0;
}
