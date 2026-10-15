/**
    * Copyright (c) 2026, Fallen One
    * SPDX-License-Identifier: BSD-3-Clause
    * See the LICENSE file in the project root for license information.
    * Actual implementation starts at line 317. Main starts at line 344.
    * Refer to windows_socket_integration.cpp and unix_socket_integration.cpp for full context.
    * For knobs, it uses basic knobs plus extras, for reference on basic knobs, see basic_knobs.cpp.

    Exploring FCS-Workers on runtime swap without recompilation / redeployment.
    This example uses socket to set up a server. Real world may not look like this.
*/

#include <FCS/Worker/workers.hpp>
#include <array>
#include <atomic>
#include <cstddef>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <iostream>
 
#ifndef _WIN32
#include <cerrno>
#endif
 
#include <system_error>
 
#ifdef _WIN32
#include <winsock2.h>
#include <WS2tcpip.h>
#   ifdef _MSC_VER
#       pragma comment(lib, "ws2_32.lib")
#   endif
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <unistd.h>
#endif
 
#include <chrono>
 
using namespace std::chrono_literals;
namespace worker = FCS::Worker;
 
using aging_policy = worker::detail::aging_granularity;
 
template<std::size_t Size>
struct basic_recv {
    template<typename Source>
    [[nodiscard]] worker::read_result<Size> operator()(Source& source) const {
        worker::read_result<Size> result{};
        result.size = source.read(std::span<std::byte>(result.buffer.data(), result.buffer.size()));
        return result;
    }
};

struct on_read{};
 
static constexpr size_t profile_io_threads = 2;
static constexpr size_t profile_compute_threads = 4;
static constexpr size_t swapped_compute_threads = 2;
static constexpr size_t max_buffer_read = 1024;
static constexpr std::chrono::nanoseconds disable_watchdog = 0ns;
static constexpr std::chrono::nanoseconds worse_io_latency = 1s;
 
#ifdef _WIN32
struct wsa_wrap {
    static int acquire_runtime() noexcept {
        WSADATA data;
        if(WSAStartup(MAKEWORD(2, 2), &data) == 0) return 0;
        return WSAGetLastError();
    }
    static void release_runtime() noexcept {
        static_cast<void>(WSACleanup());
    }
};
 
class windows_tcp {
    SOCKET sockfd = INVALID_SOCKET;
    int error;
    sockaddr_in addr{};
    std::atomic_bool keep_running{true};
    bool owns_runtime{false};
public:
    windows_tcp () noexcept {
        error = wsa_wrap::acquire_runtime();
        if(error != 0) return;
        owns_runtime = (error == 0);
        sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if(sockfd == INVALID_SOCKET) {
            error = WSAGetLastError();
            return;
        }
    }
    static FCS::Expected<windows_tcp, int> try_create() noexcept {
        windows_tcp p{};
        if(p.error != 0) {
            if(p.owns_runtime) {
                wsa_wrap::release_runtime();
            }
            return FCS::Unexpected{p.error};
        }
        return p;
    };
 
    ~windows_tcp() noexcept {
        if(sockfd != INVALID_SOCKET) {
            closesocket(sockfd);
        }
        if(owns_runtime) {
            wsa_wrap::release_runtime();
        }
    }
 
    windows_tcp(const windows_tcp &other) noexcept = delete;
    windows_tcp &operator=(const windows_tcp &other) noexcept = delete;
 
    windows_tcp(windows_tcp &&other) :
    sockfd(std::exchange(other.sockfd, INVALID_SOCKET)), error(std::exchange(other.error, 0)),
    addr(std::exchange(other.addr, {})), keep_running(other.keep_running.exchange(false, std::memory_order_relaxed)),
    owns_runtime(std::exchange(other.owns_runtime, false)) {}
 
    windows_tcp &operator=(windows_tcp &&other) noexcept {
        if(this != &other) {
            if(sockfd != INVALID_SOCKET) {
                closesocket(sockfd);
            }
            if(owns_runtime) {
                wsa_wrap::release_runtime();
            }
            sockfd = std::exchange(other.sockfd, INVALID_SOCKET);
            error = std::exchange(other.error, 0);
            addr = std::exchange(other.addr, {});
            keep_running = other.keep_running.exchange(false, std::memory_order_relaxed);
            owns_runtime = std::exchange(other.owns_runtime, false);
        }
        return *this;
    }

    bool bind(const char *address="127.0.0.1", uint16_t port=9090) {
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        int err = inet_pton(AF_INET, address, &addr.sin_addr);
        if(err <= 0) {
            if(err == 0) {
                std::cerr << "Invalid address.\n";
                return false;
            }
            std::cerr << "inet_pton: " << std::system_category().message(WSAGetLastError()) << '\n';
            return false;
        }
        const int reuse = 1;
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
        const bool success = ::bind(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
        if(!success) {
            std::cerr << "bind: " << std::system_category().message(WSAGetLastError()) << '\n';
        }
        return success;
    }
    bool listen(int backlog=5) const noexcept {
        if(::listen(sockfd, backlog) < 0) {
            std::cerr << "listen: " << std::system_category().message(WSAGetLastError()) << '\n';
            return false;
        }
        return true;
    }

    SOCKET accept_one(std::chrono::milliseconds timeout) noexcept {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while(keep_running.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline) {
            WSAPOLLFD waiting{sockfd, POLLRDNORM, 0};
            const int ready = WSAPoll(&waiting, 1, 200);
            if(ready < 0) {
                int error = WSAGetLastError();
                if(error == WSAEINTR) continue;
                std::cerr << "poll: " << std::system_category().message(error) << '\n';
                return INVALID_SOCKET;
            }
            if(ready == 0) continue;
            const SOCKET accepted = accept(sockfd, nullptr, nullptr);
            if(accepted != INVALID_SOCKET) return accepted;
            int error_accept = WSAGetLastError();
            if(error_accept != WSAEWOULDBLOCK && error_accept != WSAEINTR && error_accept != WSAECONNABORTED) {
                std::cerr << "accept: " << std::system_category().message(error_accept) << '\n';
                return INVALID_SOCKET;
            }
        }
        return INVALID_SOCKET;
    }

    void stop() noexcept {
        keep_running.store(false, std::memory_order_relaxed);
    }

    SOCKET handle() const noexcept {
        return sockfd;
    }

    static int close_fd(SOCKET sock) noexcept {
        return closesocket(sock);
    }
};
using socket_wrapper = windows_tcp;
using native_socket = SOCKET;
static constexpr native_socket invalid_socket = INVALID_SOCKET;
#else
class unix_tcp {
    int sockfd = -1;
    size_t addr_len = 0;
    sockaddr_in addr{};
    std::atomic_bool keep_running{true};
public:
    unix_tcp() {
        sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    }
    static FCS::Expected<unix_tcp, int> try_create() noexcept {
        unix_tcp p{};
        if(p.sockfd < 0) {
            return FCS::Unexpected{errno};
        }
        return p;
    }
    ~unix_tcp() {
        close(sockfd);
    }
 
    unix_tcp(const unix_tcp &other) noexcept = delete;
    unix_tcp& operator=(const unix_tcp &other) noexcept = delete;
 
    unix_tcp(unix_tcp &&other) :
    sockfd(std::exchange(other.sockfd, -1)), addr_len(std::exchange(other.addr_len, 0)),
    addr(std::exchange(other.addr, {})), keep_running(other.keep_running.exchange(false, std::memory_order_relaxed)) {}
    unix_tcp &operator=(unix_tcp &&other) {
        if(this != &other) {
            if(sockfd >= 0) {
                close(sockfd);
            }
            sockfd = std::exchange(other.sockfd, -1);
            addr_len = std::exchange(other.addr_len, 0);
        }
        return *this;
    }
 
    bool bind(const char *address="127.0.0.1", uint16_t port=9090) {
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        int pton = inet_pton(AF_INET, address, &addr.sin_addr);
        if(pton <= 0) {
            if(pton == 0) {
                std::cerr << "Invalid IP Address.\n";
                return false;
            }
            perror("inet_pton");
            return false;
        }
        const int reuse = 1;
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        const bool success = ::bind(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
        if(!success) {
            perror("bind");
        }
        return success;
    }
 
    bool listen(int backlog=5) const noexcept {
        if(::listen(sockfd, backlog) < 0) {
            perror("listen");
            return false;
        }
        return true;
    }
    int accept_one(std::chrono::milliseconds timeout) noexcept {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while(keep_running.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline) {
            pollfd waiting{sockfd, POLLIN, 0};
            const int ready = poll(&waiting, 1, 200);
            if(ready < 0) {
                if(errno == EINTR) continue;
                perror("poll");
                return -1;
            }
            if(ready == 0) continue;
            const int accepted = accept4(sockfd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if(accepted >= 0) return accepted;
            if(errno != EAGAIN && errno != EINTR && errno != ECONNABORTED) {
                perror("accept4");
                return -1;
            }
        }
        return -1;
    }
    void stop() noexcept {
        keep_running.store(false, std::memory_order_relaxed);
    }
    int handle() const noexcept {
        return sockfd;
    }
    static int close_fd(int sock) noexcept {
        return close(sock);
    }
};
using socket_wrapper = unix_tcp;
using native_socket = int;
static constexpr native_socket invalid_socket = -1;
#endif
 
 
struct profiles {
    static worker::pool_service<> io_pool; // configure capacity if you need higher backpressure
    static worker::pool_service<128> compute_pool; // 128 capacity for example
};
worker::pool_service<> profiles::io_pool;
worker::pool_service<128> profiles::compute_pool;
 
static std::atomic_bool connection_done{false};
static std::atomic<int> compute_pending{0};
 
static void on_completion(worker::completion<max_buffer_read> c) {
    if(c.status != worker::completion_status::ok) {
        connection_done.store(true, std::memory_order_release); // closed or error
        connection_done.notify_all();
        return;
    }
    compute_pending.fetch_add(1, std::memory_order_relaxed);
    const bool accepted = profiles::compute_pool.enqueue(
        [](std::string text) {
            std::cout << "[compute] " << text;
            compute_pending.fetch_sub(1, std::memory_order_relaxed);
        },
        std::string(reinterpret_cast<const char*>(c.result.buffer.data()), c.result.size));
    if(!accepted) compute_pending.fetch_sub(1, std::memory_order_relaxed);
}

static void swap_compute_profile(std::size_t threads) {
    // Drain first so the swap can't strand queued work.
    while(compute_pending.load(std::memory_order_relaxed) != 0) std::this_thread::yield();
    profiles::compute_pool.stop();
    profiles::compute_pool
        .concurrency(threads)
        .execution_policy(worker::execution::shared_worker, worker::execution::task_type::segment, threads);
    profiles::compute_pool.start();
}

int main() {
    auto sock_expect = socket_wrapper::try_create();
    if(!sock_expect) {
        std::string error = std::system_category().message(sock_expect.error());
        std::cerr << "Error creating socket: " << error << '\n';
        return 1;
    }
    socket_wrapper sock = std::move(*sock_expect);
    if(!sock.bind()) {
        return 1;
    }
    if(!sock.listen()) {
        return 1;
    }
    profiles prof;
    prof.io_pool
        .concurrency(profile_io_threads)
        .execution_policy(worker::execution::dedicated_poller)
        .burst_backpressure()
        .watchdog_interval(disable_watchdog);
    
    prof.compute_pool
        .concurrency(profile_compute_threads)
        .execution_policy(worker::execution::shared_worker, worker::execution::task_type::segment, 4)
        .timeout_tolerate(worse_io_latency) /* Longer = better sleep, bad I/O latency. Shorter = CPU burn (spin), responsive I/O latency. */
        .age_starvation(aging_policy::segment, 3, 1, 3)
        /*
            normal work anti-starvation mechanism, segment or task?
            segment = per N segments popped from worker's queue
            individual = per N tasks popped from worker's queue.
            Affects how tasks_count is counted.
 
            tasks_count: How many tasks to tolerate before aging takes effect.
            base_growth: By how much to grow?
            max: Cap before a work is promoted.
        */
        ;
    prof.io_pool.start();
    prof.compute_pool.start();
    worker::eventlooper loop{prof.io_pool};
    loop.start();
 
    std::vector<native_socket> accepted;
    for(std::size_t round = 0; round < 2; ++round) {
        std::cout << "Round " << round + 1 << ": waiting for a client on 127.0.0.1:9090 (30s)\n";
        const auto conn = sock.accept_one(30s);
        if(conn == invalid_socket) break;
        accepted.push_back(conn);
 
        connection_done.store(false, std::memory_order_relaxed);
        auto sub = loop.subscribe<on_read, max_buffer_read>(
            worker::source_ref<decltype(conn)>{.native = conn, .kind = worker::source_kind::network},
            basic_recv<max_buffer_read>{},
            [](worker::completion<max_buffer_read> c) { on_completion(c); });
        while(!connection_done.load(std::memory_order_acquire)) connection_done.wait(false);
        (void)sub.cancel();
 
        if(round == 0) {
            std::cout << "Client done. Swapping compute profile: " << profile_compute_threads
                      << " -> " << swapped_compute_threads << " threads\n";
            swap_compute_profile(swapped_compute_threads);
        }
    }
 
    loop.stop();
    // Close after stop(): cancel() is async; fd reuse would race it.
    for(auto fd : accepted) socket_wrapper::close_fd(fd);
    prof.compute_pool.stop();
    return 0;
}