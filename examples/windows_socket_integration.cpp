/**
    * Copyright (c) 2026, Fallen One
    * SPDX-License-Identifier: BSD-3-Clause
    * See the LICENSE file in the project root for license information.
    * For knobs, see basic_knobs.cpp
    * Assumptions: basic knowledge of Winsock2 to set up server.
    * Main starts at line 210.

    Exploring on how to bind Windows socket to FCS-Workers.
    Compiled using:
    cl /I".\include" /permissive- /utf-8 /Zc:preprocessor /Zc:__cplusplus /MP /O2 /DFCS_WORKER_BACKEND_IOCP=1 /DNOMINMAX /std:c++20 /MD examples/windows_socket_integration.cpp
*/

#include "FCS/Worker/execution.hpp"
#include "FCS/Worker/pool_service.hpp"
#include <FCS/Worker/workers.hpp>

#include <system_error>
#include <iostream>

#ifdef _WIN32
#include <winsock2.h>
#include <WS2tcpip.h>

#   ifdef _MSC_VER
#       pragma comment(lib, "ws2_32.lib")
#   endif
 
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
 
static constexpr size_t io_threads = 2;
static constexpr size_t max_buffer_read = 1024;
static constexpr std::chrono::nanoseconds disable_watchdog = 0ns;
 
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
            // Use poll to handle on our side. While the IOCP part is handled by FCS-Workers.
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

            // Client side error or actual server error?
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
#endif

static std::atomic_bool connection_done{false};

static worker::pool_service<> io_pool;

// FCS-Workers model is completion-based, so we need to provide a type to read the source.
static void on_completion(worker::completion<max_buffer_read> c) {
    if(c.status != worker::completion_status::ok) {
        connection_done.store(true, std::memory_order_release); // closed or error
        connection_done.notify_all();
        return;
    }
    std::cout << std::string(reinterpret_cast<const char*>(c.result.buffer.data()), c.result.size);
}

int main(void) {
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
    // Why need a eventlooper? Didn't FCS-Workers provide .subscribe already?
    // subscribe is using general assumptions and did not provide I/O operation.
    // eventlooper in FCS-Workers is not an event loop + thread pool, but the other way around.
    // eventlooper provides what we need for reading (recv)
    worker::eventlooper looper{io_pool};
    io_pool
        .concurrency(io_threads)
        .execution_policy(worker::execution::dedicated_poller)
        .burst_backpressure()
        .watchdog_interval(disable_watchdog);
    io_pool.start();
    looper.start();

        std::vector<native_socket> accepted;
    for(std::size_t round = 0; round < 2; ++round) {
        std::cout << "Round " << round + 1 << ": waiting for a client on 127.0.0.1:9090 (30s)\n";
        const auto conn = sock.accept_one(30s);
        if(conn == invalid_socket) break;
        accepted.push_back(conn);
 
        connection_done.store(false, std::memory_order_relaxed);
        auto sub = looper.subscribe<on_read, max_buffer_read>(
            worker::source_ref<decltype(conn)>{.native = conn, .kind = worker::source_kind::network},
            basic_recv<max_buffer_read>{},
            [](worker::completion<max_buffer_read> c) { on_completion(c); });
        while(!connection_done.load(std::memory_order_acquire)) connection_done.wait(false);
        (void)sub.cancel(); // cancel subscription, or let RAII handle it.
    }

    looper.stop();
    io_pool.stop();
    return 0;
}
#else
int main() {
    std::cout << "Windows only." << std::endl;
    return 0;
}
#endif