/**
    * Copyright (c) 2026, Fallen One
    * SPDX-License-Identifier: BSD-3-Clause
    * See the LICENSE file in the project root for license information.
    * Assumptions: basic knowledge of Unix-like socket to set up server.
    * Main starts at line 158.

    Exploring on how to bind Unix/Unix-like socket to FCS-Workers.
    Compiled using: clang++ -DFCS_WORKER_BACKEND_EPOLL -DNDEBUG -O3 -Iinclude -std=c++20 examples/unix_socket_integration.cpp -o unix_socket
*/


#include <FCS/Worker/workers.hpp>
#include <iostream>
#include <chrono>

namespace worker = FCS::Worker;
using namespace std::chrono_literals;
#ifndef _WIN32

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <unistd.h>

// FCS-Workers I/O model is completion-based with a unified Source template.
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

static std::atomic_bool connection_done{false};

static constexpr size_t io_threads = 2;
static constexpr std::chrono::nanoseconds disable_watchdog = 0ns;
static constexpr size_t max_buffer_read = 1024;

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
int main(void) {
    std::clog << "Linux only.\n";
    return 0;
}
#endif