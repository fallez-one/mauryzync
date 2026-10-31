// Two behaviours that readiness backends can easily get subtly wrong, checked identically on
// whichever backend the build selected (epoll, kqueue, io_uring, IOCP). POSIX sockets only.
//
//   1. A peer that sends N bytes and closes at once: every byte is delivered and exactly one
//      `closed` follows. (kqueue reports EV_EOF *together with* the unread bytes, so a backend that
//      treats EOF as "done" would drop the tail.) Compared order-insensitively on purpose: each
//      completion is an independent pool task, so the pool may run two completions of one
//      registration out of order -- that is pool scheduling, not something a backend controls.
//   2. UDP keeps datagram boundaries: each datagram is one completion carrying its own sender,
//      including zero-length payload bytes not being mistaken for a close.
//
// Exit code 0 = passed.

#include <FCS/Worker/workers.hpp>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

namespace W = FCS::Worker;
using namespace std::chrono_literals;

namespace {
    struct stream_tag {};
    struct dgram_tag {};
    constexpr std::size_t chunk = 256;
    int failures = 0;
    void check(bool ok, const char* what) { std::printf("%s %s\n", ok ? "[ ok ]" : "[FAIL]", what); if (!ok) ++failures; }
    template<typename P> bool wait_for(P&& p, std::chrono::milliseconds limit = 5000ms) {
        const auto end = std::chrono::steady_clock::now() + limit;
        while (!p()) { if (std::chrono::steady_clock::now() > end) return false; std::this_thread::sleep_for(1ms); }
        return true;
    }
    void nonblocking(int fd) { ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK); }

    struct reader {
        W::read_result<chunk> operator()(W::backend::detail::readiness_source<>& src) const {
            W::read_result<chunk> r{};
            r.size = src.read({r.buffer.data(), r.buffer.size()});
            return r;
        }
    };
    struct dgram_reader {
        template<typename Source>
        W::read_result<chunk> operator()(Source& src) const {
            W::read_result<chunk> r{};
            const auto n = src.read({r.buffer.data(), r.buffer.size()});
            r.size = n + 1;                            // +1 marker byte: an empty datagram must not look like EOF
            sockaddr_storage peer{};
            if (const auto s = src.sender()) peer = *s;
            r.buffer[chunk - 1] = static_cast<std::byte>(peer.ss_family == AF_INET ? 1 : 0);
            return r;
        }
    };

    void run(W::execution::policy policy, const char* name) {
        std::printf("== %s\n", name);
        W::pool_service<> pool;
        pool.concurrency(2);
        pool.execution_policy(policy);
        W::eventlooper loop{pool};
        loop.start();

        // 1. send-then-close
        {
            int sv[2];
            if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) std::abort();
            nonblocking(sv[0]);
            constexpr std::size_t total = 100000;
            // static-lifetime state: queued completions may run after cancel()/the block ends
            static std::mutex m; static std::vector<unsigned char> got; static std::atomic<int> closed; static std::atomic<std::size_t> after_close; // informational only
            got.clear(); closed = 0; after_close = 0;
            auto sub = loop.subscribe<stream_tag, chunk>(W::source_ref<int>{sv[0], W::source_kind::network}, reader{},
                [](W::completion<chunk> c) {
                    std::lock_guard g{m};
                    if (closed.load() != 0) after_close.fetch_add(1);
                    if (c.status == W::completion_status::ok) { for (std::size_t i = 0; i < c.result.size; ++i) got.push_back(static_cast<unsigned char>(c.result.buffer[i])); }
                    else if (c.status == W::completion_status::closed) closed.fetch_add(1);
                });
            std::vector<unsigned char> payload(total);
            for (std::size_t i = 0; i < total; ++i) payload[i] = static_cast<unsigned char>(i * 7 + 3);
            std::thread writer([&] {
                std::size_t off = 0;
                while (off < total) {
                    const auto n = ::send(sv[1], payload.data() + off, total - off, 0);
                    if (n > 0) off += static_cast<std::size_t>(n); else std::this_thread::sleep_for(100us);
                }
                ::close(sv[1]);                       // immediately: EOF arrives with bytes still unread
            });
            writer.join();
            check(wait_for([] { return closed.load() >= 1; }), "send-then-close: closed delivered");
            std::this_thread::sleep_for(100ms);
            std::lock_guard g{m};
            auto sorted_got = got, sorted_payload = payload;
            std::sort(sorted_got.begin(), sorted_got.end());
            std::sort(sorted_payload.begin(), sorted_payload.end());
            check(got.size() == total && sorted_got == sorted_payload, "send-then-close: every byte delivered (none lost to EOF)");
            check(closed.load() == 1, "send-then-close: exactly one closed");
            (void)sub.cancel();
            ::close(sv[0]);
        }

        // 2. UDP boundaries + sender
        {
            const int rx = ::socket(AF_INET, SOCK_DGRAM, 0);
            sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
            if (::bind(rx, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) std::abort();
            socklen_t len = sizeof(a);
            ::getsockname(rx, reinterpret_cast<sockaddr*>(&a), &len);
            nonblocking(rx);
            static std::mutex m; static std::vector<std::size_t> sizes; static std::atomic<int> with_sender;
            sizes.clear(); with_sender = 0;
            auto sub = loop.subscribe<dgram_tag, chunk>(W::source_ref<int>{rx, W::source_kind::datagram}, dgram_reader{},
                [](W::completion<chunk> c) {
                    if (c.status != W::completion_status::ok) return;
                    std::lock_guard g{m};
                    sizes.push_back(c.result.size - 1);
                    if (static_cast<unsigned>(c.result.buffer[chunk - 1]) == 1u) with_sender.fetch_add(1);
                });
            const int tx = ::socket(AF_INET, SOCK_DGRAM, 0);
            const char msg[] = "0123456789";
            const std::size_t lens[] = {10, 3, 0, 7};
            for (auto l : lens) { (void)!::sendto(tx, msg, l, 0, reinterpret_cast<sockaddr*>(&a), sizeof(a)); std::this_thread::sleep_for(5ms); }
            check(wait_for([] { std::lock_guard g{m}; return sizes.size() >= 4; }), "udp: four datagrams delivered");
            { std::lock_guard g{m};
              // Sorted: completions are independent pool tasks, so two of them may run out of order.
              // What this checks is that each datagram stayed ONE event of its own size -- a backend
              // that merged or split datagrams would change the multiset of sizes, not just the order.
              auto got_sizes = sizes;
              std::sort(got_sizes.begin(), got_sizes.end());
              const bool boundaries_ok = got_sizes == std::vector<std::size_t>({0, 3, 7, 10});
              if (!boundaries_ok) { std::printf("  sizes seen:"); for (auto z : sizes) std::printf(" %zu", z); std::printf("\n"); }
              check(boundaries_ok, "udp: boundaries preserved (incl. the empty datagram)");
              check(with_sender.load() == 4, "udp: sender reported for every datagram"); }
            (void)sub.cancel();
            ::close(tx); ::close(rx);
        }
        loop.stop();
    }
}

int main() {
    run(W::execution::policy::shared_worker, "shared_worker");
    run(W::execution::policy::dedicated_poller, "dedicated_poller");
    std::printf("eof_and_datagram: %s\n", failures ? "FAILED" : "all passed");
    return failures ? 1 : 0;
}
