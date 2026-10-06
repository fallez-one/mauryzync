// Backend contract test: the same assertions run against whichever backend the
// build selected (epoll, io_uring or IOCP) under both execution policies.
// It pins down the *observable* behaviour the three backends must share:
//
//   1. stream reads deliver exactly the bytes sent
//   2. EOF yields exactly one `closed` completion, then the subscription is dead
//   3. register_sink(): Writer runs, bytes reach the peer, cancel() stops it
//   4. recv_option::peek leaves the data unconsumed
//   5. datagram sources capture the sender address
//   6. cancel() is idempotent, and nothing is delivered after it settles
//   7. register/cancel churn on one fd (cancel racing a not-yet-armed registration)
//   8. stop() with live registrations returns promptly and frees everything
//
// Exit code 0 = all passed. Run it under ASan/UBSan/TSan on POSIX.

#include <FCS/Worker/workers.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
using sock_t = SOCKET;
constexpr sock_t bad_sock = INVALID_SOCKET;
#else
#  include <arpa/inet.h>
#  include <fcntl.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#  include <unistd.h>
using sock_t = int;
constexpr sock_t bad_sock = -1;
#endif

namespace {

using namespace std::chrono_literals;
namespace W = FCS::Worker;
constexpr std::size_t chunk = 256;

std::atomic<int> failures{0};
#define CHECK(cond) do { if (!(cond)) { failures.fetch_add(1); std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); } } while (0)

template<typename Pred>
bool wait_for(Pred&& pred, std::chrono::milliseconds limit = 5000ms) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > end) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}
void settle() { std::this_thread::sleep_for(150ms); }

void close_sock(sock_t s) {
#if defined(_WIN32)
    ::closesocket(s);
#else
    ::close(s);
#endif
}
void make_nonblocking(sock_t s) {
#if defined(_WIN32)
    u_long on = 1; ::ioctlsocket(s, static_cast<long>(FIONBIO), &on); // FIONBIO is an unsigned constant; ioctlsocket takes long
#else
    ::fcntl(s, F_SETFL, ::fcntl(s, F_GETFL, 0) | O_NONBLOCK);
#endif
}
sock_t new_socket(int type) {
#if defined(_WIN32)
    // Overlapped on purpose: IOCP cannot complete I/O on a non-overlapped socket.
    return ::WSASocketW(AF_INET, type, 0, nullptr, 0, WSA_FLAG_OVERLAPPED);
#else
    return ::socket(AF_INET, type, 0);
#endif
}
int send_bytes(sock_t s, const void* p, std::size_t n) {
#if defined(_WIN32)
    return ::send(s, static_cast<const char*>(p), static_cast<int>(n), 0);
#else
    return static_cast<int>(::send(s, p, n, 0));
#endif
}
int recv_bytes(sock_t s, void* p, std::size_t n) {
#if defined(_WIN32)
    return ::recv(s, static_cast<char*>(p), static_cast<int>(n), 0);
#else
    return static_cast<int>(::recv(s, p, n, 0));
#endif
}
void shutdown_write(sock_t s) {
#if defined(_WIN32)
    ::shutdown(s, SD_SEND);
#else
    ::shutdown(s, SHUT_WR);
#endif
}
std::uint16_t local_port(sock_t s) {
    sockaddr_in a{};
#if defined(_WIN32)
    int len = sizeof(a);
#else
    socklen_t len = sizeof(a);
#endif
    ::getsockname(s, reinterpret_cast<sockaddr*>(&a), &len);
    return ntohs(a.sin_port);
}
sockaddr_in loopback(std::uint16_t port) {
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(port);
    return a;
}

struct pair_t { sock_t a{bad_sock}, b{bad_sock}; };

pair_t make_pair() {
    pair_t p;
    const sock_t listener = new_socket(SOCK_STREAM);
    p.b = new_socket(SOCK_STREAM);
    auto addr = loopback(0);
    if (listener == bad_sock || p.b == bad_sock) std::abort();
    if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(listener, 1) != 0) std::abort();
    addr = loopback(local_port(listener));
    if (::connect(p.b, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) std::abort();
    p.a = ::accept(listener, nullptr, nullptr);
    close_sock(listener);
    if (p.a == bad_sock) std::abort();
    make_nonblocking(p.a);
    make_nonblocking(p.b);
    return p;
}
// cancel() does not wait for a handler that is already running on the backend's
// thread, so closing an fd right after cancel() can race it. Same rule the
// stressor documents: fds are only closed once the backend has been stopped.
std::vector<sock_t>& deferred_closes() { static std::vector<sock_t> v; return v; }
void close_later(sock_t s) { deferred_closes().push_back(s); }
void close_pair(pair_t& p) { close_later(p.a); close_later(p.b); }
void flush_closes() { for (auto s : deferred_closes()) close_sock(s); deferred_closes().clear(); }

// Generic over the source type: readiness_source on epoll, completed_source on io_uring/IOCP.
struct any_reader {
    std::atomic<std::uint16_t>* sender_port{nullptr};
    template<typename Source>
    W::read_result<chunk> operator()(Source& src) const {
        W::read_result<chunk> r{};
        r.size = src.read({r.buffer.data(), r.buffer.size()});
        if (sender_port) {
            if (auto s = src.sender()) {
                sender_port->store(ntohs(reinterpret_cast<const sockaddr_in*>(&*s)->sin_port));
            }
        }
        return r;
    }
};

struct tag_stream {}; struct tag_sink {}; struct tag_peek {}; struct tag_dgram {}; struct tag_churn {}; struct tag_stop_r {}; struct tag_stop_w {};

struct counters {
    std::atomic<std::uint64_t> ok{0}, bytes{0}, closed{0}, errors{0}, checksum{0};
    void take(const W::completion<chunk>& c) {
        switch (c.status) {
        case W::completion_status::ok:
            ok.fetch_add(1);
            bytes.fetch_add(c.result.size);
            for (std::size_t i = 0; i < c.result.size; ++i) checksum.fetch_add(static_cast<std::uint8_t>(c.result.buffer[i]));
            break;
        case W::completion_status::closed: closed.fetch_add(1); break;
        case W::completion_status::error: errors.fetch_add(1); break;
        default: break;
        }
    }
};

// ------------------------------------------------------------------ tests
//
// Everything a callback touches is shared_ptr-owned: a completion that was
// already queued when cancel() ran may legitimately still execute afterwards,
// so it must not reference the test function's stack.

void test_stream_and_eof(W::eventlooper& loop) {
    std::printf(" stream roundtrip + EOF\n");
    auto p = make_pair();
    auto c = std::make_shared<counters>();
    auto sub = loop.subscribe<tag_stream, chunk>(W::source_ref<sock_t>{p.a, W::source_kind::network}, any_reader{},
                                                 [c](W::completion<chunk> r) { c->take(r); });
    CHECK(sub.active());

    std::uint64_t expect_sum = 0;
    std::uint64_t expect_bytes = 0;
    for (int i = 0; i < 10; ++i) {
        std::array<unsigned char, 100> buf{};
        for (std::size_t j = 0; j < buf.size(); ++j) { buf[j] = static_cast<unsigned char>(static_cast<std::size_t>(i) * 7 + j); expect_sum += buf[j]; }
        CHECK(send_bytes(p.b, buf.data(), buf.size()) == 100);
        expect_bytes += 100;
        std::this_thread::sleep_for(3ms);
    }
    CHECK(wait_for([&] { return c->bytes.load() == expect_bytes; }));
    CHECK(c->checksum.load() == expect_sum);

    // EOF straight after the last data: the terminal completion must still be
    // delivered even though the registration is freed right away.
    shutdown_write(p.b);
    CHECK(wait_for([&] { return c->closed.load() == 1; }));
    settle();
    CHECK(c->closed.load() == 1);   // exactly once
    CHECK(c->errors.load() == 0);
    CHECK(!sub.cancel());           // already retired by the EOF
    close_pair(p);
}

void test_sink(W::eventlooper& loop) {
    std::printf(" sink (register_sink)\n");
    auto p = make_pair();
    auto invocations = std::make_shared<std::atomic<int>>(0);
    auto sent = std::make_shared<std::atomic<int>>(0);
    auto c = std::make_shared<counters>();
    auto sub = loop.subscribe_write<tag_sink, chunk>(W::source_ref<sock_t>{p.b, W::source_kind::network},
        [invocations, sent](W::backend::detail::readiness_sink& sink) -> std::size_t {
            invocations->fetch_add(1);
            if (sent->load() >= 5) return 0;                // stop producing, keep being polled
            const char payload[16] = {'w','w','w','w','w','w','w','w','w','w','w','w','w','w','w','w'};
            const auto n = sink.write(std::as_bytes(std::span{payload, sizeof(payload)}));
            if (n) sent->fetch_add(1);
            return n;
        },
        [c](W::completion<chunk> r) { c->take(r); });
    CHECK(sub.active());
    CHECK(wait_for([&] { return sent->load() >= 5; }));

    std::size_t got = 0;
    CHECK(wait_for([&] {
        char buf[64];
        const int n = recv_bytes(p.a, buf, sizeof(buf));
        if (n > 0) got += static_cast<std::size_t>(n);
        return got >= 80;
    }));
    CHECK(got == 80);

    CHECK(sub.cancel());
    CHECK(!sub.cancel());
    settle();
    const int after = invocations->load();
    settle();
    CHECK(invocations->load() == after);   // nothing runs after cancel has settled
    close_pair(p);
}

void test_peek(W::eventlooper& loop) {
    std::printf(" recv_option::peek leaves data unconsumed\n");
    auto p = make_pair();
    CHECK(send_bytes(p.b, "peek-me", 7) == 7);
    auto hits = std::make_shared<std::atomic<std::uint64_t>>(0);
    auto sub = loop.subscribe<tag_peek, chunk, W::backend::detail::recv_option::peek>(
        W::source_ref<sock_t>{p.a, W::source_kind::network}, any_reader{},
        [hits](W::completion<chunk> r) { if (r.status == W::completion_status::ok && r.result.size == 7) hits->fetch_add(1); });
    CHECK(wait_for([&] { return hits->load() >= 1; }));
    CHECK(sub.cancel());
    settle();
    char buf[16]{};
    int n = 0;
    CHECK(wait_for([&] { n = recv_bytes(p.a, buf, sizeof(buf)); return n > 0; }));
    CHECK(n == 7);
    CHECK(std::memcmp(buf, "peek-me", 7) == 0);
    close_pair(p);
}

void test_datagram(W::eventlooper& loop) {
    std::printf(" datagram sender capture\n");
    const sock_t rx = new_socket(SOCK_DGRAM);
    const sock_t tx = new_socket(SOCK_DGRAM);
    auto any = loopback(0);
    CHECK(::bind(rx, reinterpret_cast<sockaddr*>(&any), sizeof(any)) == 0);
    CHECK(::bind(tx, reinterpret_cast<sockaddr*>(&any), sizeof(any)) == 0);
    make_nonblocking(rx);
    const auto rx_addr = loopback(local_port(rx));
    const auto tx_port = local_port(tx);

    auto seen_port = std::make_shared<std::atomic<std::uint16_t>>(0);
    auto c = std::make_shared<counters>();
    auto sub = loop.subscribe<tag_dgram, chunk>(W::source_ref<sock_t>{rx, W::source_kind::datagram}, any_reader{seen_port.get()},
                                                [c, seen_port](W::completion<chunk> r) { c->take(r); });
    CHECK(sub.active());
    CHECK(::sendto(tx, "dgram", 5, 0, reinterpret_cast<const sockaddr*>(&rx_addr), sizeof(rx_addr)) == 5);
    CHECK(wait_for([&] { return c->bytes.load() == 5; }));
    CHECK(seen_port->load() == tx_port);
    CHECK(sub.cancel());
    settle();
    close_later(rx);
    close_later(tx);
}

void test_cancel_semantics_and_churn(W::eventlooper& loop) {
    std::printf(" cancel semantics + register/cancel churn\n");
    auto p = make_pair();
    auto c = std::make_shared<counters>();

    auto sub = loop.subscribe<tag_churn, chunk>(W::source_ref<sock_t>{p.a, W::source_kind::network}, any_reader{},
                                                [c](W::completion<chunk> r) { c->take(r); });
    CHECK(sub.cancel());
    CHECK(!sub.cancel());
    settle();
    CHECK(send_bytes(p.b, "x", 1) == 1);
    settle();
    CHECK(c->ok.load() == 0);          // cancelled subscription never delivers

    // Cancel racing a registration the driver may not have armed yet.
    auto delivered = std::make_shared<std::atomic<std::uint64_t>>(0);
    for (int i = 0; i < 1500; ++i) {
        auto s = loop.subscribe<tag_churn, chunk>(W::source_ref<sock_t>{p.a, W::source_kind::network}, any_reader{},
                                                  [delivered](W::completion<chunk>) { delivered->fetch_add(1); });
        if (i % 3 == 0) std::this_thread::yield();
        (void)s.cancel();
    }
    // Two threads hammering distinct pairs at once.
    auto q = make_pair();
    std::vector<std::thread> threads;
    for (auto* pp : {&p, &q}) {
        threads.emplace_back([&loop, delivered, pp] {
            for (int i = 0; i < 800; ++i) {
                auto s = loop.subscribe<tag_churn, chunk>(W::source_ref<sock_t>{pp->a, W::source_kind::network}, any_reader{},
                                                          [delivered](W::completion<chunk>) { delivered->fetch_add(1); });
                (void)s.cancel();
            }
        });
    }
    for (auto& t : threads) t.join();
    settle();
    const auto before = delivered->load();
    CHECK(send_bytes(p.b, "y", 1) == 1);
    CHECK(send_bytes(q.b, "y", 1) == 1);
    settle();
    CHECK(delivered->load() == before);   // nothing live is left registered on either fd
    close_pair(q);
    close_pair(p);
}

void test_stop_with_live_registrations(W::pool_service<>& service) {
    std::printf(" stop() with live registrations\n");
    std::vector<pair_t> pairs;
    for (int i = 0; i < 8; ++i) pairs.push_back(make_pair());
    {
        W::eventlooper loop{service};
        loop.start();
        std::vector<W::subscription> subs;
        auto c = std::make_shared<counters>();
        for (auto& p : pairs) {
            subs.push_back(loop.subscribe<tag_stop_r, chunk>(W::source_ref<sock_t>{p.a, W::source_kind::network}, any_reader{},
                                                             [c](W::completion<chunk> r) { c->take(r); }));
            subs.push_back(loop.subscribe_write<tag_stop_w, chunk>(W::source_ref<sock_t>{p.b, W::source_kind::network},
                [](W::backend::detail::readiness_sink&) -> std::size_t { return 0; },
                [](W::completion<chunk>) {}));
        }
        // Keep data flowing while it shuts down.
        std::atomic<bool> feed{true};
        std::thread feeder([&] { while (feed.load()) { for (auto& p : pairs) (void)send_bytes(p.b, "z", 1); std::this_thread::sleep_for(1ms); } });
        std::this_thread::sleep_for(100ms);
        std::atomic<bool> stopped{false};
        std::thread stopper([&] { loop.stop(); stopped.store(true); });
        CHECK(wait_for([&] { return stopped.load(); }, 10000ms));
        feed.store(false);
        feeder.join();
        stopper.join();
        // `subs` die here, after the backend has already shut down: must be a harmless no-op.
    }
    for (auto& p : pairs) close_pair(p);
}

void run_all(W::execution::policy policy, const char* name) {
    std::printf("== execution policy: %s\n", name);
    W::pool_service<> service;
    service.concurrency(3).execution_policy(policy).start();
    {
        W::eventlooper loop{service};
        loop.start();
        test_stream_and_eof(loop);
        test_sink(loop);
        test_peek(loop);
        test_datagram(loop);
        test_cancel_semantics_and_churn(loop);
        loop.stop();
    }
    flush_closes();
    test_stop_with_live_registrations(service);
    flush_closes();
    service.stop();
}

} // namespace

int main() {
#if defined(_WIN32)
    WSADATA wsa{};
    if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return 2;
#endif
    run_all(W::execution::shared_worker, "shared_worker");
    run_all(W::execution::dedicated_poller, "dedicated_poller");
#if defined(_WIN32)
    ::WSACleanup();
#endif
    if (failures.load() != 0) { std::printf("\nbackend_contract: %d FAILED\n", failures.load()); return 1; }
    std::printf("\nbackend_contract: all passed\n");
    return 0;
}
