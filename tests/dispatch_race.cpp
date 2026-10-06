// Targets epoll::backend::dispatch() being unprotected against (a) a
// concurrent reclaim() from another poller and (b) a second poller
// dispatching the same registration. Many shared_worker pollers, one hot
// stream (level-triggered overlap), several threads churning
// register/cancel on their own fds (retire + reclaim while others dispatch).
#include <FCS/Worker/workers.hpp>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

struct hot_tag {};
struct churn_tag {};
struct duplex_read_tag {};
struct duplex_write_tag {};
constexpr std::size_t chunk = 64;

// Deliberately non-atomic state: concurrent invocation on one registration is a data race TSan can see.
struct counting_reader {
    std::uint64_t seen{};
    // Generic over the source: readiness_source<> on epoll, completed_source on io_uring/IOCP.
    template<typename Source>
    FCS::Worker::read_result<chunk> operator()(Source& src) {
        FCS::Worker::read_result<chunk> r{};
        r.size = src.read({r.buffer.data(), r.buffer.size()});
        ++seen;
        // Yield the core while still inside dispatch(): on a single core this is what
        // makes another poller actually run (and re-see the still-readable fd, or reclaim
        // a just-cancelled registration) while this call is mid-flight.
        std::this_thread::sleep_for(std::chrono::microseconds(150));
        return r;
    }
};

static void make_pair(int fds[2]) {
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) std::abort();
    for (int i = 0; i < 2; ++i) ::fcntl(fds[i], F_SETFL, ::fcntl(fds[i], F_GETFL) | O_NONBLOCK);
}

int main(int argc, char** argv) {
    const auto duration = std::chrono::milliseconds(argc > 1 ? std::stoul(argv[1]) : 1500);
    const std::size_t workers = 6;

    FCS::Worker::pool_service service;
    service.concurrency(workers).execution_policy(FCS::Worker::execution::shared_worker, FCS::Worker::execution::task_type::segment, workers).start();
    FCS::Worker::eventlooper loop{service};
    loop.start();

    std::atomic<std::uint64_t> hot_ok{0}, hot_dead{0}, churn_cycles{0};
    int hot[2];
    make_pair(hot);
    auto hot_sub = loop.subscribe<hot_tag, chunk>(
        FCS::Worker::source_ref<int>{hot[0], FCS::Worker::source_kind::io}, counting_reader{},
        [&](FCS::Worker::completion<chunk> c) {
            if (c.status == FCS::Worker::completion_status::ok) hot_ok.fetch_add(1, std::memory_order_relaxed);
            else hot_dead.fetch_add(1, std::memory_order_relaxed);
        });

    constexpr std::size_t churners = 3, pairs_each = 8;
    std::vector<std::array<int, 2>> churn_fds(churners * pairs_each);
    for (auto& p : churn_fds) make_pair(p.data());

    // Scenario B: full-duplex churn. A permanent read subscription keeps this fd's
    // registration alive, so subscribe_write()/cancel() cycles REUSE it -- reassigning
    // on_writable (a std::function) from this thread while a poller may still be
    // executing the previous writer.
    int dup[2];
    make_pair(dup);
    std::atomic<std::uint64_t> duplex_cycles{0}, duplex_read_ok{0};
    auto dup_read = loop.subscribe<duplex_read_tag, chunk>(
        FCS::Worker::source_ref<int>{dup[0], FCS::Worker::source_kind::io}, counting_reader{},
        [&](FCS::Worker::completion<chunk> c) { if (c.status == FCS::Worker::completion_status::ok) duplex_read_ok.fetch_add(1, std::memory_order_relaxed); });

    std::atomic_bool stop{false};
    std::thread duplex_churn([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto w = loop.subscribe_write<duplex_write_tag, chunk>(
                FCS::Worker::source_ref<int>{dup[0], FCS::Worker::source_kind::io},
                [](FCS::Worker::backend::detail::readiness_sink& sink) -> std::size_t {
                    std::this_thread::sleep_for(std::chrono::microseconds(150)); // mid-dispatch, yields the core
                    const char b = 'w';
                    return sink.write(std::as_bytes(std::span{&b, 1}));
                },
                [](FCS::Worker::completion<chunk>) {});
            std::this_thread::yield();
            (void)w.cancel();
            duplex_cycles.fetch_add(1, std::memory_order_relaxed);
        }
    });
    std::thread feeder([&] {
        const char buf[32] = {};
        while (!stop.load(std::memory_order_relaxed)) {
            (void)!::send(hot[1], buf, sizeof(buf), MSG_DONTWAIT);
            (void)!::send(dup[1], buf, sizeof(buf), MSG_DONTWAIT);
            for (auto& p : churn_fds) (void)!::send(p[1], buf, sizeof(buf), MSG_DONTWAIT);
            std::this_thread::yield();
        }
    });

    std::vector<std::thread> churn_threads;
    for (std::size_t t = 0; t < churners; ++t) {
        churn_threads.emplace_back([&, t] {
            while (!stop.load(std::memory_order_relaxed)) {
                for (std::size_t k = 0; k < pairs_each; ++k) {
                    const int fd = churn_fds[t * pairs_each + k][0];
                    auto sub = loop.subscribe<churn_tag, chunk>(
                        FCS::Worker::source_ref<int>{fd, FCS::Worker::source_kind::io}, counting_reader{},
                        [](FCS::Worker::completion<chunk>) {});
                    std::this_thread::yield();
                    (void)sub.cancel();
                    churn_cycles.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    std::this_thread::sleep_for(duration);
    stop.store(true);
    feeder.join();
    duplex_churn.join();
    for (auto& t : churn_threads) t.join();
    (void)hot_sub.cancel();
    (void)dup_read.cancel();
    loop.stop();
    service.stop();

    std::printf("dispatch_race: hot_ok=%llu hot_dead(unexpected retire/error)=%llu churn_cycles=%llu duplex_cycles=%llu\n",
                static_cast<unsigned long long>(hot_ok.load()), static_cast<unsigned long long>(hot_dead.load()),
                static_cast<unsigned long long>(churn_cycles.load()), static_cast<unsigned long long>(duplex_cycles.load()));
    for (int fd : {hot[0], hot[1], dup[0], dup[1]}) ::close(fd);
    for (auto& p : churn_fds) { ::close(p[0]); ::close(p[1]); }
    return hot_dead.load() == 0 ? 0 : 2;
}
