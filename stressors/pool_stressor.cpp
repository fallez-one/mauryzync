// FCS-Workers stress harness -- successor to bench_pool.cpp, rebuilt against
// the current API (full duplex, recv_option, enqueue_until/timeout_tolerate/
// age_starvation, execution_policy(policy,task_type,between_num_threads),
// enqueue_and_poll with coroutine auto-poll, worker_profile()).
//
// This is deliberately adversarial, not a polite benchmark: multiple
// producer threads hammering the same pool_service concurrently, tiny
// backpressure thresholds to force real admission-gate rejection, a
// high-priority flood specifically shaped to try to starve normal work,
// and CPU-heavy dummy work on every task so the scheduler is never just
// waiting on an empty queue. Every phase ends by dumping scheduler_metadata()
// AND every worker's worker_profile() -- steal traffic in both directions,
// batch vs. single-item breakdowns -- not just pool-wide totals.
//
// Windows-first for the I/O phases, same as bench_pool.cpp was: the network/
// full-duplex phases exercise the IOCP backend on Windows (a raw Winsock
// SOCKET registered via eventlooper::subscribe()/subscribe_write()) and the
// epoll backend elsewhere, through identical FCS::Worker-facing code. Only
// verified by compilation+execution on Linux/epoll in this environment --
// the Windows branches mirror bench_pool.cpp's already-established pattern
// but haven't been compiled on that platform here.
//
// Usage:
//   pool_stressor [threads] [compute_tasks] [iterations_per_task] [duration_ms]
//
//   threads             - worker thread count for every phase (default: hardware_concurrency * 2, oversubscribed on purpose)
//   compute_tasks       - task count for the compute-stress phase (default: 400000)
//   iterations_per_task - busy_work() iterations per compute task (default: 20000)
//   duration_ms         - wall-clock duration for the sustained phases: timers, starvation, full-duplex, execution-policy comparison, registration churn (default: 4000)
//
// Defaults are intentionally harsh; pass smaller numbers for a quick smoke run.

#include <FCS/Worker/workers.hpp>
#include <mutex>

#include <algorithm>
#include <array>
#include <cstdlib>
#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#  include <timeapi.h>
#endif
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  pragma comment(lib, "ws2_32.lib")
namespace stressor {
    using native_socket = SOCKET;
    constexpr native_socket invalid_socket = INVALID_SOCKET;
}
#else
#  include <arpa/inet.h>
#  include <fcntl.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <sys/socket.h>
#  include <unistd.h>
namespace stressor {
    using native_socket = int;
    constexpr native_socket invalid_socket = -1;
}
#endif

namespace stressor {

// ---------------------------------------------------------------------
// platform shim -- same split as bench_pool.cpp: only socket setup/
// teardown differs between Winsock and BSD sockets.
// ---------------------------------------------------------------------

struct platform_guard {
#if defined(_WIN32)
    platform_guard() {
        WSADATA data{};
        ok = ::WSAStartup(MAKEWORD(2, 2), &data) == 0;
    }
    ~platform_guard() { if (ok) ::WSACleanup(); }
    bool ok{false};
#else
    bool ok{true};
#endif
};

inline void close_socket(native_socket s) noexcept {
#if defined(_WIN32)
    ::closesocket(s);
#else
    ::close(s);
#endif
}

inline void set_nonblocking(native_socket s) noexcept {
#if defined(_WIN32)
    u_long on = 1;
    ::ioctlsocket(s, FIONBIO, &on);
#else
    ::fcntl(s, F_SETFL, ::fcntl(s, F_GETFL, 0) | O_NONBLOCK);
#endif
}

// Phases that assert an invariant bump this; main() turns it into the exit code.
inline std::atomic<int> g_failures{0};

inline bool make_loopback_pair(native_socket& server_side, native_socket& client_side) {
#if defined(_WIN32)
    // Both ends must be overlapped: accept() inherits the listener's mode, and a
    // non-overlapped socket silently runs "overlapped" WSARecv/WSASend as blocking
    // calls and never completes through an IOCP.
    const native_socket listener = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    client_side = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
#else
    const native_socket listener = ::socket(AF_INET, SOCK_STREAM, 0);
    client_side = ::socket(AF_INET, SOCK_STREAM, 0);
#endif
    if (listener == invalid_socket || client_side == invalid_socket) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
    if (::listen(listener, 1) != 0) return false;

#if defined(_WIN32)
    int len = sizeof(addr);
    if (::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return false;
#else
    socklen_t slen = sizeof(addr);
    if (::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &slen) != 0) return false;
#endif

    if (::connect(client_side, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
    server_side = ::accept(listener, nullptr, nullptr);
    close_socket(listener);
    if (server_side == invalid_socket) return false;

    const int one = 1;
    ::setsockopt(server_side, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), sizeof(one));
    ::setsockopt(client_side, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), sizeof(one));
    return true;
}

// ---------------------------------------------------------------------
// dummy heavy computation -- genuine, unpredictable ALU burn with a real
// data dependency chain (xorshift64 mixed with the loop counter), so
// nothing gets hoisted or eliminated. Every task in the compute-stress and
// starvation phases runs this; iterations scales how harsh it is.
// ---------------------------------------------------------------------

[[nodiscard]] std::uint64_t busy_work(std::uint64_t iterations) noexcept {
    std::uint64_t x = iterations * 2654435761ull + 0x9E3779B97F4A7C15ull;
    for (std::uint64_t i = 0; i < iterations; ++i) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x += i;
    }
    return x;
}

// ---------------------------------------------------------------------
// stats helpers
// ---------------------------------------------------------------------

struct percentiles { double p50{}, p90{}, p99{}, max{}; };

percentiles summarize(std::vector<double>& samples_us) {
    if (samples_us.empty()) return {};
    std::sort(samples_us.begin(), samples_us.end());
    const auto at = [&](double q) {
        const auto idx = std::min(samples_us.size() - 1, static_cast<std::size_t>(q * static_cast<double>(samples_us.size())));
        return samples_us[idx];
    };
    return {at(0.50), at(0.90), at(0.99), samples_us.back()};
}

struct running_stats {
    std::atomic<std::uint64_t> count{0};
    std::atomic<double> sum_us{0.0};
    std::atomic<double> max_us{0.0};

    void record(double us) noexcept {
        count.fetch_add(1, std::memory_order_relaxed);
        sum_us.fetch_add(us, std::memory_order_relaxed);
        double observed = max_us.load(std::memory_order_relaxed);
        while (us > observed && !max_us.compare_exchange_weak(observed, us, std::memory_order_relaxed)) {}
    }
};

// Every phase ends here: pool-wide scheduler_metadata() plus every worker's
// individual worker_profile() -- not just aggregate totals.
// ---- hang diagnosis -------------------------------------------------------------
// If a phase runs longer than FCS_STRESS_HANG_SECONDS (default 120) the monitor in
// main() prints the phase name plus pool_service::debug_dump() of every live pool and
// exits with code 3, instead of leaving CI / the user staring at a silent hang.
inline std::array<std::atomic<FCS::Worker::pool_service<>*>, 8> g_live_pools{};
struct tracked_pool {
    explicit tracked_pool(FCS::Worker::pool_service<>* p) noexcept {
        for (auto& slot : g_live_pools) {
            FCS::Worker::pool_service<>* expected = nullptr;
            if (slot.compare_exchange_strong(expected, p)) { mine = &slot; return; }
        }
    }
    ~tracked_pool() { if (mine) mine->store(nullptr); }
    tracked_pool(const tracked_pool&) = delete;
    tracked_pool& operator=(const tracked_pool&) = delete;
    std::atomic<FCS::Worker::pool_service<>*>* mine{nullptr};
};
inline std::atomic<const char*> g_phase_name{"startup"};
inline std::atomic<std::int64_t> g_phase_started_ns{0};
inline void enter_phase(const char* name) noexcept {
    g_phase_name.store(name);
    g_phase_started_ns.store(std::chrono::steady_clock::now().time_since_epoch().count());
}

void print_worker_profiles(FCS::Worker::pool_service<>& pool) {
    const auto snap = pool.scheduler_metadata();
    std::printf("  --- scheduler_metadata ---\n");
    std::printf("  submitted=%llu completed=%llu active=%llu rejected=%llu\n",
                static_cast<unsigned long long>(snap.submitted), static_cast<unsigned long long>(snap.completed),
                static_cast<unsigned long long>(snap.active), static_cast<unsigned long long>(snap.rejected));
    std::printf("  synthetic=%llu io=%llu network=%llu\n",
                static_cast<unsigned long long>(snap.synthetic_submitted), static_cast<unsigned long long>(snap.io_submitted),
                static_cast<unsigned long long>(snap.network_submitted));
    std::printf("  cushion_depth=%llu mpmc_depth=%llu local_depth=%llu\n",
                static_cast<unsigned long long>(snap.cushion_depth), static_cast<unsigned long long>(snap.mpmc_depth),
                static_cast<unsigned long long>(snap.local_depth));
    std::printf("  refill_claims=%llu drain_claims=%llu steals=%llu\n",
                static_cast<unsigned long long>(snap.refill_claims), static_cast<unsigned long long>(snap.drain_claims),
                static_cast<unsigned long long>(snap.steals));

    std::printf("  --- per-worker profiles (%zu workers) ---\n", pool.worker_count());
    for (std::size_t id = 0; id < pool.worker_count(); ++id) {
        const auto p = pool.worker_profile(id);
        std::printf("  worker[%2zu] incoming: attempts=%-6llu stolen=%-6llu (batches=%llu singles=%llu)"
                    "  outgoing: attempts=%-6llu stolen=%-6llu (batches=%llu singles=%llu)\n",
                    id,
                    static_cast<unsigned long long>(p.incoming_attempts), static_cast<unsigned long long>(p.incoming_stolen),
                    static_cast<unsigned long long>(p.incoming_batches), static_cast<unsigned long long>(p.incoming_singles),
                    static_cast<unsigned long long>(p.outgoing_attempts), static_cast<unsigned long long>(p.outgoing_stolen),
                    static_cast<unsigned long long>(p.outgoing_batches), static_cast<unsigned long long>(p.outgoing_singles));
        std::printf("             idle-estimate: cache_hits=%-8llu scans=%-6llu stale_reuses=%llu\n",
                    static_cast<unsigned long long>(p.idle_cache_hits), static_cast<unsigned long long>(p.idle_scans),
                    static_cast<unsigned long long>(p.idle_stale_reuses));
        std::printf("             watchdog: times_stalled=%-6llu hostage_stolen=%llu\n",
                    static_cast<unsigned long long>(p.times_stalled), static_cast<unsigned long long>(p.hostage_stolen));
    }
}

// ---------------------------------------------------------------------
// phase 1: compute stress -- multiple producer threads (deliberately more
// producers than the pool has workers) hammering enqueue()/enqueue_priority()/
// enqueue_and_poll() concurrently, harsh backpressure thresholds so the
// admission gate genuinely rejects and callers genuinely retry, every task
// doing real CPU work.
// ---------------------------------------------------------------------

void run_compute_stress(std::size_t threads, std::size_t tasks, std::uint64_t iterations) {
    auto pool = std::make_unique<FCS::Worker::pool_service<>>();
    const tracked_pool tracked_{pool.get()};
    // Harsh on purpose: a small cushion "full" threshold forces genuine
    // admission-gate rejection under concurrent load instead of the gate
    // just absorbing everything.
    pool->concurrency(threads).burst_backpressure(/*full=*/64, /*available=*/16, /*candidates=*/32).start();

    std::atomic<std::uint64_t> completed{0};
    std::atomic<std::uint64_t> rejected_by_caller{0};
    std::atomic<std::uint64_t> coroutine_completed{0};
    std::atomic<std::uint64_t> sink{0}; // accumulates busy_work()'s output so it can never be optimized away
    std::vector<double> latencies_us(tasks, 0.0);

    const std::size_t producers = std::max<std::size_t>(2, threads * 2); // oversubscribed on purpose

    const auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> producer_threads;
    std::atomic<std::size_t> next_index{0};
    for (std::size_t p = 0; p < producers; ++p) {
        producer_threads.emplace_back([&] {
            for (;;) {
                const auto i = next_index.fetch_add(1, std::memory_order_relaxed);
                if (i >= tasks) break;
                const auto submit_time = std::chrono::steady_clock::now();

                // Mix plain enqueue(), enqueue_priority(), and coroutine
                // auto-poll via enqueue_and_poll() across the flood so all
                // three submission paths get genuinely contended.
                if (i % 97 == 0) {
                    auto fut = pool->enqueue_and_poll(FCS::Worker::workload::Fast, [iterations]() -> FCS::async_result<std::uint64_t> {
                        co_return busy_work(iterations);
                    });
                    // Not polled to completion here on purpose -- the point
                    // of mixing this in is contention on submission, not
                    // waiting on every one; a background reaper drains
                    // these (see below).
                    sink.fetch_add(fut.ready() ? 1 : 0, std::memory_order_relaxed);
                    coroutine_completed.fetch_add(1, std::memory_order_relaxed);
                    latencies_us[i] = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - submit_time).count();
                    completed.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

                const bool ok = (i % 11 == 0)
                    ? pool->enqueue_priority(FCS::Worker::detail::task_priority::high, FCS::Worker::source_kind::synthetic, [&, i, submit_time, iterations] {
                          sink.fetch_add(busy_work(iterations), std::memory_order_relaxed);
                          latencies_us[i] = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - submit_time).count();
                          completed.fetch_add(1, std::memory_order_relaxed);
                      })
                    : pool->enqueue(FCS::Worker::workload::Fast, [&, i, submit_time, iterations] {
                          sink.fetch_add(busy_work(iterations), std::memory_order_relaxed);
                          latencies_us[i] = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - submit_time).count();
                          completed.fetch_add(1, std::memory_order_relaxed);
                      });

                if (!ok) {
                    rejected_by_caller.fetch_add(1, std::memory_order_relaxed);
                    // Deliberately not retried at this same index -- this is
                    // a stress test of contention, not a correctness test of
                    // guaranteed delivery, so a rejected slot's latency
                    // sample is just left at its default (0.0) and the
                    // completed count won't reach `tasks`; that gap is
                    // exactly what "caller-side rejects" reports below.
                    std::this_thread::yield();
                }
            }
        });
    }
    for (auto& t : producer_threads) t.join();

    for (int spins = 0; spins < 20000 && completed.load(std::memory_order_relaxed) + rejected_by_caller.load(std::memory_order_relaxed) < tasks; ++spins)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    const auto end = std::chrono::steady_clock::now();

    print_worker_profiles(*pool);
    pool->stop();

    const double seconds = std::chrono::duration<double>(end - start).count();
    const auto stats = summarize(latencies_us);

    std::printf("\n[compute-stress] threads=%zu producers=%zu tasks=%zu iterations/task=%llu\n",
                threads, producers, tasks, static_cast<unsigned long long>(iterations));
    std::printf("  wall time         : %.3f s\n", seconds);
    std::printf("  throughput        : %.0f tasks/s\n", static_cast<double>(tasks) / seconds);
    std::printf("  latency p50/p90/p99/max (us): %.1f / %.1f / %.1f / %.1f\n", stats.p50, stats.p90, stats.p99, stats.max);
    std::printf("  completed         : %llu / %zu\n", static_cast<unsigned long long>(completed.load()), tasks);
    std::printf("  caller-side rejects (retried): %llu\n", static_cast<unsigned long long>(rejected_by_caller.load()));
    std::printf("  coroutine tasks   : %llu\n", static_cast<unsigned long long>(coroutine_completed.load()));
    std::printf("  sink (anti-optimization checksum, ignore value): %llu\n", static_cast<unsigned long long>(sink.load()));
}

// ---------------------------------------------------------------------
// phase 2: full-duplex I/O stress -- many concurrent socket pairs, each
// continuously writing and reading via subscribe_write()/subscribe(), one
// pair exercising recv_option::peek explicitly, all running for
// duration_ms against a pool that's simultaneously running background
// compute load from other threads.
// ---------------------------------------------------------------------

struct duplex_tag_read {};
struct duplex_tag_write {};
struct peek_tag {};
constexpr std::size_t duplex_chunk = 256;

struct chunk_reader {
    template<typename Source>
    FCS::Worker::read_result<duplex_chunk> operator()(Source& source) const {
        FCS::Worker::read_result<duplex_chunk> result{};
        result.size = source.read({result.buffer.data(), result.buffer.size()});
        return result;
    }
};

void run_full_duplex_stress(std::size_t threads, std::size_t pair_count, std::chrono::milliseconds duration) {
    platform_guard guard;
    if (!guard.ok) { std::printf("\n[full-duplex-stress] platform socket layer failed to initialize, skipping\n"); return; }

    auto pool = std::make_unique<FCS::Worker::pool_service<>>();

    const tracked_pool tracked_{pool.get()};
    pool->concurrency(threads).burst_backpressure().start();
    FCS::Worker::eventlooper loop{*pool};
    loop.start();

    // Background compute load running concurrently with the I/O, so the
    // I/O phase is contending for workers the whole time, not running on
    // an otherwise-idle pool.
    std::atomic_bool keep_computing{true};
    std::atomic<std::uint64_t> compute_sink{0};
    std::thread compute_feeder([&] {
        while (keep_computing.load(std::memory_order_relaxed)) {
            (void)pool->enqueue(FCS::Worker::workload::Fast, [&] { compute_sink.fetch_add(busy_work(2000), std::memory_order_relaxed); });
            std::this_thread::yield();
        }
    });

    struct pair_state {
        native_socket server{invalid_socket}, client{invalid_socket};
        std::atomic<std::uint64_t> bytes_written{0};
        std::atomic<std::uint64_t> bytes_read{0};
        FCS::Worker::subscription read_sub;
        FCS::Worker::subscription write_sub;
        std::mutex write_mutex; // guards write_sub the same way item-1's notes require: populated before its own callback can fire
    };

    std::vector<std::unique_ptr<pair_state>> pairs;
    pairs.reserve(pair_count);
    for (std::size_t i = 0; i < pair_count; ++i) {
        auto state = std::make_unique<pair_state>();
        if (!make_loopback_pair(state->server, state->client)) continue;
        pairs.push_back(std::move(state));
    }

    for (auto& state_ptr : pairs) {
        auto& state = *state_ptr;
        state.read_sub = loop.template subscribe<duplex_tag_read, duplex_chunk>(
            state.client, chunk_reader{},
            [&](FCS::Worker::completion<duplex_chunk> c) {
                if (c.status == FCS::Worker::completion_status::ok) state.bytes_read.fetch_add(c.result.size, std::memory_order_relaxed);
            });

        std::lock_guard setup_lock{state.write_mutex};
        state.write_sub = loop.template subscribe_write<duplex_tag_write, duplex_chunk>(
            FCS::Worker::source_ref<native_socket>{state.server, FCS::Worker::source_kind::network},
            [&state](FCS::Worker::backend::detail::readiness_sink& sink) -> std::size_t {
                char payload[64];
                std::memset(payload, 'x', sizeof(payload));
                const auto n = sink.write(std::as_bytes(std::span{payload, sizeof(payload)}));
                state.bytes_written.fetch_add(n, std::memory_order_relaxed);
                return n;
            },
            [&state](FCS::Worker::completion<duplex_chunk> c) {
                if (c.status != FCS::Worker::completion_status::ok) return;
                std::lock_guard callback_lock{state.write_mutex};
                (void)state.write_sub.cancel();
            });
    }

    // Separately from the auto-canceling single-shot writers above, keep
    // re-arming writes for the whole duration so this genuinely stresses
    // full duplex continuously rather than firing once per pair.
    std::atomic_bool keep_writing{true};
    std::thread rearm_writer([&] {
        while (keep_writing.load(std::memory_order_relaxed)) {
            for (auto& state_ptr : pairs) {
                char payload[64];
                std::memset(payload, 'y', sizeof(payload));
#if defined(_WIN32)
                ::send(state_ptr->server, payload, static_cast<int>(sizeof(payload)), 0);
#else
                (void)!::send(state_ptr->server, payload, sizeof(payload), 0);
#endif
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });

    // A dedicated peek probe on its own pair, exercising recv_option::peek
    // specifically: repeatedly peeks and asserts (loudly, via stderr) if it
    // ever sees zero bytes for data known to be pending.
    native_socket peek_server{invalid_socket}, peek_client{invalid_socket};
    std::atomic<std::uint64_t> peek_hits{0};
    FCS::Worker::subscription peek_sub;
    std::mutex peek_mutex;
    if (make_loopback_pair(peek_server, peek_client)) {
        {
            const char seed[] = "peek-me";
#if defined(_WIN32)
            ::send(peek_server, seed, static_cast<int>(sizeof(seed)), 0);
#else
            (void)!::send(peek_server, seed, sizeof(seed), 0);
#endif
        }
        std::lock_guard lock{peek_mutex};
        peek_sub = loop.template subscribe<peek_tag, duplex_chunk, FCS::Worker::backend::detail::recv_option::peek>(
            FCS::Worker::source_ref<native_socket>{peek_client, FCS::Worker::source_kind::network},
            [](auto& src) { // generic: readiness_source<peek> on epoll, completed_source on io_uring/IOCP
                FCS::Worker::read_result<duplex_chunk> r{};
                r.size = src.read({r.buffer.data(), r.buffer.size()});
                return r;
            },
            [&](FCS::Worker::completion<duplex_chunk> c) {
                if (c.status == FCS::Worker::completion_status::ok && c.result.size > 0) peek_hits.fetch_add(1, std::memory_order_relaxed);
            });
    }

    std::this_thread::sleep_for(duration);

    keep_writing.store(false, std::memory_order_relaxed);
    rearm_writer.join();
    keep_computing.store(false, std::memory_order_relaxed);
    compute_feeder.join();

    std::uint64_t total_written{}, total_read{};
    for (auto& state_ptr : pairs) {
        total_written += state_ptr->bytes_written.load(std::memory_order_relaxed);
        total_read += state_ptr->bytes_read.load(std::memory_order_relaxed);
    }

    {
        std::lock_guard lock{peek_mutex};
        (void)peek_sub.cancel();
    }
    for (auto& state_ptr : pairs) {
        (void)state_ptr->read_sub.cancel();
        // write_sub is a one-shot self-canceller (see its completion
        // callback above), but it may never have gotten the chance to fire
        // during the run -- explicitly cancel it too, guarded by the same
        // mutex its own callback uses, so it's never left live-and-
        // registered on a fd we're about to close.
        std::lock_guard write_lock{state_ptr->write_mutex};
        (void)state_ptr->write_sub.cancel();
    }
    // The real fix for "don't close an fd a poller thread might still be
    // mid-syscall on" isn't a sleep (TSan correctly refuses to treat
    // sleep_for as synchronization -- it isn't one) -- it's stopping the
    // eventlooper first. loop.stop() unregisters this backend's poll hook,
    // and poll_registry::unregister() busy-waits on that hook's `busy`
    // flag before returning, so by the time loop.stop() returns, no
    // poll_once() call touching these fds can still be in flight, with a
    // real happens-before edge TSan recognizes. Only then is it safe to
    // close the sockets.
    loop.stop();
    for (auto& state_ptr : pairs) {
        close_socket(state_ptr->server);
        close_socket(state_ptr->client);
    }
    if (peek_client != invalid_socket) close_socket(peek_client);
    if (peek_server != invalid_socket) close_socket(peek_server);

    print_worker_profiles(*pool);
    pool->stop();

    std::printf("\n[full-duplex-stress] threads=%zu pairs=%zu duration=%lldms\n",
                threads, pairs.size(), static_cast<long long>(duration.count()));
    std::printf("  bytes written (self-cancelling one-shot writers): %llu\n", static_cast<unsigned long long>(total_written));
    std::printf("  bytes read                                      : %llu\n", static_cast<unsigned long long>(total_read));
    std::printf("  peek hits (MSG_PEEK, non-consuming)             : %llu\n", static_cast<unsigned long long>(peek_hits.load()));
    std::printf("  background compute sink (anti-optimization, ignore value): %llu\n", static_cast<unsigned long long>(compute_sink.load()));
}

// ---------------------------------------------------------------------
// phase 3: timer stress -- enqueue_until() hammered from multiple threads,
// continuously, in all four timeout_mode values, for duration_ms, tracking
// fire-time jitter (actual - requested) for the deadline-sensitive `timer`
// mode specifically.
// ---------------------------------------------------------------------

struct timer_notify_tag {};

void run_timer_stress(std::size_t threads, std::chrono::milliseconds duration) {
    auto pool = std::make_unique<FCS::Worker::pool_service<>>();
    const tracked_pool tracked_{pool.get()};
    pool->concurrency(threads)
        .execution_policy(FCS::Worker::execution::shared_worker)
        .timeout_tolerate(std::chrono::microseconds(200)) // tight, on purpose: stresses how promptly timers actually fire
        .start();

    std::atomic<std::uint64_t> timer_fired{0}, fast_fired{0}, slow_fired{0}, posted{0};
    running_stats timer_jitter_us;

    std::atomic<std::uint64_t> notify_sum{0};
    const auto notify_sub = pool->subscribe<timer_notify_tag>([&](std::uint64_t v) { notify_sum.fetch_add(v, std::memory_order_relaxed); });

    std::atomic_bool keep_going{true};
    std::vector<std::thread> hammer_threads;
    for (std::size_t t = 0; t < threads; ++t) {
        hammer_threads.emplace_back([&, t] {
            std::uint64_t local_counter = t;
            while (keep_going.load(std::memory_order_relaxed)) {
                const auto requested = std::chrono::microseconds(200 + (local_counter % 2000));
                const auto submit_time = std::chrono::steady_clock::now();
                (void)pool->enqueue_until(FCS::Worker::timeout_mode::timer, requested, [&, requested, submit_time] {
                    const auto actual = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - submit_time);
                    timer_jitter_us.record(static_cast<double>((actual - requested).count()));
                    timer_fired.fetch_add(1, std::memory_order_relaxed);
                });
                (void)pool->enqueue_until(FCS::Worker::timeout_mode::fast_lane, std::chrono::microseconds(300), [&] {
                    fast_fired.fetch_add(1, std::memory_order_relaxed);
                });
                (void)pool->enqueue_until(FCS::Worker::timeout_mode::slow_lane, std::chrono::microseconds(300), [&] {
                    slow_fired.fetch_add(1, std::memory_order_relaxed);
                });
                (void)pool->enqueue_until<timer_notify_tag>(FCS::Worker::timeout_mode::event_post, std::chrono::microseconds(300), [](std::uint64_t) {}, local_counter);
                posted.fetch_add(1, std::memory_order_relaxed);
                ++local_counter;
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        });
    }

    std::this_thread::sleep_for(duration);
    keep_going.store(false, std::memory_order_relaxed);
    for (auto& t : hammer_threads) t.join();

    // Let anything already in flight settle before reading final counts.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    print_worker_profiles(*pool);
    pool->stop();

    const auto jitter_count = timer_jitter_us.count.load();
    const double jitter_mean = jitter_count ? timer_jitter_us.sum_us.load() / static_cast<double>(jitter_count) : 0.0;

    std::printf("\n[timer-stress] threads=%zu duration=%lldms\n", threads, static_cast<long long>(duration.count()));
    std::printf("  timeout_mode::timer      fired : %llu (mean jitter %.1f us, max %.1f us)\n",
                static_cast<unsigned long long>(timer_fired.load()), jitter_mean, timer_jitter_us.max_us.load());
    std::printf("  timeout_mode::fast_lane  fired : %llu\n", static_cast<unsigned long long>(fast_fired.load()));
    std::printf("  timeout_mode::slow_lane  fired : %llu\n", static_cast<unsigned long long>(slow_fired.load()));
    std::printf("  timeout_mode::event_post fired : %llu (posted %llu, sum received %llu)\n",
                static_cast<unsigned long long>(notify_sum.load() > 0 ? posted.load() : posted.load()),
                static_cast<unsigned long long>(posted.load()), static_cast<unsigned long long>(notify_sum.load()));
}

// ---------------------------------------------------------------------
// phase 4: adversarial starvation stress -- a flood of high-priority work
// specifically shaped to try to starve normal-priority work, run at
// several age_starvation() configurations, reporting how long normal
// tasks actually had to wait under each one.
// ---------------------------------------------------------------------

void run_starvation_stress(std::size_t threads, std::uint64_t iterations, std::chrono::milliseconds duration) {
    struct config { const char* name; FCS::Worker::detail::aging_granularity clamp_when; std::uint64_t tasks_count; std::uint8_t base_growth; std::uint8_t max; };
    const config configs[] = {
        {"default (max=0, hard cutoff at 8)", FCS::Worker::detail::aging_granularity::task, 8, 1, 0},
        {"aggressive (tasks_count=2, max=0)", FCS::Worker::detail::aging_granularity::task, 2, 1, 0},
        {"gradual ramp (tasks_count=4, growth=1, max=8)", FCS::Worker::detail::aging_granularity::task, 4, 1, 8},
        {"segment-granularity (tasks_count=4, max=0)", FCS::Worker::detail::aging_granularity::segment, 4, 1, 0},
    };

    for (const auto& cfg : configs) {
        auto pool = std::make_unique<FCS::Worker::pool_service<>>();
        const tracked_pool tracked_{pool.get()};
        pool->concurrency(threads).age_starvation(cfg.clamp_when, cfg.tasks_count, cfg.base_growth, cfg.max).start();

        std::atomic_bool keep_flooding{true};
        std::atomic<std::uint64_t> high_priority_served{0};
        std::vector<std::thread> flood_threads;
        // Deliberately more flooders than workers, all pushing high
        // priority continuously -- as harsh a starvation attempt as this
        // API surface allows.
        for (std::size_t f = 0; f < threads * 2; ++f) {
            flood_threads.emplace_back([&] {
                while (keep_flooding.load(std::memory_order_relaxed)) {
                    // A rejected submit means the queue is already full -- the flood has done its
                    // job. Spinning on it only burns a core: with 2x flooders per worker on an
                    // oversubscribed box those spinners starved the *probe thread* (and any worker
                    // the OS had descheduled) for whole scheduler quanta, so "normal tasks served"
                    // measured the OS run queue, not the pool. Yield on reject; the queue stays
                    // saturated either way.
                    if (!pool->enqueue_priority(FCS::Worker::detail::task_priority::high, FCS::Worker::source_kind::synthetic, [&] {
                        (void)busy_work(iterations / 20); // short, so it churns through many high-priority items quickly
                        high_priority_served.fetch_add(1, std::memory_order_relaxed);
                    })) std::this_thread::yield();
                }
            });
        }

        // Normal-priority probes submitted at a steady, modest rate for
        // the whole duration -- what we actually care about is how long
        // each of these waits under the flood.
        running_stats normal_latency_us;
        std::atomic<std::uint64_t> normal_served{0};
        // The probe's enqueue() result used to be discarded, which hid the real story:
        // under a high-priority flood most probes were REJECTED AT ADMISSION and never
        // entered the queue, so the anti-starvation aging (which only reorders admitted
        // work) was never even given a chance. Count attempts vs admissions explicitly.
        std::uint64_t probes_attempted = 0, probes_admitted = 0;
        std::thread probe_thread([&] {
#if defined(_WIN32)
            // The probe is the measurement instrument, not the load: keep the OS from
            // delaying it behind the flooders for a 15 ms quantum.
            ::SetThreadPriority(::GetCurrentThread(), THREAD_PRIORITY_ABOVE_NORMAL);
#endif
            const auto probe_end = std::chrono::steady_clock::now() + duration;
            while (std::chrono::steady_clock::now() < probe_end) {
                const auto submit_time = std::chrono::steady_clock::now();
                ++probes_attempted;
                if (pool->enqueue(FCS::Worker::workload::Fast, [&, submit_time, iterations] {
                    (void)busy_work(iterations);
                    normal_latency_us.record(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - submit_time).count());
                    normal_served.fetch_add(1, std::memory_order_relaxed);
                })) ++probes_admitted;
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        });
        probe_thread.join();

        keep_flooding.store(false, std::memory_order_relaxed);
        for (auto& t : flood_threads) t.join();

        // Drain whatever's left so the next config starts clean.
        for (int spins = 0; spins < 5000 && pool->queued() > 0; ++spins) std::this_thread::sleep_for(std::chrono::milliseconds(1));

        const auto normal_count = normal_latency_us.count.load();
        const double normal_mean = normal_count ? normal_latency_us.sum_us.load() / static_cast<double>(normal_count) : 0.0;

        std::printf("\n[starvation-stress] config=\"%s\" threads=%zu flooders=%zu duration=%lldms\n",
                    cfg.name, threads, threads * 2, static_cast<long long>(duration.count()));
        std::printf("  high-priority tasks served : %llu\n", static_cast<unsigned long long>(high_priority_served.load()));
        // "expected" is the nominal 1 probe / 2 ms; the real attempt count (below) is lower
        // because sleep_for(2ms) takes longer than 2 ms, especially on Windows.
        std::printf("  normal tasks served        : %llu / nominal %lld\n",
                    static_cast<unsigned long long>(normal_served.load()), static_cast<long long>(duration.count() / 2));
        std::printf("  normal probes admitted     : %llu / %llu attempted (rejected at admission: %llu)\n",
                    static_cast<unsigned long long>(probes_admitted), static_cast<unsigned long long>(probes_attempted),
                    static_cast<unsigned long long>(probes_attempted - probes_admitted));
        std::printf("  normal latency mean/max (us): %.1f / %.1f\n", normal_mean, normal_latency_us.max_us.load());

        print_worker_profiles(*pool);
        pool->stop();
    }
}

// ---------------------------------------------------------------------
// phase 5: execution_policy comparison -- the same combined I/O + compute
// workload run under dedicated_poller, shared_worker(segment,1), and
// shared_worker(individual, threads) to compare how each handles a
// registered backend under heavy concurrent load.
// ---------------------------------------------------------------------

struct policy_tag {};

void run_execution_policy_stress(std::size_t threads, std::chrono::milliseconds duration) {
    struct config { const char* name; FCS::Worker::execution::policy policy; FCS::Worker::execution::task_type type; std::size_t between; };
    const config configs[] = {
        {"dedicated_poller", FCS::Worker::execution::dedicated_poller, FCS::Worker::execution::task_type::segment, 1},
        {"shared_worker(segment,1)", FCS::Worker::execution::shared_worker, FCS::Worker::execution::task_type::segment, 1},
        {"shared_worker(individual,threads)", FCS::Worker::execution::shared_worker, FCS::Worker::execution::task_type::individual, threads},
    };

    for (const auto& cfg : configs) {
        platform_guard guard;
        if (!guard.ok) continue;
        native_socket server{invalid_socket}, client{invalid_socket};
        if (!make_loopback_pair(server, client)) continue;

        auto pool = std::make_unique<FCS::Worker::pool_service<>>();

        const tracked_pool tracked_{pool.get()};
        pool->concurrency(threads).execution_policy(cfg.policy, cfg.type, cfg.between).start();
        FCS::Worker::eventlooper loop{*pool};
        loop.start();

        std::atomic<std::uint64_t> received{0};
        auto sub = loop.template subscribe<policy_tag, duplex_chunk>(
            client, chunk_reader{},
            [&](FCS::Worker::completion<duplex_chunk> c) {
                if (c.status == FCS::Worker::completion_status::ok) received.fetch_add(c.result.size, std::memory_order_relaxed);
            });

        std::atomic_bool keep_going{true};
        std::atomic<std::uint64_t> compute_sink{0};
        std::thread compute_feeder([&] {
            while (keep_going.load(std::memory_order_relaxed)) {
                (void)pool->enqueue(FCS::Worker::workload::Fast, [&] { compute_sink.fetch_add(busy_work(3000), std::memory_order_relaxed); });
                std::this_thread::yield();
            }
        });
        std::thread sender([&] {
            char payload[128];
            std::memset(payload, 'z', sizeof(payload));
            const auto sender_end = std::chrono::steady_clock::now() + duration;
            while (std::chrono::steady_clock::now() < sender_end) {
#if defined(_WIN32)
                ::send(server, payload, static_cast<int>(sizeof(payload)), 0);
#else
                (void)!::send(server, payload, sizeof(payload), 0);
#endif
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });

        const auto start = std::chrono::steady_clock::now();
        sender.join();
        keep_going.store(false, std::memory_order_relaxed);
        compute_feeder.join();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        const auto end = std::chrono::steady_clock::now();

        (void)sub.cancel();
        // Real fix, not a sleep: loop.stop() unregisters this backend's
        // poll hook and poll_registry::unregister() busy-waits for that
        // hook's in-flight poll_once() (if any) to finish first -- a real
        // happens-before edge, unlike sleep_for. Only then is it safe to
        // close the sockets. See run_full_duplex_stress()'s teardown for
        // the fuller version of this note.
        loop.stop();
        close_socket(client);
        close_socket(server);

        const double seconds = std::chrono::duration<double>(end - start).count();
        std::printf("\n[execution-policy-stress] config=\"%s\" threads=%zu duration=%.3fs\n", cfg.name, threads, seconds);
        std::printf("  bytes received     : %llu (%.0f bytes/s)\n",
                    static_cast<unsigned long long>(received.load()), static_cast<double>(received.load()) / seconds);
        std::printf("  background compute sink (anti-optimization, ignore value): %llu\n", static_cast<unsigned long long>(compute_sink.load()));

        print_worker_profiles(*pool);
        pool->stop();
    }
}


// ---------------------------------------------------------------------
// phase 6: registration churn under concurrent dispatch -- targets the
// epoll backend's registration lifecycle, which earlier phases barely touch:
//   * threads registering/cancelling their own fds nonstop while pollers
//     dispatch and reclaim (registration retire + free vs. user threads),
//   * one hot stream whose *reader keeps non-atomic state* (a race TSan sees
//     if two threads ever run one registration's handler at once, and whose
//     subscription must never die spuriously),
//   * full-duplex churn: subscribe_write()/cancel() cycles on an fd whose
//     read side stays registered, so the registration is REUSED and its
//     handler is replaced while a poller may still be executing the old one
//     (that was a reproducible heap-use-after-free).
// Readers/writers sleep a little while "inside" dispatch so that, even on one
// core, other threads genuinely run mid-call. Fails the run if the hot
// stream's subscription reports anything but ok.
// ---------------------------------------------------------------------

struct churn_hot_tag {};
struct churn_tag {};
struct churn_duplex_read_tag {};
struct churn_duplex_write_tag {};

struct stateful_reader {
    std::uint64_t seen{}; // deliberately non-atomic
    template<typename Source>
    FCS::Worker::read_result<duplex_chunk> operator()(Source& source) {
        FCS::Worker::read_result<duplex_chunk> result{};
        result.size = source.read({result.buffer.data(), result.buffer.size()});
        ++seen;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        return result;
    }
};

void run_registration_churn_stress(std::size_t threads, std::chrono::milliseconds duration) {
    platform_guard guard;
    if (!guard.ok) return;

    auto pool = std::make_unique<FCS::Worker::pool_service<>>();

    const tracked_pool tracked_{pool.get()};
    pool->concurrency(threads)
        .execution_policy(FCS::Worker::execution::shared_worker, FCS::Worker::execution::task_type::segment, threads)
        .start();
    FCS::Worker::eventlooper loop{*pool};
    loop.start();

    struct pair { native_socket a{invalid_socket}, b{invalid_socket}; };
    const auto make = [] { pair p; if (make_loopback_pair(p.a, p.b)) { set_nonblocking(p.a); set_nonblocking(p.b); } return p; };

    const pair hot = make();
    const pair duplex = make();
    constexpr std::size_t churners = 3, pairs_each = 8;
    std::vector<pair> churn_pairs;
    for (std::size_t i = 0; i < churners * pairs_each; ++i) churn_pairs.push_back(make());

    std::atomic<std::uint64_t> hot_ok{0}, hot_bad{0}, churn_cycles{0}, duplex_cycles{0};
    auto hot_sub = loop.template subscribe<churn_hot_tag, duplex_chunk>(hot.a, stateful_reader{},
        [&](FCS::Worker::completion<duplex_chunk> c) {
            (c.status == FCS::Worker::completion_status::ok ? hot_ok : hot_bad).fetch_add(1, std::memory_order_relaxed);
        });
    auto duplex_read = loop.template subscribe<churn_duplex_read_tag, duplex_chunk>(duplex.a, stateful_reader{},
        [](FCS::Worker::completion<duplex_chunk>) {});

    std::atomic_bool stop{false};
    std::thread feeder([&] {
        char buf[32];
        std::memset(buf, 'f', sizeof(buf));
        const auto feed = [&](native_socket s) {
#if defined(_WIN32)
            ::send(s, buf, static_cast<int>(sizeof(buf)), 0);
#else
            (void)!::send(s, buf, sizeof(buf), 0);
#endif
        };
        while (!stop.load(std::memory_order_relaxed)) {
            feed(hot.b);
            feed(duplex.b);
            for (auto& p : churn_pairs) feed(p.b);
            std::this_thread::yield();
        }
    });

    std::thread duplex_churn([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto w = loop.template subscribe_write<churn_duplex_write_tag, duplex_chunk>(
                FCS::Worker::source_ref<native_socket>{duplex.a, FCS::Worker::source_kind::network},
                [](FCS::Worker::backend::detail::readiness_sink& sink) -> std::size_t {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                    const char b = 'w';
                    return sink.write(std::as_bytes(std::span{&b, 1}));
                },
                [](FCS::Worker::completion<duplex_chunk>) {});
            std::this_thread::yield();
            (void)w.cancel();
            duplex_cycles.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::vector<std::thread> churn_threads;
    for (std::size_t t = 0; t < churners; ++t) {
        churn_threads.emplace_back([&, t] {
            while (!stop.load(std::memory_order_relaxed)) {
                for (std::size_t k = 0; k < pairs_each; ++k) {
                    auto sub = loop.template subscribe<churn_tag, duplex_chunk>(churn_pairs[t * pairs_each + k].a, stateful_reader{},
                        [](FCS::Worker::completion<duplex_chunk>) {});
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
    (void)duplex_read.cancel();
    loop.stop(); // before closing any fd -- see run_full_duplex_stress()

    for (auto& p : churn_pairs) { close_socket(p.a); close_socket(p.b); }
    close_socket(hot.a); close_socket(hot.b);
    close_socket(duplex.a); close_socket(duplex.b);

    print_worker_profiles(*pool);
    pool->stop();

    std::printf("\n[registration-churn-stress] threads=%zu duration=%lldms\n", threads, static_cast<long long>(duration.count()));
    std::printf("  register/cancel cycles      : %llu (own fds) + %llu (full-duplex, registration reused)\n",
                static_cast<unsigned long long>(churn_cycles.load()), static_cast<unsigned long long>(duplex_cycles.load()));
    std::printf("  hot stream completions ok   : %llu\n", static_cast<unsigned long long>(hot_ok.load()));
    std::printf("  hot stream unexpected errors: %llu %s\n", static_cast<unsigned long long>(hot_bad.load()),
                hot_bad.load() == 0 ? "(ok)" : "<-- FAIL: subscription died or errored while nothing cancelled it");
    if (hot_bad.load() != 0) g_failures.fetch_add(1);
}

} // namespace stressor

int main(int argc, char** argv) {
    const std::size_t threads = argc > 1 ? static_cast<std::size_t>(std::stoul(argv[1]))
                                          : std::max<std::size_t>(2, std::thread::hardware_concurrency() * 2);
    const std::size_t compute_tasks = argc > 2 ? static_cast<std::size_t>(std::stoul(argv[2])) : 400000;
    const std::uint64_t iterations = argc > 3 ? static_cast<std::uint64_t>(std::stoull(argv[3])) : 20000;
    const auto duration = std::chrono::milliseconds(argc > 4 ? std::stoul(argv[4]) : 4000);

#if FCS_WORKER_BACKEND_IOCP
    std::printf("FCS-Workers pool_stressor (backend: IOCP)\n");
#elif FCS_WORKER_BACKEND_EPOLL
    std::printf("FCS-Workers pool_stressor (backend: epoll)\n");
#elif FCS_WORKER_BACKEND_KQUEUE
    std::printf("FCS-Workers pool_stressor (backend: kqueue)\n");
#else
    std::printf("FCS-Workers pool_stressor (backend: io_uring)\n");
#endif
    std::printf("threads=%zu compute_tasks=%zu iterations/task=%llu duration=%lldms\n",
                threads, compute_tasks, static_cast<unsigned long long>(iterations), static_cast<long long>(duration.count()));

    // MSVC rejects getenv() as "unsafe" (C4996); _dupenv_s is its sanctioned spelling.
    double hang_seconds = 120.0;
#if defined(_MSC_VER)
    {
        char* value = nullptr; std::size_t length = 0;
        if (_dupenv_s(&value, &length, "FCS_STRESS_HANG_SECONDS") == 0 && value) { hang_seconds = std::atof(value); std::free(value); }
    }
#else
    if (const char* value = std::getenv("FCS_STRESS_HANG_SECONDS")) hang_seconds = std::atof(value);
#endif
#if defined(_WIN32)
    // Windows' default timer tick is ~15.6 ms: every sleep_for(1ms) in the harness and every
    // timed park in the pool would otherwise really be 1-16 ms, which dominates the timer /
    // I/O-phase numbers (jitter, bytes/s) and says nothing about the backend itself.
    ::timeBeginPeriod(1);
#endif
    stressor::enter_phase("compute");
    std::thread([hang_seconds] {
        for (;;) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            const auto started = stressor::g_phase_started_ns.load();
            if (started == 0) continue;
            const double age = std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch() - std::chrono::steady_clock::duration{started}).count();
            if (age < hang_seconds) continue;
            std::fprintf(stderr, "\n*** HANG: phase \"%s\" has run %.0fs (limit %.0fs, FCS_STRESS_HANG_SECONDS) -- scheduler state:\n",
                         stressor::g_phase_name.load(), age, hang_seconds);
            for (auto& slot : stressor::g_live_pools) if (auto* p = slot.load()) p->debug_dump(stderr);
            std::fflush(stderr);
            std::_Exit(3);
        }
    }).detach();

    stressor::run_compute_stress(threads, compute_tasks, iterations);
    stressor::enter_phase("full-duplex");
    stressor::run_full_duplex_stress(threads, /*pair_count=*/64, duration);
    stressor::enter_phase("timer");
    stressor::run_timer_stress(threads, duration);
    stressor::enter_phase("starvation");
    stressor::run_starvation_stress(threads, iterations, duration);
    stressor::enter_phase("execution-policy");
    stressor::run_execution_policy_stress(threads, duration);
    stressor::enter_phase("registration-churn");
    stressor::run_registration_churn_stress(threads, duration);
    stressor::g_phase_started_ns.store(0);

    if (stressor::g_failures.load() != 0) {
        std::printf("\nSTRESS RUN FAILED: %d phase invariant(s) violated (see FAIL lines above)\n", stressor::g_failures.load());
        return 1;
    }
    std::printf("\nALL STRESS PHASES COMPLETE\n");
    return 0;
}
