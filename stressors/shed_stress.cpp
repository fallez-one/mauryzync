// Stress test for the total-stall poll + runtime shed (FCS_EXPERIMENTAL_ALWAYS_ON). Meant to be unfair.
//
//   fcs_workers_shed_stress [mode] [seed] [rounds]      mode: all (default) | storm | chain | pause
//
// storm  In-process. A resurrector whose clone ALWAYS fails after a random delay, so every shed is
//        entered and then rolled back, over and over, while several producer threads hammer
//        enqueue() (high/normal priority, both lanes, fan-out children) and "poison" tasks wedge
//        random subsets of workers for random lengths -- shorter than the poll window (must NOT shed),
//        longer (must). Every accepted task id must run exactly once; telemetry must add up.
//
// chain  REAL fork, several generations. Each run is a throw-away process tree: hard-poison tasks
//        wedge every worker (some while holding a mutex that nobody will ever unlock), the watchdog
//        clones the process, the clone's workers pick up the migrated backlog -- and wedge again on
//        the next batch of poisons -- until the final generation, where poison is harmless. Producers
//        in generation 0 keep submitting THROUGH the shed. A ledger written to a pipe from every
//        process checks, across all of them: no task starts twice, no accepted task is lost, no task
//        that merely sat in a queue is stranded, and every hook found exactly one thread.
//
// pause  REAL fork machinery, healthy workload, and the whole process is SIGSTOPped/SIGCONTed at
//        random: a pool that was only descheduled (a paused VM, a debugger, a stopped container)
//        must NOT be resurrected.
//
// Every violation prints the seed; rerun with it. POSIX only.
#include <FCS/Worker/workers.hpp>
#include <FCS/Worker/sync/interruptible_mutex.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <dirent.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#if !FCS_EXPERIMENTAL_ALWAYS_ON
#  error "build this with FCS_EXPERIMENTAL_ALWAYS_ON=1"
#endif

namespace {
using namespace std::chrono_literals;
namespace W = FCS::Worker;
namespace D = FCS::Worker::detail;
using clock_type = std::chrono::steady_clock;

int g_violations = 0;
[[nodiscard]] inline unsigned long long ull(std::uint64_t v) noexcept { return static_cast<unsigned long long>(v); }
[[nodiscard]] inline long long ll(long long v) noexcept { return v; }
#define VIOLATION(...) do { ++g_violations; std::printf("  VIOLATION %s:%d: ", __FILE__, __LINE__); std::printf(__VA_ARGS__); std::printf("\n"); } while (0)

template<typename Pred>
bool wait_for(Pred&& pred, std::chrono::milliseconds limit) {
    const auto end = clock_type::now() + limit;
    while (!pred()) {
        if (clock_type::now() >= end) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

// ------------------------------------------------------------------ task kinds
enum class kind : std::uint8_t { normal, poison_short, poison_hard };

[[nodiscard]] kind pick_kind(std::mt19937& rng, std::uint64_t id) {
    if (id % 97 == 13) return kind::poison_hard;     // steady supply: every generation needs a fresh batch
    const auto roll = rng() % 60;
    if (roll == 0) return kind::poison_short;
    return kind::normal;
}

// ====================================================================== storm (in-process)
struct always_fails final : D::process_resurrect<always_fails> {
    static inline std::atomic<unsigned> seed{1};
    static inline std::atomic<int> calls{0};
    [[nodiscard]] FCS::Expected<D::fork_role, D::resurrect_error> try_fork_impl() const noexcept {
        calls.fetch_add(1);
        std::mt19937 r{seed.fetch_add(1)};
        std::this_thread::sleep_for(std::chrono::microseconds{r() % 25'000});   // sometimes instant, sometimes slow
        return FCS::Unexpected<D::resurrect_error>{12u};
    }
};
struct storm_traits : W::default_pool_traits { using resurrector = always_fails; };
using storm_pool = W::pool_service<1024, storm_traits>;

constexpr std::size_t kLedger = 400'000;

bool run_storm(unsigned seed, int round) {
    std::mt19937 rng{seed};
    const std::size_t workers = 2 + rng() % 5;
    const auto window = std::chrono::milliseconds{40 + rng() % 80};
    const std::size_t quorum = 1 + rng() % workers;
    const int producers = 2 + static_cast<int>(rng() % 3);
    const int per_producer = 400 + static_cast<int>(rng() % 500);
    always_fails::seed = seed; always_fails::calls = 0;

    std::printf("storm %d: seed=%u workers=%zu window=%lldms quorum=%zu producers=%d x %d tasks\n", round, seed, workers,
                static_cast<long long>(window.count()), quorum, producers, per_producer);

    storm_pool pool;
    pool.concurrency(workers).execution_policy(W::execution::dedicated_poller)
        .watchdog_interval(1ms).watchdog_mark_stall(5ms).watchdog_total_stall_poll(window, quorum);
    pool.start();

    std::vector<std::atomic<std::uint8_t>> accepted(kLedger), ran(kLedger);
    std::atomic<std::uint64_t> next_id{1}, accepted_total{0}, ran_total{0}, refused_by_shed{0};
    std::atomic<bool> producers_done{false};

    // The task body. Fan-out: some tasks enqueue a child from inside a worker (a worker that is
    // mid-shed gets refused and simply drops the child: it was never accepted, so it is not owed).
    std::function<bool(std::uint64_t, kind, std::uint32_t)> submit_one;
    auto body = [&](std::uint64_t id, kind k, std::uint32_t salt) {
        if (k == kind::poison_short) std::this_thread::sleep_for(std::chrono::milliseconds{3 + salt % 25});
        if (k == kind::poison_hard) std::this_thread::sleep_for(std::chrono::milliseconds{window.count() / 2 + salt % (window.count() * 3)});
        ran[id].fetch_add(1);
        ran_total.fetch_add(1);
        if (salt % 11 == 0 && next_id.load() < kLedger - 4096) {
            const auto child = next_id.fetch_add(1);
            if (!submit_one(child, kind::normal, salt / 11)) { /* refused: not accepted, not owed */ }
        }
    };
    submit_one = [&](std::uint64_t id, kind k, std::uint32_t salt) -> bool {
        auto fn = [&body, id, k, salt] { body(id, k, salt); };
        bool ok;
        switch (salt % 4) {
            case 0: ok = pool.enqueue(fn); break;
            case 1: ok = pool.enqueue(W::workload::Slow, fn); break;
            case 2: ok = pool.enqueue_priority(D::task_priority::high, W::source_kind::synthetic, fn); break;
            default: ok = pool.enqueue(W::workload::Fast, fn); break;
        }
        if (ok) { accepted[id].store(1); accepted_total.fetch_add(1); }
        return ok;
    };

    std::vector<std::thread> threads;
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&, p] {
            std::mt19937 local{seed * 7919u + static_cast<unsigned>(p)};
            for (int i = 0; i < per_producer; ++i) {
                const auto id = next_id.fetch_add(1);
                if (id >= kLedger - 4096) return;
                const auto k = pick_kind(local, id);
                const auto salt = static_cast<std::uint32_t>(local());
                // Unfair: retry the SAME task through every refusal (a shed, a full cushion), no backoff beyond a yield.
                for (int attempt = 0; !submit_one(id, k, salt); ++attempt) {
                    refused_by_shed.fetch_add(1, std::memory_order_relaxed);
                    if (attempt % 64 == 63) std::this_thread::sleep_for(1ms); else std::this_thread::yield();
                }
            }
        });
    }
    for (auto& t : threads) t.join();
    producers_done = true;

    // Everything accepted must finish. Progress watchdog: a wedge in the pool that never lifts is a failure, not a hang.
    std::uint64_t last = 0; auto last_move = clock_type::now();
    bool ok = true;
    while (ran_total.load() < accepted_total.load()) {
        if (ran_total.load() != last) { last = ran_total.load(); last_move = clock_type::now(); }
        if (clock_type::now() - last_move > 15s) { VIOLATION("seed %u: stuck: %llu accepted, %llu ran", seed, ull(accepted_total.load()), ull(ran_total.load())); ok = false; break; }
        std::this_thread::sleep_for(2ms);
    }
    std::this_thread::sleep_for(150ms);   // let a duplicate show up

    std::uint64_t lost = 0, dup = 0, ghost = 0;
    const auto top = std::min<std::uint64_t>(next_id.load(), kLedger);
    for (std::uint64_t id = 1; id < top; ++id) {
        const auto a = accepted[id].load(), r = ran[id].load();
        if (a && r == 0) ++lost;
        if (r > 1) ++dup;
        if (!a && r) ++ghost;
    }
    const auto st = pool.resurrection_stats();
    std::printf("   accepted=%llu  refused-retries=%llu  clone attempts=%d | polls=%llu cleared=%llu sheds=%llu aborted=%llu | evac=%llu reinj=%llu refused=%llu unreachable=%llu submits_refused=%llu\n",
                ull(accepted_total.load()), ull(refused_by_shed.load()), always_fails::calls.load(),
                ull(st.polls_opened), ull(st.polls_cleared), ull(st.sheds_started),
                ull(st.sheds_aborted), ull(st.tasks_evacuated), ull(st.tasks_reinjected),
                ull(st.tasks_refused), ull(st.tasks_unreachable), ull(st.submits_refused));
    if (lost) VIOLATION("seed %u: %llu accepted task(s) NEVER ran", seed, ull(lost));
    if (dup) VIOLATION("seed %u: %llu task(s) ran MORE THAN ONCE", seed, ull(dup));
    if (ghost) VIOLATION("seed %u: %llu task(s) ran that were never accepted", seed, ull(ghost));
    if (st.sheds_started != st.sheds_aborted) VIOLATION("seed %u: sheds_started %llu != sheds_aborted %llu (every clone here fails)", seed, ull(st.sheds_started), ull(st.sheds_aborted));
    if (st.polls_opened != st.polls_cleared + st.sheds_started) VIOLATION("seed %u: polls don't add up", seed);
    if (st.tasks_evacuated != st.tasks_reinjected + st.tasks_refused) VIOLATION("seed %u: evacuated %llu != reinjected %llu + refused %llu", seed, ull(st.tasks_evacuated), ull(st.tasks_reinjected), ull(st.tasks_refused));
    if (st.tasks_refused) VIOLATION("seed %u: %llu task(s) had nowhere to go on rollback", seed, ull(st.tasks_refused));
    if (st.tasks_unreachable) VIOLATION("seed %u: sweep left %llu task(s) behind (gauge)", seed, ull(st.tasks_unreachable));
    if (st.resurrections || st.generation) VIOLATION("seed %u: a stand-in clone that always fails reported a resurrection", seed);
    if (pool.shedding_migration()) VIOLATION("seed %u: still shedding at the end", seed);

    pool.stop();
    return ok && g_violations == 0;
}

// ====================================================================== chain / pause (real fork)
struct wire { std::uint32_t kind; std::uint32_t pad; std::uint64_t a, b, c; };
enum : std::uint32_t { w_acc = 1, w_start = 2, w_done = 3, w_hook = 4, w_final = 5, w_stats = 6, w_polls = 7 };
int g_fd = -1;
void send(std::uint32_t k, std::uint64_t a = 0, std::uint64_t b = 0, std::uint64_t c = 0) {
    const wire w{k, 0, a, b, c};
    const auto n = ::write(g_fd, &w, sizeof w);
    (void)n;
}
std::size_t threads_here() {
    std::size_t n = 0;
    if (DIR* d = ::opendir("/proc/self/task")) { while (const auto* e = ::readdir(d)) if (e->d_name[0] != '.') ++n; ::closedir(d); }
    return n;
}

using fork_pool = W::pool_service<>;
FCS::synchronization::interruptible_mutex g_mutex;

struct scenario_cfg {
    unsigned seed{}; std::size_t workers{}; std::chrono::milliseconds window{}; std::size_t quorum{};
    std::uint64_t target_gen{}; int producers{}; int per_producer{}; bool hard_poison{}; bool pause_mode{};
};

// Runs in the first process of the tree and never returns.
[[noreturn]] void tree_root(const scenario_cfg cfg) {
    static fork_pool pool;
    static std::atomic<std::uint64_t> next_id{1};
    static std::atomic<bool> submit_done{false};
    static scenario_cfg c; c = cfg;

    static auto body = [](std::uint64_t id, kind k, std::uint32_t salt) {
        send(w_start, id, static_cast<std::uint64_t>(::getpid()));
        const auto gen = pool.resurrection_stats().generation;
        if (k == kind::poison_hard && gen < c.target_gen) {
            if (salt % 2 == 0) g_mutex.lock();                       // wedge WHILE HOLDING it: nobody will ever unlock it
            for (;;) std::this_thread::sleep_for(1s);
        }
        // Pause mode: "healthy but slow" tasks (15-45 ms, far over the 8 ms stall tolerance), so that most of the
        // time EVERY worker is flagged stalled and a poll is almost always open when a SIGSTOP lands.
        if (k == kind::poison_short) std::this_thread::sleep_for(std::chrono::milliseconds{c.pause_mode ? 15 + salt % 30 : 2 + salt % 12});
        if (salt % 37 == 0) { g_mutex.lock(); g_mutex.unlock(); }    // ordinary traffic that needs it
        send(w_done, id, static_cast<std::uint64_t>(::getpid()));
    };

    // Final generation's way out: the clone has no main thread, so a re-arming timer task watches for "idle for good".
    static std::function<void()> idle_check;
    static std::atomic<int> idle_rounds{0};
    idle_check = [] {
        const auto st = pool.resurrection_stats();
        const auto m = pool.scheduler_metadata();
        const bool idle = m.cushion_depth + m.mpmc_depth + m.local_depth == 0 && m.active <= 1;
        // The producer threads do not exist in a clone: whatever they had not submitted yet was never accepted, so a clone is "done submitting".
        if (st.generation >= c.target_gen && (submit_done.load() || st.generation > 0) && idle) {
            if (idle_rounds.fetch_add(1) + 1 >= 6) {
                send(w_final, st.generation, st.resurrections, static_cast<std::uint64_t>(::getpid()));
                send(w_stats, st.tasks_evacuated, st.tasks_reinjected, st.tasks_refused | (st.tasks_unreachable << 32));
                send(w_polls, st.polls_opened, st.polls_cleared, st.sheds_started | (st.polls_extended << 32));
                ::_exit(0);
            }
        } else idle_rounds.store(0);
        (void)pool.enqueue_until(W::timeout_mode::timer, 60ms, [] { idle_check(); });
    };

    pool.concurrency(c.workers).execution_policy(W::execution::dedicated_poller)
        .watchdog_interval(1ms).watchdog_mark_stall(8ms)
        .watchdog_total_stall_poll(c.window, c.quorum)
        .watchdog_finalize_wait(400ms)
        .watchdog_on_migrate([](fork_pool::migration_context& ctx) {
            send(w_hook, static_cast<std::uint64_t>(::getpid()), static_cast<std::uint64_t>(::getppid()), threads_here());
            if (g_mutex.locking()) g_mutex.unlock();                  // repair what a vanished worker held
            (void)ctx;
        });
    pool.start();
    (void)pool.enqueue_until(W::timeout_mode::timer, 60ms, [] { idle_check(); });

    std::vector<std::thread> producers;
    for (int p = 0; p < c.producers; ++p) {
        producers.emplace_back([p] {
            std::mt19937 rng{c.seed * 104729u + static_cast<unsigned>(p)};
            for (int i = 0; i < c.per_producer; ++i) {
                const auto id = next_id.fetch_add(1);
                auto k = c.hard_poison ? pick_kind(rng, id) : (rng() % (c.pause_mode ? 2u : 40u) == 0 ? kind::poison_short : kind::normal);
                if (!c.hard_poison && k == kind::poison_hard) k = kind::normal;
                const auto salt = static_cast<std::uint32_t>(rng());
                for (;;) {
                    auto fn = [id, k, salt] { body(id, k, salt); };
                    const bool ok = (salt % 3 == 0) ? pool.enqueue_priority(D::task_priority::high, W::source_kind::synthetic, fn)
                                  : (salt % 3 == 1) ? pool.enqueue(W::workload::Slow, fn) : pool.enqueue(fn);
                    if (ok) { send(w_acc, id); break; }
                    std::this_thread::sleep_for(500us);   // refused: a shed is running, or the cushion is full
                }
                if (c.pause_mode && i % 16 == 0) std::this_thread::sleep_for(2ms);
            }
        });
    }
    for (auto& t : producers) t.join();   // in a shed this thread is simply gone in the clone
    submit_done.store(true);
    std::this_thread::sleep_for(90s);
    ::_exit(9);                           // nothing ended the tree
}

struct ledger {
    std::unordered_map<std::uint64_t, int> acc, starts, dones;
    std::unordered_map<std::uint64_t, std::set<std::uint64_t>> start_pids;
    std::set<std::uint64_t> pids, resurrected_parents, hook_pids;
    int hooks{0}; std::uint64_t final_gen{~0ull}; bool final_seen{false}; bool bad_thread_count{false};
    std::uint64_t evac{0}, reinj{0}, refused{0}, unreach{0}, polls{0}, cleared{0}, sheds{0}, extended{0};
};

bool run_tree(const scenario_cfg& cfg, bool pause_signals, const char* label) {
    std::printf("%s: seed=%u workers=%zu window=%lldms quorum=%zu target_gen=%llu producers=%d x %d%s\n", label, cfg.seed, cfg.workers,
                static_cast<long long>(cfg.window.count()), cfg.quorum, static_cast<unsigned long long>(cfg.target_gen), cfg.producers, cfg.per_producer,
                pause_signals ? "  (+ random SIGSTOP/SIGCONT)" : "");
    std::fflush(stdout);
    int fds[2];
    if (::pipe(fds) != 0) { VIOLATION("pipe"); return false; }
    const pid_t root = ::fork();
    if (root < 0) { VIOLATION("fork"); return false; }
    if (root == 0) { ::close(fds[0]); g_fd = fds[1]; tree_root(cfg); }
    ::close(fds[1]);

    ledger L;
    std::vector<unsigned char> buf;
    std::mt19937 rng{cfg.seed ^ 0x9e3779b9u};
    const auto start = clock_type::now();
    auto next_pause = start + std::chrono::milliseconds{300 + rng() % 500};
    int pauses = 0; bool stopped = false; auto resume_at = start;
    bool timed_out = false;
    const auto budget = 90s;

    for (;;) {
        const auto now = clock_type::now();
        if (now - start > budget) { timed_out = true; break; }
        if (pause_signals) {
            if (!stopped && now >= next_pause && pauses < 10) { ::kill(root, SIGSTOP); stopped = true; ++pauses; resume_at = now + std::chrono::milliseconds{120 + rng() % 380}; }
            else if (stopped && now >= resume_at) { ::kill(root, SIGCONT); stopped = false; next_pause = now + std::chrono::milliseconds{15 + rng() % 120}; }
        }
        pollfd p{fds[0], POLLIN, 0};
        const int r = ::poll(&p, 1, 20);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) break;
        if (r == 0) continue;
        unsigned char tmp[4096 * 8];
        const auto n = ::read(fds[0], tmp, sizeof tmp);
        if (n == 0) break;                         // EOF: every process of the tree has closed its end
        if (n < 0) { if (errno == EINTR) continue; break; }
        buf.insert(buf.end(), tmp, tmp + n);
        std::size_t off = 0;
        while (buf.size() - off >= sizeof(wire)) {
            wire w; std::memcpy(&w, buf.data() + off, sizeof w); off += sizeof w;
            switch (w.kind) {
                case w_acc: ++L.acc[w.a]; break;
                case w_start: ++L.starts[w.a]; L.start_pids[w.a].insert(w.b); L.pids.insert(w.b); break;
                case w_done: ++L.dones[w.a]; break;
                case w_hook: ++L.hooks; L.hook_pids.insert(w.a); L.pids.insert(w.a); L.resurrected_parents.insert(w.b); if (w.c != 1) L.bad_thread_count = true; break;
                case w_final: L.final_seen = true; L.final_gen = w.a; L.pids.insert(w.c); break;
                case w_polls: L.polls = w.a; L.cleared = w.b; L.sheds = w.c & 0xffffffffu; L.extended = w.c >> 32; break;
                case w_stats: L.evac = w.a; L.reinj = w.b; L.refused = w.c & 0xffffffffu; L.unreach = w.c >> 32; break;
            }
        }
        buf.erase(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(off));
    }
    if (stopped) ::kill(root, SIGCONT);
    ::close(fds[0]);
    if (timed_out) { for (const auto pid : L.pids) ::kill(static_cast<pid_t>(pid), SIGKILL); ::kill(root, SIGKILL); }
    int status = 0; (void)::waitpid(root, &status, 0);
    for (const auto pid : L.pids) ::kill(static_cast<pid_t>(pid), SIGKILL);   // belt and braces: no strays left behind

    const int before = g_violations;
    if (timed_out) VIOLATION("seed %u: the tree never finished (%zu accepted, %zu started, %zu done, %d hook(s))", cfg.seed, L.acc.size(), L.starts.size(), L.dones.size(), L.hooks);
    std::size_t lost = 0, dup_start = 0, dup_done = 0, stuck_unexplained = 0, never_started = 0, ghost = 0, stuck_explained = 0;
    for (const auto& [id, n] : L.acc) {
        (void)n;
        const auto s = L.starts.count(id) ? L.starts[id] : 0;
        const auto d = L.dones.count(id) ? L.dones[id] : 0;
        if (s == 0) { ++never_started; ++lost; continue; }
        if (s > 1) ++dup_start;
        if (d > 1) ++dup_done;
        if (d == 0) {
            // Started, never finished: fine ONLY if every process that started it was resurrected (it was
            // in flight, inside a worker that got left behind) -- or it was a hard poison. Anything else is a strand.
            bool explained = true;
            for (const auto pid : L.start_pids[id]) if (!L.resurrected_parents.count(pid)) explained = false;
            if (explained) ++stuck_explained; else ++stuck_unexplained;
        }
    }
    for (const auto& [id, n] : L.starts) { (void)n; if (!L.acc.count(id)) ++ghost; }
    std::printf("   accepted=%zu started=%zu done=%zu | resurrections(hooks)=%d final_gen=%llu | in-flight-lost (by design)=%zu\n", L.acc.size(), L.starts.size(),
                L.dones.size(), L.hooks, static_cast<unsigned long long>(L.final_gen), stuck_explained);
    if (never_started) VIOLATION("seed %u: %zu accepted task(s) NEVER STARTED anywhere (lost in a shed)", cfg.seed, never_started);
    if (dup_start) VIOLATION("seed %u: %zu task(s) STARTED MORE THAN ONCE (duplicated across a clone)", cfg.seed, dup_start);
    if (dup_done) VIOLATION("seed %u: %zu task(s) completed more than once", cfg.seed, dup_done);
    if (stuck_unexplained) VIOLATION("seed %u: %zu task(s) started and never finished in a process that was NOT resurrected (stranded)", cfg.seed, stuck_unexplained);
    if (ghost) VIOLATION("seed %u: %zu task(s) started that were never recorded as accepted (harness race, informational)", cfg.seed, ghost);
    if (L.bad_thread_count) VIOLATION("seed %u: a migration hook saw more than one thread: a clone must have exactly one", cfg.seed);
    if (!L.final_seen && !timed_out) VIOLATION("seed %u: no final record: the last generation never went idle", cfg.seed);
    if (pause_signals) std::printf("   telemetry of the surviving process: polls opened=%llu cleared(false alarms)=%llu extended(overslept)=%llu sheds=%llu\n",
                                   static_cast<unsigned long long>(L.polls), static_cast<unsigned long long>(L.cleared), static_cast<unsigned long long>(L.extended), static_cast<unsigned long long>(L.sheds));
    if (pause_signals) {
        if (L.hooks != 0) VIOLATION("seed %u: a pool that was only PAUSED was resurrected %d time(s)", cfg.seed, L.hooks);
    } else {
        if (static_cast<std::uint64_t>(L.hooks) < cfg.target_gen) VIOLATION("seed %u: expected >= %llu resurrection(s), saw %d", cfg.seed, static_cast<unsigned long long>(cfg.target_gen), L.hooks);
        if (L.final_seen && L.final_gen < cfg.target_gen) VIOLATION("seed %u: final generation %llu < target", cfg.seed, static_cast<unsigned long long>(L.final_gen));
        if (L.final_seen && L.refused) VIOLATION("seed %u: %llu task(s) were refused on re-injection", cfg.seed, static_cast<unsigned long long>(L.refused));
        if (L.final_seen && L.unreach) VIOLATION("seed %u: %llu task(s) unreachable to a sweep", cfg.seed, static_cast<unsigned long long>(L.unreach));
    }
    return g_violations == before;
}

} // namespace

int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "all";
    const unsigned base_seed = argc > 2 ? static_cast<unsigned>(std::strtoul(argv[2], nullptr, 10)) : 20260711u;
    const int rounds = argc > 3 ? std::atoi(argv[3]) : 6;
    std::signal(SIGPIPE, SIG_IGN);
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    if (mode == "all" || mode == "storm")
        for (int r = 0; r < rounds; ++r) run_storm(base_seed + static_cast<unsigned>(r) * 101u, r);

    if (mode == "all" || mode == "chain") {
        for (int r = 0; r < std::max(2, rounds / 2); ++r) {
            std::mt19937 rng{base_seed + static_cast<unsigned>(r) * 977u};
            scenario_cfg c;
            c.seed = base_seed + static_cast<unsigned>(r);
            c.workers = 2 + rng() % 4;
            c.window = std::chrono::milliseconds{40 + rng() % 60};
            c.quorum = 1 + rng() % c.workers;
            c.target_gen = 1 + rng() % 3;
            c.producers = 2 + static_cast<int>(rng() % 2);
            c.per_producer = 500 + static_cast<int>(rng() % 700);
            c.hard_poison = true;
            run_tree(c, false, "chain");
        }
    }

    if (mode == "all" || mode == "pause") {
        for (int r = 0; r < std::max(2, rounds / 3); ++r) {
            std::mt19937 rng{base_seed + 555u + static_cast<unsigned>(r) * 31u};
            scenario_cfg c;
            c.seed = base_seed + 555u + static_cast<unsigned>(r);
            c.workers = 2 + rng() % 4;
            c.window = std::chrono::milliseconds{50 + rng() % 40};
            c.quorum = 1;
            c.target_gen = 0;
            c.producers = 2;
            c.per_producer = 350;
            c.hard_poison = false;
            c.pause_mode = true;
            run_tree(c, true, "pause");
        }
    }

    std::printf(g_violations ? "shed_stress: %d VIOLATION(S)\n" : "shed_stress: all invariants held\n", g_violations);
    return g_violations ? 1 : 0;
}
