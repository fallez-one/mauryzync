// epoll::backend ownership regression: a registration's destructor runs user
// code (the callback's captures, the Reader/Writer's captures). That code may
// legitimately own *another* subscription of the same backend, so destroying it
// calls back into cancel_*_slot(). The backend must therefore never run those
// destructors while holding a lock that cancel_*_slot()/register_*() take.
//
// Before the fix reclaim() freed registrations under slots_mutex_, so the
// nested cancel locked the same non-recursive mutex on the same thread and the
// poller hung forever. Each scenario below gets a watchdog; a hang is a failure.
#include <FCS/Worker/workers.hpp>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

namespace W = FCS::Worker;
using namespace std::chrono_literals;

struct tag_a {}; struct tag_b {}; struct tag_c {}; struct tag_d {};
struct tag_e {}; struct tag_f {}; struct tag_g {}; struct tag_h {}; struct tag_i {};  // channels broadcast: one tag per live registration
constexpr std::size_t chunk = 64;

static void make_pair(int fds[2]) {
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) std::abort();
    for (int i = 0; i < 2; ++i) ::fcntl(fds[i], F_SETFL, ::fcntl(fds[i], F_GETFL) | O_NONBLOCK);
}

struct reader {
    W::read_result<chunk> operator()(W::backend::detail::readiness_source<>& src) const {
        W::read_result<chunk> r{};
        r.size = src.read({r.buffer.data(), r.buffer.size()});
        return r;
    }
};

// Owns a subscription of the same backend; its destructor cancels it.
struct holder {
    W::subscription sub;
    std::atomic<int>* destroyed{};
    explicit holder(std::atomic<int>* counter) : destroyed(counter) {}
    ~holder() { (void)sub.cancel(); destroyed->fetch_add(1); }
};

static std::atomic_bool g_done{false};

static void watchdog(const char* what) {
    std::thread([what] {
        for (int i = 0; i < 100 && !g_done.load(); ++i) std::this_thread::sleep_for(50ms);
        if (!g_done.load()) { std::printf("FAIL: deadlock in '%s'\n", what); std::fflush(stdout); std::_Exit(3); }
    }).detach();
}

// cancel() is asynchronous (the driver does the epoll_ctl): like backend_contract.cpp,
// fds are only closed once the backend has been stopped.
static std::vector<int>& deferred() { static std::vector<int> v; return v; }
static void close_later(int fd) { deferred().push_back(fd); }
static void flush_closes() { for (int fd : deferred()) ::close(fd); deferred().clear(); }

static bool wait_for(auto&& pred, std::chrono::milliseconds limit = 3000ms) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < end) { if (pred()) return true; std::this_thread::sleep_for(1ms); }
    return pred();
}

static int run(W::execution::policy policy, const char* name) {
    std::printf("== %s\n", name);
    W::pool_service<> service;
    service.concurrency(3).execution_policy(policy).start();
    W::eventlooper loop{service};
    loop.start();

    int a[2], b[2], c[2], d[2];
    make_pair(a); make_pair(b); make_pair(c); make_pair(d);
    std::atomic<int> destroyed{0};

    // 1. Cancelled registration whose callback owns another subscription.
    //    cancel() -> reclaim() frees registration A -> ~callback -> ~holder -> cancel(B).
    {
        g_done = false; watchdog("callback owns a subscription (cancel path)");
        auto h = std::make_shared<holder>(&destroyed);
        h->sub = loop.subscribe<tag_b, chunk>(W::source_ref<int>{b[0], W::source_kind::io}, reader{}, [](W::completion<chunk>) {});
        auto sub_a = loop.subscribe<tag_a, chunk>(W::source_ref<int>{a[0], W::source_kind::io}, reader{},
                                                  [h = std::move(h)](W::completion<chunk>) {});
        if (!sub_a.cancel()) { std::printf("FAIL: cancel returned false\n"); return 1; }
        if (!wait_for([&] { return destroyed.load() == 1; })) { std::printf("FAIL: holder never destroyed\n"); return 1; }
        g_done = true;
    }

    // 2. The backend must still be fully usable afterwards (nothing left locked),
    //    including re-registering the fd whose owner just went away.
    {
        g_done = false; watchdog("register after reclaim");
        std::atomic<int> got{0};
        auto s = loop.subscribe<tag_c, chunk>(W::source_ref<int>{b[0], W::source_kind::io}, reader{},
                                              [&](W::completion<chunk> r) { if (r.status == W::completion_status::ok) got.fetch_add(1); });
        if (!s.active()) { std::printf("FAIL: re-register on b failed\n"); return 1; }
        if (::send(b[1], "x", 1, 0) != 1) return 1;
        if (!wait_for([&] { return got.load() == 1; })) { std::printf("FAIL: no delivery after reclaim\n"); return 1; }
        (void)s.cancel();
        g_done = true;
    }

    // 3. Same thing through the Reader's captures: the handler is freed with the
    //    registration, and its Reader owns a subscription.
    {
        g_done = false; watchdog("reader owns a subscription (cancel path)");
        destroyed = 0;
        auto h = std::make_shared<holder>(&destroyed);
        h->sub = loop.subscribe<tag_d, chunk>(W::source_ref<int>{d[0], W::source_kind::io}, reader{}, [](W::completion<chunk>) {});
        auto owning_reader = [h = std::move(h)](W::backend::detail::readiness_source<>& src) {
            W::read_result<chunk> r{};
            r.size = src.read({r.buffer.data(), r.buffer.size()});
            return r;
        };
        auto sub_c = loop.subscribe<tag_c, chunk>(W::source_ref<int>{c[0], W::source_kind::io}, std::move(owning_reader), [](W::completion<chunk>) {});
        if (!sub_c.cancel()) { std::printf("FAIL: cancel returned false (3)\n"); return 1; }
        if (!wait_for([&] { return destroyed.load() == 1; })) { std::printf("FAIL: reader owner never destroyed\n"); return 1; }
        g_done = true;
    }

    // 4. epoll_ctl() runs on the driver: a refused fd is reported through the
    //    callback as exactly one error completion, never silently dropped.
    {
        g_done = false; watchdog("arm failure report");
        std::atomic<int> errors{0}, other{0};
        const int regular = ::open("/proc/self/exe", O_RDONLY | O_CLOEXEC); // epoll can't watch regular files (EPERM)
        auto cb = [&](W::completion<chunk> r) { (r.status == W::completion_status::error ? errors : other).fetch_add(1); };
        auto s1 = loop.subscribe<tag_e, chunk>(W::source_ref<int>{regular, W::source_kind::io}, reader{}, cb);
        auto s2 = loop.subscribe<tag_f, chunk>(W::source_ref<int>{-1, W::source_kind::io}, reader{}, cb);
        if (!wait_for([&] { return errors.load() == 2; })) { std::printf("FAIL: arm failures not reported (%d)\n", errors.load()); return 1; }
        std::this_thread::sleep_for(50ms);
        if (errors.load() != 2 || other.load() != 0) { std::printf("FAIL: arm failure delivered more than once\n"); return 1; }
        if (s1.cancel() || s2.cancel()) { std::printf("FAIL: failed registration still cancellable\n"); return 1; }
        close_later(regular);
        g_done = true;
    }

    // 5. Registering a direction that is already registered replaces it: only the
    //    newest delivers, and the replaced handle is dead.
    {
        g_done = false; watchdog("replace semantics");
        int e[2]; make_pair(e);
        std::atomic<int> old_hits{0}, new_hits{0};
        auto older = loop.subscribe<tag_g, chunk>(W::source_ref<int>{e[0], W::source_kind::io}, reader{},
                                                  [&](W::completion<chunk>) { old_hits.fetch_add(1); });
        auto newer = loop.subscribe<tag_h, chunk>(W::source_ref<int>{e[0], W::source_kind::io}, reader{},
                                                  [&](W::completion<chunk> r) { if (r.status == W::completion_status::ok) new_hits.fetch_add(1); });
        // Full duplex alongside, registered from another thread at the same time.
        std::atomic<int> sink_hits{0};
        W::subscription sink;
        std::thread t([&] {
            sink = loop.subscribe_write<tag_i, chunk>(W::source_ref<int>{e[0], W::source_kind::io},
                [](W::backend::detail::readiness_sink&) -> std::size_t { return 0; },
                [&](W::completion<chunk>) { sink_hits.fetch_add(1); });
        });
        t.join();
        if (::send(e[1], "x", 1, 0) != 1) return 1;
        if (!wait_for([&] { return new_hits.load() >= 1 && sink_hits.load() >= 1; })) { std::printf("FAIL: replacement/duplex not delivering\n"); return 1; }
        if (older.cancel()) { std::printf("FAIL: replaced registration still cancellable\n"); return 1; }
        if (old_hits.load() != 0) { std::printf("FAIL: replaced registration delivered\n"); return 1; }
        if (!newer.cancel() || !sink.cancel()) { std::printf("FAIL: live registrations not cancellable\n"); return 1; }
        close_later(e[0]); close_later(e[1]);
        g_done = true;
    }

    // 6. Stopped, then cancelled: nothing pending may leak or crash (ASan/LSan).
    {
        g_done = false; watchdog("cancel after stop");
        int f[2]; make_pair(f);
        auto s = loop.subscribe<tag_d, chunk>(W::source_ref<int>{f[0], W::source_kind::io}, reader{}, [](W::completion<chunk>) {});
        loop.stop();
        (void)s.cancel();
        auto late = loop.subscribe<tag_d, chunk>(W::source_ref<int>{f[0], W::source_kind::io}, reader{}, [](W::completion<chunk>) {});
        (void)late;                       // registered while stopped: freed by ~backend, never armed
        close_later(f[0]); close_later(f[1]);
        g_done = true;
    }

    loop.stop();
    service.stop();
    for (int fd : {a[0], a[1], b[0], b[1], c[0], c[1], d[0], d[1]}) close_later(fd);
    flush_closes();
    return 0;
}

int main() {
    if (run(W::execution::policy::dedicated_poller, "dedicated_poller")) return 1;
    if (run(W::execution::policy::shared_worker, "shared_worker")) return 1;
    std::printf("reclaim_reentry: all passed\n");
    return 0;
}