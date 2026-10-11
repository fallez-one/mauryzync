#pragma once

#include "experimental.hpp"
#include "async_completed_result.hpp"
#include "detail/admission_gate.hpp"
#include "detail/channel.hpp"
#include "detail/cpu_relax.hpp"
#include "detail/mpmc_queue.hpp"
#include "detail/parking_lot.hpp"
#include "detail/poll_registry.hpp"
#include "detail/priority_mpmc_queue.hpp"
#include "detail/scheduled_result.hpp"
#include "detail/resurrection_metrics.hpp"
#include "detail/shed_migration.hpp"
#include "detail/scheduler_metrics.hpp"
#include "detail/task.hpp"
#include "detail/timer_heap.hpp"
#include "pool_traits.hpp"
#include "detail/worker_registry.hpp"
#include "coroutine.hpp"
#include "cooperative.hpp"
#include "execution.hpp"
#include "types.hpp"
#if FCS_EXPERIMENTAL_ALWAYS_ON
#  include "detail/process_resurrect.hpp"
#endif

#include <atomic>
#include <cstdio>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

namespace FCS::Worker {

    // Detects "cb(args...) returns FCS::async_result<T>" for
    // enqueue_and_poll()'s auto-poll path (see its declaration below);
    // async_result_value<X>::type is only defined when X is some
    // async_result<T>, so it doubles as the requires-clause check.
    template<typename T> struct async_result_value;
    template<typename T> struct async_result_value<FCS::async_result<T>> { using type = T; };
    template<typename T> using async_result_value_t = typename async_result_value<T>::type;

    // enqueue_and_poll()'s actual R: async_result_value_t<X> when cb returns
    // some async_result<T> (X itself, otherwise -- a plain returned value).
    // SFINAE via void_t, not std::conditional_t: the latter would eagerly
    // try to instantiate async_result_value<X>::type even for a plain X,
    // which is ill-formed rather than merely unused.
    template<typename X, typename = void>
    struct enqueue_and_poll_result { using type = X; };
    template<typename X>
    struct enqueue_and_poll_result<X, std::void_t<typename async_result_value<X>::type>> { using type = typename async_result_value<X>::type; };
    template<typename X> using enqueue_and_poll_result_t = typename enqueue_and_poll_result<X>::type;

    template<std::size_t QueueCapacity = 1024, typename Traits = default_pool_traits>
    class pool_service {
        static_assert(QueueCapacity >= 8 && (QueueCapacity & (QueueCapacity - 1)) == 0);

        static constexpr std::size_t slow_capacity = QueueCapacity / 4;
        static constexpr std::size_t cushion_capacity = QueueCapacity * 2;
        static constexpr std::size_t local_capacity = QueueCapacity * 2;
        static constexpr std::size_t max_workers = 64;

        // The parking primitive comes from Traits and is checked against its concept here -- at the
        // point a concrete pool is instantiated, not in the headers.
        using semaphore_type = typename Traits::semaphore;
        // Every task this pool queues is exactly Traits::task_bytes of inline storage, wherever it sits.
        using task_type = detail::task_for<Traits::task_bytes>;
        using queued_task_type = detail::basic_queued_task<Traits::task_bytes>;
        using gate_type = detail::admission_gate<cushion_capacity, slow_capacity, queued_task_type>;
        static_assert(BinarySemaphore<semaphore_type>, "Traits::semaphore must model FCS::Worker::BinarySemaphore");
        using registry_type = detail::worker_registry<local_capacity, max_workers, queued_task_type>;
        using fast_queue_type = detail::priority_mpmc_queue<queued_task_type, QueueCapacity>;

    public:
        static constexpr std::size_t fast_batch_size = 64;
        static constexpr std::size_t slow_batch_size = 8;
        static constexpr std::size_t minimum_steal_depth = 7;

        pool_service() = default;
        pool_service(const pool_service&) = delete;
        pool_service& operator=(const pool_service&) = delete;
        ~pool_service();

        pool_service& concurrency(std::size_t count) noexcept;
        // Stall watchdog -- a small, non-intrusive monitor (one extra thread,
        // no central scheduler) for workers held up by a long-running task. A
        // worker stuck in a task cannot work through its own local deque, yet
        // thieves normally skip any victim below minimum_steal_depth; a stalled
        // victim is instead fair game at any depth. Covers the pool's own
        // workers under both execution policies (a dedicated_poller thread has
        // no deque to rescue; the tasks it feeds still run on pool workers).
        //
        //  * watchdog_interval(): how often the monitor looks (default 5ms).
        //    0 turns it off. Call before start() to enable/disable; afterwards a
        //    non-zero value just retunes the cadence.
        //  * watchdog_mark_stall(): how long one task (or one shared_worker poll
        //    pass, or one timer callback) may run before its worker is marked
        //    stalled (default 10ms). Safe to change anytime. Detection lands
        //    between `tolerance` and `tolerance + interval` after the task began.
        //
        // Workers never read a clock for this: they only bump a per-worker
        // progress counter (see worker_registry::scan_stalls).
        // Cooperative coroutines. A coroutine driven by enqueue_and_poll() that reaches a
        // checkpoint (co_await FCS::Worker::yield_point(), or any plain suspension) while the
        // watchdog has flagged its worker gives the thread back: its continuation goes into
        // that worker's OWN deque, `behind` tasks back from the bottom (the end the owner pops),
        // so the worker runs that many other tasks first -- 1 is "swap with the next one", the
        // default 3 lets a few go by. Nobody's consent is needed (thieves can still take it from
        // the head if the owner stays stalled) and nothing unbounded is involved: if the deque
        // is full the continuation goes through normal admission, and if that refuses too the
        // coroutine simply keeps running. There is no priority demotion. Clamped to
        // [1, worker_registry::max_requeue_behind].
        pool_service& coroutine_yield_depth(std::uint32_t behind) noexcept {
            coop_yield_depth_.store(behind == 0 ? 1 : behind, std::memory_order_relaxed);
            return *this;
        }

        pool_service& watchdog_interval(std::chrono::nanoseconds interval = std::chrono::milliseconds{5}) noexcept;
        pool_service& watchdog_mark_stall(std::chrono::nanoseconds tolerance = std::chrono::milliseconds{10}) noexcept;

        // ---- total stall: poll, then shed (experimental; FCS_EXPERIMENTAL_ALWAYS_ON) ----------
        //
        // Rescuing hostage work from ONE wedged worker is the stall watchdog's job (above). This
        // is for when EVERY worker is stalled at once and nobody is left to rescue anything: the
        // pool can't tell "all descheduled" (starved of CPU, a stopped VM, a debugger) from "all
        // truly wedged" (a deadlock, a task that never returns).
        //
        //   watchdog_total_stall_poll(window, quorum)
        //     When the watchdog sees every worker stalled it asks for proof of life: it needs
        //     `quorum` DISTINCT workers to make progress (finish or start a task) within
        //     `window` -- chrono literals work: (500ms, 2), (3s, 1). That many do: it was
        //     descheduling, the watchdog stands down and nothing happens. Fewer: it escalates to
        //     a RUNTIME SHED. `window` <= 0 disables the whole thing. Arms at start() (that is
        //     when the evacuation array is allocated -- not while already wedged); `window` and
        //     `quorum` can be retuned afterwards.
        //
        //   Runtime shed -- the point of no return:
        //     1. the shedding_migration() flag goes up. A worker that was merely descheduled and
        //        wakes up late sees it, touches nothing, and parks until stop() joins it. For a
        //        truly wedged worker the flag is irrelevant: it never looks. From now on
        //        submit() refuses (enqueue() returns false): work accepted now would be lost.
        //     2. every queue -- each worker's deque, the fast and slow lanes, the cushion and its
        //        refill staging -- is MOVED (not copied) into one bounded 2-D array (see
        //        detail/shed_migration.hpp).
        //     3. the process is cloned (fork / RtlCloneUserProcess; Traits::resurrector, the
        //        native one by default). Only the watchdog thread exists in the clone.
        //     4. In the CLONE: the pool's runtime state is rebuilt, the hook below runs, the
        //        tasks are re-injected -- straight into the worker's own deque where possible,
        //        else through the cushion, else straight into the fast/slow lane -- and fresh
        //        workers start. The cloned thread carries on as the new pool's watchdog.
        //        In the PARENT: sleep watchdog_finalize_wait(), then resurrector.exit().
        //     If the clone itself fails the shed is undone: tasks go back in through the shared
        //     queues, the flag drops, the stood-down workers resume.
        //
        //   watchdog_on_migrate(fn)    fn(migration_context&), runs in the clone, see above. Before start().
        //   watchdog_finalize_wait(ms) how long the parent lingers after cloning before it exits (default 0).
        //
        // Disabled build (FCS_EXPERIMENTAL_ALWAYS_ON=0): all three exist and do nothing.
        using migration_context = detail::migration_context<queued_task_type>;
        using migration_hook = detail::inline_function<void(migration_context&), Traits::callback_bytes>;
#if FCS_EXPERIMENTAL_ALWAYS_ON
        pool_service& watchdog_total_stall_poll(std::chrono::nanoseconds window, std::size_t quorum) noexcept;
        pool_service& watchdog_finalize_wait(std::chrono::milliseconds wait) noexcept;
        template<typename F>
        pool_service& watchdog_on_migrate(F&& fn);
        // True from the point of no return until the parent exits (or a failed clone is undone).
        [[nodiscard]] bool shedding_migration() const noexcept { return shedding_migration_.load(std::memory_order_acquire); }
        // What the last shed evacuated / re-injected. Read it when the pool is quiet (in the hook,
        // or after a failed clone): the shedding thread writes it without synchronisation.
        [[nodiscard]] detail::migration_report last_migration_report() const noexcept { return shed_report_; }
        // Telemetry of polls, sheds and resurrections -- see detail/resurrection_metrics.hpp.
        [[nodiscard]] detail::resurrection_snapshot resurrection_stats() const noexcept { return resurrection_.snapshot(); }
#else
        pool_service& watchdog_total_stall_poll(std::chrono::nanoseconds, std::size_t) noexcept { return *this; }
        pool_service& watchdog_finalize_wait(std::chrono::milliseconds) noexcept { return *this; }
        template<typename F>
        pool_service& watchdog_on_migrate(F&&) noexcept { return *this; }
        [[nodiscard]] bool shedding_migration() const noexcept { return false; }
        [[nodiscard]] detail::migration_report last_migration_report() const noexcept { return {}; }
        [[nodiscard]] detail::resurrection_snapshot resurrection_stats() const noexcept { return {}; }
#endif

        // Steal-sizing idle estimate: how many steal attempts may reuse one
        // O(threads) scan before it is refreshed (default max(threads, 4); 0 =
        // always rescan). Takes effect only before start().
        pool_service& idle_cache_ttl(std::size_t uses) noexcept;
        pool_service& burst_backpressure(std::size_t full = cushion_capacity - 1, std::size_t available = QueueCapacity / 4, std::size_t candidates = 80) noexcept;

        // Chooses how a native-multiplexer backend (epoll/io_uring/IOCP)
        // built on this service polls for readiness -- see execution.hpp.
        // Only takes effect if called before start(); defaults to
        // FCS_WORKER_DEFAULT_EXECUTION_POLICY (execution::shared_worker
        // unless overridden). Backends read this once, in start_backend(),
        // so changing it after a backend has already started has no effect
        // on that backend.
        pool_service& execution_policy(execution::policy policy) noexcept;

        // `type`/`between_num_threads` shape how shared_worker-policy
        // workers drive poll_registry_ -- see execution::task_type and
        // poll_registry::poll_once(). Ignored for dedicated_poller, which
        // already blocks in one specific backend's native wait call on its
        // own committed OS thread instead. `between_num_threads` caps how
        // many workers concurrently attempt a poll pass at once (0 is
        // treated as 1); the rest skip polling that round rather than all
        // piling onto poll_registry_'s per-hook `busy` CAS together.
        pool_service& execution_policy(execution::policy policy, execution::task_type type, std::size_t between_num_threads) noexcept;
        [[nodiscard]] execution::policy execution_policy() const noexcept;

        // How long a shared_worker-policy worker sleeps between passes over
        // the registered backend multiplexer(s) (poll_registry_) when
        // there's nothing else to do -- replaces the previously-hardcoded
        // 1ms. dedicated_poller ignores this: that backend blocks in its
        // own OS wait call on its own thread instead of being polled here.
        pool_service& timeout_tolerate(std::chrono::nanoseconds tolerate) noexcept;

        // Tunes fast_'s anti-starvation aging -- see detail::starvation_policy
        // for what each knob does and its defaults (which reproduce the
        // queue's original fixed-cutoff behavior). Safe to call anytime;
        // takes effect on the next try_pop() after it lands.
        // How many admission slots (of the cushion's full threshold) are kept for
        // normal-priority work: high-priority submits are rejected `slots` short of
        // full, so a high-priority flood cannot fill the cushion and reject every
        // normal submit at the door. Default max(1, full/16); 0 = no reserve.
        pool_service& normal_admission_reserve(std::size_t slots) noexcept { gate_.reserve_for_normal(slots); return *this; }

        pool_service& age_starvation(detail::aging_granularity clamp_when, std::uint64_t tasks_count, std::uint8_t base_growth, std::uint8_t max) noexcept;

        // Registration point for execution::shared_worker backends -- see
        // detail/poll_registry.hpp. Not usually called directly; backends
        // do this themselves in start_backend()/stop_backend(). Wakes any
        // worker already parked (workers only wait indefinitely rather than
        // with a bounded timeout when poll_registry_ was empty at the time
        // they last checked -- see worker_loop()), so a hook registered
        // just after that check doesn't sit unpolled until something
        // unrelated happens to wake the pool up.
        [[nodiscard]] std::size_t register_poll_hook(void* context, detail::poll_registry::poll_fn fn) noexcept {
            const auto handle = poll_registry_.register_hook(context, fn);
            signal_all();
            return handle;
        }
        void unregister_poll_hook(std::size_t handle) noexcept {
            poll_registry_.unregister(handle);
            signal_all();
        }

        void start();
        void stop() noexcept;

        template<typename F, typename... Args>
        [[nodiscard]] bool enqueue(workload lane, source_kind source, F&& fn, Args&&... args);
        template<typename F, typename... Args>
        [[nodiscard]] bool enqueue(workload lane, F&& fn, Args&&... args) { return enqueue(lane, source_kind::synthetic, std::forward<F>(fn), std::forward<Args>(args)...); }
        template<typename F, typename... Args>
        [[nodiscard]] bool enqueue(F&& fn, Args&&... args) { return enqueue(workload::Fast, source_kind::synthetic, std::forward<F>(fn), std::forward<Args>(args)...); }

        // Same as enqueue(), but marks the task detail::task_priority::high in
        // the fast lane -- see detail/priority_mpmc_queue.hpp. Always
        // workload::Fast: priority is meaningless for the slow lane.
        template<typename F, typename... Args>
        [[nodiscard]] bool enqueue_priority(detail::task_priority priority, source_kind source, F&& fn, Args&&... args);

        // Fires once, timeout from now, via an internal min-heap checked
        // whenever a worker wakes (see timeout_tolerate() and
        // time_until_next_timer()) -- no OS timer of its own, but a
        // pending deadline still shortens a parked worker's wait the same
        // way a registered backend does. `mode` picks how the firing is
        // dispatched -- see timeout_mode in types.hpp. For every mode but
        // event_post, `cb(args...)` runs on a worker (or inline, for
        // event_post with no Tag). For event_post, `cb` is unused and
        // `Tag` (required) instead names an existing subscribe<Tag>()
        // channel that args... is delivered to via post_from() on the
        // firing thread -- args... must match that subscriber's parameter
        // types, same as any other post_from() call. No cancellation once
        // scheduled.
        template<typename Tag = void, typename Callback, typename... Args>
        bool enqueue_until(timeout_mode mode, std::chrono::nanoseconds timeout, Callback&& cb, Args&&... args);

        template<typename Tag, typename F>
        [[nodiscard]] subscription subscribe(F&& fn);
        template<typename Tag, std::size_t Size, typename Reader, typename Source, typename Callback>
            requires reader_callable<Size, Reader, std::remove_cvref_t<Source>>
        [[nodiscard]] subscription subscribe(Source&&, Callback&& cb) { return subscribe<Tag>(std::forward<Callback>(cb)); }

        template<typename Tag, typename... Args>
            requires (std::copy_constructible<std::decay_t<Args>> && ...)
        [[nodiscard]] std::size_t post(Args&&... args) { return post_from<Tag>(source_kind::synthetic, std::forward<Args>(args)...); }
        template<typename Tag, typename... Args>
            requires (std::copy_constructible<std::decay_t<Args>> && ...)
        [[nodiscard]] std::size_t post_from(source_kind source, Args&&... args);

        // Runs cb(args...) on a worker and hands back a co_await-able
        // async_completed_result<T, R> (T = decayed callback type, R =
        // std::invoke_result_t<T, Args...>) carrying the result plus which
        // worker produced it -- see async_completed_result.hpp. cb must
        // return a real value: R == void is a compile error, since
        // user_data() would have nothing to expose. `cb` follows the usual
        // forwarding-reference rule -- an lvalue is copied in, an rvalue is
        // moved in -- same as enqueue()'s callable.
        //
        // If cb(args...) returns FCS::async_result<T> instead of a plain
        // value, it's auto-polled: the coroutine is created, driven to
        // completion via repeated resume(), and destroyed entirely within
        // this one worker's execution -- it never crosses threads, which is
        // the only safe way to drive an async_result<T> (see coroutine.hpp
        // notes on pull_result<T>/chunk_stream<T> for the standalone,
        // single-thread-owned alternative). R is then async_result<T>'s T,
        // and a coroutine that threw surfaces as error::callback_threw,
        // same as a plain callback throwing.
        template<typename Callback, typename... Args>
        [[nodiscard]] auto enqueue_and_poll(workload lane, Callback&& cb, Args&&... args);

        [[nodiscard]] scheduler_snapshot scheduler_metadata() const noexcept;
        [[nodiscard]] std::size_t queued() const noexcept;

        // One-shot, lock-free snapshot of scheduler state for hang diagnosis: running
        // flag, queue depths, wake epoch / sleeper count, and per worker whether it is
        // parked, inside a task, flagged stalled, and how much it holds locally. Safe
        // to call from any thread (including a monitor while the pool is wedged).
        void debug_dump(std::FILE* out = stderr) const noexcept;

        // Per-worker steal telemetry (incoming/outgoing attempts, items stolen,
        // batch vs. single-item breakdowns). `id` must be < concurrency().
        [[nodiscard]] detail::worker_steal_snapshot worker_profile(std::size_t id) const noexcept;
        [[nodiscard]] std::size_t worker_count() const noexcept;

    private:
        template<typename Tag, typename C>
        static C& channel() { static C instance; return instance; }

        [[nodiscard]] bool submit(queued_task_type work);
        // Bypasses the cushion entirely, pushing straight into fast_ --
        // used only by enqueue_until()'s timeout_mode::timer path, where
        // going through the cushion's async refill() would blunt the whole
        // point of a deadline-driven wakeup.
        [[nodiscard]] bool submit_direct(queued_task_type work);
        bool refill();
        bool drain_global(std::size_t owner);
        void execute(queued_task_type& work);
        void worker_loop(std::size_t id);
        [[nodiscard]] bool submit_requeue(queued_task_type&& work) noexcept; // normal admission; false = refused
        void signal_one() noexcept;   // new work: wake at most one sleeping worker
        void signal_all() noexcept;   // pool shape changed: every sleeper re-evaluates
        void watchdog_loop();
        void launch_workers();
        void launch_watchdog();

#if FCS_EXPERIMENTAL_ALWAYS_ON
        // Total-stall shed -- all defined in pool_service_shed.inl.
        struct submit_flight {
            explicit submit_flight(std::atomic<std::uint32_t>& n) noexcept : count(n) { count.fetch_add(1, std::memory_order_seq_cst); }
            ~submit_flight() { count.fetch_sub(1, std::memory_order_release); }
            submit_flight(const submit_flight&) = delete;
            submit_flight& operator=(const submit_flight&) = delete;
            std::atomic<std::uint32_t>& count;
        };
        [[nodiscard]] bool total_stall_armed() const noexcept { return shed_batch_ != nullptr && shed_window_ns_.load(std::memory_order_relaxed) > 0; }
        void escalate_total_stall();                // watchdog thread: poll, then (maybe) shed. Does not return in the parent of a successful clone.
        void await_stand_down();
        void await_submitters();
        void evacuate_all();
        void reinject_all(bool direct_local);
        void abort_shed();                          // the clone failed: undo
        void resume_in_clone(std::uint64_t shed_ns) noexcept;            // the clone: rebuild, hook, re-inject, restart workers
        void stop_after_commit() noexcept;
        [[nodiscard]] bool shed_stand_down(std::size_t id);   // worker: false == pool is stopping, leave the loop
#endif

        // Pops and runs one due entry from timers_, if any; false if none
        // was due. time_until_next_timer() is the read-only counterpart
        // worker_loop() uses to size its wait when nothing else is due.
        bool try_fire_due_timers(std::size_t id);
        [[nodiscard]] std::optional<std::chrono::nanoseconds> time_until_next_timer() const;

        // The fixed-size bulk of the scheduler -- every queue slot, the admission cushion, the
        // timer table -- lives in ONE heap block allocated when the pool is constructed. Its size
        // is still a compile-time constant (nothing here ever grows), but with inline tasks it is
        // several megabytes, which does not belong inside an object that a user may well declare
        // as a local: it would overflow a Windows main-thread stack (1-2 MB) or a worker
        // thread's. The references below keep every `fast_` / `slow_` / `gate_` / `timers_` use
        // in this class unchanged.
        struct bulk_storage {
            fast_queue_type fast;
            detail::bounded_mpmc_queue<queued_task_type, slow_capacity> slow;
            gate_type gate;
            detail::timer_heap<Traits::timer_capacity, task_type> timers;
        };
        std::unique_ptr<bulk_storage> bulk_{std::make_unique<bulk_storage>()};
        fast_queue_type& fast_{bulk_->fast};
        detail::bounded_mpmc_queue<queued_task_type, slow_capacity>& slow_{bulk_->slow};
        gate_type& gate_{bulk_->gate};
        registry_type registry_;
        detail::scheduler_metrics metrics_;
        detail::poll_registry poll_registry_;

        std::atomic_bool running_{};
        std::atomic<execution::policy> execution_policy_{static_cast<execution::policy>(FCS_WORKER_DEFAULT_EXECUTION_POLICY)};
        std::atomic<execution::task_type> poll_task_type_{execution::task_type::segment};
        std::atomic_size_t poll_thread_limit_{1};
        std::atomic_size_t active_pollers_{};
        std::atomic<std::int64_t> poll_tolerance_ns_{1'000'000}; // 1ms default; see timeout_tolerate()
        std::atomic<std::uint64_t> next_sequence_{}; // enqueue_and_poll()'s async_completed_result::sequence() source
        std::size_t threads_{1};
        // Parked workers: lock-free (semaphore per worker), wakes targeted. See parking_lot.hpp.
        detail::parking_lot<max_workers, semaphore_type> parking_;
        std::atomic<std::uint32_t> coop_yield_depth_{3};

        // Control block of one enqueue_and_poll() coroutine: owns the callable, its arguments and the
        // coroutine frame, so the coroutine can outlive the task that started it and be resumed by
        // a different task (cooperative yield) or after an external await. Intrusively refcounted:
        // one reference per queued step, per pending trampoline, and one for the step running now.
        template<typename Async, typename Payload, typename Fn, typename Args>
        struct poll_control {
            poll_control(pool_service* owner, workload start_lane, detail::intrusive_ptr<detail::scheduled_state<Payload>> result_cell, Fn fn, Args args)
                : pool(owner), lane(start_lane), producer(std::move(result_cell)), callable(std::move(fn)), values(std::move(args)) {}

            pool_service* pool;
            workload lane;
            detail::intrusive_ptr<detail::scheduled_state<Payload>> producer;
            Fn callable;      // must outlive the coroutine: a lambda coroutine's captures live here, not in its frame
            Args values;
            std::optional<Async> coroutine;
            std::atomic<std::uint32_t> refs{1};

            static void retain(void* p) noexcept { static_cast<poll_control*>(p)->refs.fetch_add(1, std::memory_order_relaxed); }
            static void release(void* p) noexcept {
                auto* c = static_cast<poll_control*>(p);
                if (c->refs.fetch_sub(1, std::memory_order_acq_rel) == 1) delete c;
            }

            // Queues the coroutine's next resumption (takes its own reference for the queued step).
            // false: nothing could take it. Every path here is bounded.
            static bool reschedule(void* p, bool forced_yield) noexcept {
                auto* c = static_cast<poll_control*>(p);
                retain(c);
                const auto make = [c] {
                    return queued_task_type{c->lane, source_kind::synthetic, task_type{[c] { step(c); }}};
                };
                if (forced_yield) {
                    auto& view = detail::tls_worker_view;
                    // Same pool, on one of its workers: this thread owns that worker's deque, so
                    // the continuation can go straight into it, a few tasks back. No queue, no
                    // admission, no waking anyone.
                    if (view.owner == c->pool) {
                        try {
                            if (c->pool->registry_.requeue_behind(view.id, make(), c->pool->coop_yield_depth_.load(std::memory_order_relaxed), c->pool->metrics_)) {
                                view.yielded = true;
                                if (view.profile) view.profile->record_coop_yield();
                                return true;
                            }
                        } catch (...) {}
                        if (view.profile) view.profile->record_coop_global_requeue();
                    }
                    if (c->pool->submit_requeue(make())) {
                        view.yielded = true;
                        if (view.profile) view.profile->record_coop_yield();
                        return true;
                    }
                    release(c);
                    return false;
                }
                if (c->pool->submit_requeue(make())) return true;
                release(c);
                return false;
            }

            static void resume_inline(void* p) noexcept { step(static_cast<poll_control*>(p)); }

            // Runs at the coroutine's final suspend point, on whichever thread finished it.
            static void complete(void* p) noexcept {
                auto* c = static_cast<poll_control*>(p);
                std::optional<Payload> payload;
                try { payload.emplace(Payload{c->coroutine->result(), std::this_thread::get_id()}); }
                catch (...) { payload.emplace(Payload{Unexpected{error::callback_threw}, {}}); }
                c->producer->set_value(std::move(*payload));
            }

            // One pool task: resume the coroutine until it hands itself off, finishes, or yields.
            // Consumes one reference.
            static void step(poll_control* c) noexcept {
                detail::step_frame frame;
                auto* const outer = detail::tls_step_frame;   // nested when a full queue forces an inline resume
                detail::tls_step_frame = &frame;
                for (;;) {
                    frame.handed_off = false;
                    c->coroutine->handle.resume();
                    // Re-queued, parked on an external awaitable, or finished: it is someone else's
                    // now and must not be touched. Only this stack flag is trusted, never the frame.
                    if (frame.handed_off) break;
                    // A plain suspension (e.g. `co_await std::suspend_always{}`): the driver's job
                    // is to resume it. That is a checkpoint -- unless the watchdog says give the
                    // thread back, resume right away as before.
                    if (detail::yield_requested() && reschedule(c, /*forced_yield=*/true)) break;
                }
                detail::tls_step_frame = outer;
                release(c);
            }
        };
        std::vector<std::thread> workers_;

        // Stall watchdog (see watchdog_interval()). Sleeps on its own parker so
        // that waking it never rides on the workers' wakeups.
        std::atomic<std::int64_t> watchdog_interval_ns_{5'000'000};
        std::atomic<std::int64_t> watchdog_tolerance_ns_{10'000'000};
        std::atomic_bool watchdog_stop_{};
        std::atomic_bool watchdog_parked_{};
        detail::parker<semaphore_type> watchdog_parker_;
        std::thread watchdog_;

#if FCS_EXPERIMENTAL_ALWAYS_ON
        // Total-stall shed state. Configuration is atomic (retunable anytime); the hook is plain
        // (set before start()); the rest belongs to the watchdog thread, then to the clone.
        using resurrector_type = detail::resurrector_of_t<Traits>;
        std::atomic<std::int64_t> shed_window_ns_{0};
        std::atomic<std::size_t> shed_quorum_{1};
        std::atomic<std::int64_t> shed_finalize_ms_{0};
        std::atomic_bool shedding_migration_{};     // the flag: set at the point of no return
        std::atomic_bool migration_committed_{};    // parent only: the clone exists, this process is on its way out
        std::array<std::atomic_bool, max_workers> shed_acked_{};   // worker i has seen the flag and stood down
        // submit() callers between "checked the flag" and "pushed": the shed waits for them to drain
        // before it sweeps, or a task could land in a layer already swept.
        std::atomic<std::uint32_t> submits_in_flight_{0};
        detail::resurrection_metrics resurrection_;
        migration_hook migrate_hook_;
        std::unique_ptr<detail::migration_batch<queued_task_type>> shed_batch_;   // allocated at start() when armed
        detail::migration_report shed_report_{};
        bool shed_holds_drain_{false};
        bool shed_holds_refill_{false};
        resurrector_type resurrector_{};
#endif

        // Not on the hot submit path (that's fast_/slow_/cushion, all
        // lock-free), so a plain mutex here is fine -- enqueue_until() is
        // expected to be rare relative to enqueue().
        // enqueue_until() timers: a bounded min-heap (compile-time capacity, O(log N)); the earliest
        // deadline is one atomic that every worker's idle path reads without taking its spin lock
        // (see detail/timer_heap.hpp).
        detail::timer_heap<Traits::timer_capacity, task_type>& timers_{bulk_->timers};
    };

}

#include "pool_service.inl"
