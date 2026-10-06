#pragma once

#include "detail/queue_classifier.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <memory>
#include <utility>

namespace FCS::Worker {

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>::~pool_service() { stop(); }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::watchdog_interval(std::chrono::nanoseconds interval) noexcept {
        // Once running, 0 cannot turn an already-spawned monitor off (stop() does
        // that) and a non-zero value only retunes it -- see the declaration.
        if (running_ && interval.count() <= 0) return *this;
        watchdog_interval_ns_.store(interval.count() < 0 ? 0 : interval.count(), std::memory_order_relaxed);
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::watchdog_mark_stall(std::chrono::nanoseconds tolerance) noexcept {
        watchdog_tolerance_ns_.store(tolerance.count() < 0 ? 0 : tolerance.count(), std::memory_order_relaxed);
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::idle_cache_ttl(std::size_t uses) noexcept {
        if (!running_) registry_.idle_cache_ttl(uses);
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::concurrency(std::size_t count) noexcept {
        if (!running_) { threads_ = std::clamp<std::size_t>(count, 1, max_workers); registry_.configure(threads_); }
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::burst_backpressure(std::size_t full, std::size_t available, std::size_t candidates) noexcept {
        gate_.configure(full, available, candidates);
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::execution_policy(execution::policy policy) noexcept {
        return execution_policy(policy, execution::task_type::segment, 1);
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::execution_policy(execution::policy policy, execution::task_type type, std::size_t between_num_threads) noexcept {
        if (running_) return *this;
        execution_policy_.store(policy, std::memory_order_relaxed);
        if (policy == execution::policy::shared_worker) {
            poll_task_type_.store(type, std::memory_order_relaxed);
            poll_thread_limit_.store(between_num_threads == 0 ? 1 : between_num_threads, std::memory_order_relaxed);
        }
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    execution::policy pool_service<QueueCapacity, Traits>::execution_policy() const noexcept {
        return execution_policy_.load(std::memory_order_relaxed);
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::timeout_tolerate(std::chrono::nanoseconds tolerate) noexcept {
        poll_tolerance_ns_.store(tolerate.count(), std::memory_order_relaxed);
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    pool_service<QueueCapacity, Traits>& pool_service<QueueCapacity, Traits>::age_starvation(detail::aging_granularity clamp_when, std::uint64_t tasks_count, std::uint8_t base_growth, std::uint8_t max) noexcept {
        fast_.configure({clamp_when, tasks_count, base_growth, max});
        return *this;
    }

    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::start() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) return;
        registry_.configure(threads_);
        parking_.configure(threads_);
        workers_.reserve(threads_);
        for (std::size_t i{}; i < threads_; ++i) workers_.emplace_back([this, i] { worker_loop(i); });
        watchdog_stop_.store(false, std::memory_order_relaxed);
        if (watchdog_interval_ns_.load(std::memory_order_relaxed) > 0) watchdog_ = std::thread([this] { watchdog_loop(); });
    }

    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::stop() noexcept {
        if (!running_.exchange(false)) return;
        signal_all();
        for (auto& thread : workers_) if (thread.joinable()) thread.join();
        workers_.clear();
        // Only after the workers are gone: they may still be draining queued work
        // (and getting stuck on it) after running_ went false.
        watchdog_stop_.store(true, std::memory_order_seq_cst);
        (void)watchdog_parker_.unpark();
        if (watchdog_.joinable()) watchdog_.join();
    }

    template<std::size_t QueueCapacity, typename Traits>
    template<typename F, typename... Args>
    bool pool_service<QueueCapacity, Traits>::enqueue(workload lane, source_kind source, F&& fn, Args&&... args) {
        using fn_t = std::decay_t<F>;
        using args_t = std::tuple<std::decay_t<Args>...>;
        auto work = [callable = fn_t(std::forward<F>(fn)), values = args_t(std::forward<Args>(args)...)]() mutable {
            std::apply([&](auto&... v) { std::invoke(callable, std::move(v)...); }, values);
        };
        return submit({lane, source, task_type{std::move(work)}});
    }

    template<std::size_t QueueCapacity, typename Traits>
    template<typename F, typename... Args>
    bool pool_service<QueueCapacity, Traits>::enqueue_priority(detail::task_priority priority, source_kind source, F&& fn, Args&&... args) {
        using fn_t = std::decay_t<F>;
        using args_t = std::tuple<std::decay_t<Args>...>;
        auto work = [callable = fn_t(std::forward<F>(fn)), values = args_t(std::forward<Args>(args)...)]() mutable {
            std::apply([&](auto&... v) { std::invoke(callable, std::move(v)...); }, values);
        };
        return submit({workload::Fast, source, task_type{std::move(work)}, priority});
    }

    template<std::size_t QueueCapacity, typename Traits>
    template<typename Tag, typename Callback, typename... Args>
    bool pool_service<QueueCapacity, Traits>::enqueue_until(timeout_mode mode, std::chrono::nanoseconds timeout, Callback&& cb, Args&&... args) {
        using fn_t = std::decay_t<Callback>;
        using args_t = std::tuple<std::decay_t<Args>...>;

        // `mode` is only known at runtime (it's a function argument, not a
        // template one), so `fire` has to carry all four branches and pick
        // one when it actually runs; `if constexpr` on Tag still prunes the
        // event_post branch entirely at compile time when Tag is void.
        task_type fire = [this, mode, callable = fn_t(std::forward<Callback>(cb)), values = args_t(std::forward<Args>(args)...)]() mutable {
            switch (mode) {
            case timeout_mode::timer:
                (void)submit_direct({workload::Fast, source_kind::synthetic,
                    task_type{[callable = std::move(callable), values = std::move(values)]() mutable {
                        std::apply([&](auto&... v) { std::invoke(callable, std::move(v)...); }, values);
                    }}, detail::task_priority::high});
                return;
            case timeout_mode::fast_lane:
                (void)submit({workload::Fast, source_kind::synthetic,
                    task_type{[callable = std::move(callable), values = std::move(values)]() mutable {
                        std::apply([&](auto&... v) { std::invoke(callable, std::move(v)...); }, values);
                    }}});
                return;
            case timeout_mode::slow_lane:
                (void)submit({workload::Slow, source_kind::synthetic,
                    task_type{[callable = std::move(callable), values = std::move(values)]() mutable {
                        std::apply([&](auto&... v) { std::invoke(callable, std::move(v)...); }, values);
                    }}});
                return;
            case timeout_mode::event_post:
                // `cb` is unused here -- post_from<Tag>() delivers straight
                // to whatever's already subscribed to Tag (via subscribe<Tag>()
                // elsewhere), so `args...` has to match that subscriber's
                // parameter types exactly, the same as any other post_from()
                // call; folding `cb` in as an extra posted value would just
                // make it a channel nothing is subscribed to.
                if constexpr (!std::is_void_v<Tag>) {
                    std::apply([&](auto&... v) { (void)this->template post_from<Tag>(source_kind::synthetic, std::move(v)...); }, values);
                } else {
                    // No Tag given for event_post -- fall back to running inline rather than silently dropping it.
                    std::apply([&](auto&... v) { std::invoke(callable, std::move(v)...); }, values);
                }
                return;
            }
        };

        const auto added = timers_.add(std::chrono::steady_clock::now() + timeout, std::move(fire));
        if (!added.added) return false; // table full: refused, like a full admission queue
        // Sleepers computed their wake-up from the previous earliest deadline; only a timer that
        // becomes the new earliest changes what they should wait for.
        if (added.became_earliest) signal_all();
        return true;
    }

    template<std::size_t QueueCapacity, typename Traits>
    template<typename Tag, typename F>
    subscription pool_service<QueueCapacity, Traits>::subscribe(F&& fn) {
        using channel_t = typename detail::channel_from_tuple<Tag, Traits::callback_bytes, detail::callable_args_t<F>>::type;
        return channel<Tag, channel_t>().subscribe(std::forward<F>(fn));
    }

    template<std::size_t QueueCapacity, typename Traits>
    template<typename Tag, typename... Args>
        requires (std::copy_constructible<std::decay_t<Args>> && ...)
    std::size_t pool_service<QueueCapacity, Traits>::post_from(source_kind source, Args&&... args) {
        using channel_t = detail::static_channel<Tag, Traits::callback_bytes, std::decay_t<Args>...>;
        auto enqueue_work = [this, source](task_type&& work) { return submit({workload::Fast, source, std::move(work)}); };
        return channel<Tag, channel_t>().post(enqueue_work, args...);
    }

    template<std::size_t QueueCapacity, typename Traits>
    template<typename Callback, typename... Args>
    auto pool_service<QueueCapacity, Traits>::enqueue_and_poll(workload lane, Callback&& cb, Args&&... args) {
        using fn_t = std::decay_t<Callback>;
        using args_t = std::tuple<std::decay_t<Args>...>;
        using invoke_t = std::invoke_result_t<fn_t, Args...>;
        using result_t = enqueue_and_poll_result_t<invoke_t>;
        static_assert(!std::is_void_v<result_t>, "enqueue_and_poll's callback must return a value (R) -- user_data() would have nothing to expose for R == void");

        using payload_t = detail::completed_payload<result_t>;
        auto owner = detail::intrusive_ptr<detail::scheduled_state<payload_t>>::adopt(new detail::scheduled_state<payload_t>(1));
        auto producer = owner;
        const auto sequence = next_sequence_.fetch_add(1, std::memory_order_relaxed);

        const auto accepted = enqueue(lane, [this, lane, producer, callable = fn_t(std::forward<Callback>(cb)), values = args_t(std::forward<Args>(args)...)]() mutable {
            // The one place in this library that still touches catch --
            // boundary conversion only, turning a throwing callback (or a
            // coroutine that completed via an unhandled exception -- see
            // the auto-poll branch below) into a value instead of letting
            // it vanish into execute()'s outer catch(...){} (which would
            // otherwise leave this future stuck not-ready forever with no
            // error signal at all). Nothing here ever throws on purpose,
            // and nothing downstream of this ever catches: nowhere else in
            // enqueue_and_poll or async_completed_result uses
            // try/throw/catch.
            if constexpr (requires { typename async_result_value<invoke_t>::type; }) {
                // cb(args...) returns FCS::async_result<T>: auto-poll it. The coroutine, its
                // callable and its arguments move into a control block that owns them for the
                // coroutine's whole life; this task is just the first "step". A step resumes the
                // coroutine until it finishes, parks on an external awaitable, or -- when the
                // stall watchdog has flagged this worker -- yields at a checkpoint and is
                // re-queued as a new step (see detail/coop.hpp). Exactly one step owns the
                // coroutine at any time, so it is never resumed concurrently, though it may
                // resume on a different worker than it last ran on.
                using control_t = poll_control<invoke_t, payload_t, fn_t, args_t>;
                auto* c = new control_t(this, lane, std::move(producer), std::move(callable), std::move(values));
                try {
                    c->coroutine.emplace(std::apply([&](auto&... v) { return std::invoke(c->callable, std::move(v)...); }, c->values));
                } catch (...) {
                    c->producer->set_value(payload_t{Unexpected{error::callback_threw}, {}});
                    control_t::release(c);
                    return;
                }
                auto& promise = c->coroutine->handle.promise();
                promise.ctl = c;
                promise.reschedule = &control_t::reschedule;
                promise.resume_inline = &control_t::resume_inline;
                promise.retain = &control_t::retain;
                promise.release = &control_t::release;
                promise.complete = &control_t::complete;
                control_t::step(c); // consumes the initial reference
            } else {
                try {
                    auto value = std::apply([&](auto&... v) { return std::invoke(callable, std::move(v)...); }, values);
                    producer->set_value(payload_t{std::move(value), std::this_thread::get_id()});
                } catch (...) {
                    producer->set_value(payload_t{Unexpected{error::callback_threw}, {}});
                }
            }
        });
        if (!accepted) owner->set_value(payload_t{Unexpected{error::queue_full}, {}});
        return async_completed_result<fn_t, result_t>{std::move(owner), sequence};
    }

    template<std::size_t QueueCapacity, typename Traits>
    scheduler_snapshot pool_service<QueueCapacity, Traits>::scheduler_metadata() const noexcept {
        const auto mpmc_state = detail::classify_queue_state(metrics_.mpmc_depth(), QueueCapacity / 4, QueueCapacity + slow_capacity);
        return metrics_.snapshot(gate_.cushion_state(), mpmc_state, gate_.admission_state());
    }

    template<std::size_t QueueCapacity, typename Traits>
    std::size_t pool_service<QueueCapacity, Traits>::queued() const noexcept {
        return metrics_.cushion_depth() + metrics_.mpmc_depth() + metrics_.local_depth();
    }

    template<std::size_t QueueCapacity, typename Traits>
    detail::worker_steal_snapshot pool_service<QueueCapacity, Traits>::worker_profile(std::size_t id) const noexcept {
        return registry_.profile(id);
    }

    template<std::size_t QueueCapacity, typename Traits>
    std::size_t pool_service<QueueCapacity, Traits>::worker_count() const noexcept {
        return registry_.thread_count();
    }

    template<std::size_t QueueCapacity, typename Traits>
    bool pool_service<QueueCapacity, Traits>::submit_requeue(queued_task_type&& work) noexcept {
        try { return submit(std::move(work)); } catch (...) { return false; }
    }

    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::debug_dump(std::FILE* out) const noexcept {
        std::fprintf(out, "  pool: running=%d queued=%zu mpmc_depth=%llu epoch=%llu sleepers=%zu workers=%zu watchdog_parked=%d\n",
                     running_.load() ? 1 : 0, queued(), static_cast<unsigned long long>(metrics_.mpmc_depth()),
                     static_cast<unsigned long long>(parking_.epoch()), parking_.sleepers(), threads_, watchdog_parked_.load() ? 1 : 0);
        for (std::size_t i = 0; i < threads_; ++i) {
            const auto w = registry_.debug_state(i);
            std::fprintf(out, "  worker[%2zu]: %-8s %-10s local_depth=%-4zu%s\n", i, parking_.sleeping(i) ? "PARKED" : "awake",
                         (w.tick & 1u) ? "in-task" : "between", w.depth, w.stalled ? " STALLED" : "");
        }
    }

    template<std::size_t QueueCapacity, typename Traits>
    bool pool_service<QueueCapacity, Traits>::submit(queued_task_type work) {
        if (!gate_.submit(std::move(work), metrics_)) return false;
        signal_one();
        return true;
    }

    template<std::size_t QueueCapacity, typename Traits>
    bool pool_service<QueueCapacity, Traits>::submit_direct(queued_task_type work) {
        const auto source = work.source;
        if (!fast_.try_push(std::move(work))) { metrics_.record_rejected(); return false; }
        metrics_.record_submit(source);
        metrics_.adjust_mpmc_depth(1);
        signal_one();
        return true;
    }

    template<std::size_t QueueCapacity, typename Traits>
    bool pool_service<QueueCapacity, Traits>::refill() {
        const auto refilled = gate_.refill(metrics_, fast_, slow_);
        if (refilled) signal_one();
        return refilled;
    }

    template<std::size_t QueueCapacity, typename Traits>
    bool pool_service<QueueCapacity, Traits>::drain_global(std::size_t owner) {
        const auto drained = registry_.drain_global(owner, fast_, slow_, metrics_);
        if (drained) signal_one();
        return drained;
    }

    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::execute(queued_task_type& work) {
        metrics_.adjust_active(1);
        try { work.invoke(); } catch (...) {}
        // Release whatever the task's closure captured (subscriber_guard,
        // RAII handles, anything) right away, rather than leaving it alive
        // inside `work` until this worker either picks up a new task
        // (which overwrites it) or the pool stops (which destroys `work`
        // as worker_loop returns) -- either of those can be an arbitrarily
        // long time away for a worker that goes idle right after this task.
        work.invoke = nullptr;
        metrics_.adjust_active(-1);
        metrics_.record_completed();
    }

    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::worker_loop(std::size_t id) {
        queued_task_type work{};

        // Let coroutines running on this thread see the watchdog's flag (cooperative yields).
        detail::tls_worker_view = {&registry_.stalled_flag(id), &registry_.profile_ref(id), this, id, false};

        // Idle backoff. A pass that finds nothing used to go straight back to the top and
        // rescan every worker's depth, the gate and the queues at millions of passes a
        // second: pure cache-coherence traffic against the lines the *working* threads
        // are writing, and on an oversubscribed machine CPU stolen from them (a profile
        // showed ~85M steal probes per worker for ~400k tasks). Now: a few passes of
        // exponentially growing busy-wait, then a few yields, then park. Any progress
        // resets it, so a busy pool never pays for it.
        constexpr unsigned spin_rounds = 8;     // 1,2,4..128 cpu_relax() between rescans
        constexpr unsigned yield_rounds = 8;    // then give the core away
        unsigned idle_rounds = 0;

        while (running_ || queued()) {
            if (registry_.take_local(id, work, metrics_) || registry_.steal(id, work, metrics_, minimum_steal_depth)) {
                idle_rounds = 0;
                {
                    const auto in_task = registry_.scope(id); // watchdog: this worker is inside a task
                    execute(work);
                }
                if (detail::tls_worker_view.yielded) {
                    // A coroutine just yielded and sits a few tasks back in OUR deque. Pull what
                    // is waiting globally in on top of it, so that work -- not the coroutine --
                    // is what we pop next (the owner pops from the bottom).
                    detail::tls_worker_view.yielded = false;
                    if (gate_.cushion_state() != queue_state::empty) (void)refill();
                    if (detail::classify_queue_state(metrics_.mpmc_depth(), QueueCapacity / 4, QueueCapacity + slow_capacity) != queue_state::empty) (void)drain_global(id);
                }
                continue;
            }
            if (try_fire_due_timers(id)) { idle_rounds = 0; continue; }
            if (detail::classify_queue_state(metrics_.mpmc_depth(), QueueCapacity / 4, QueueCapacity + slow_capacity) != queue_state::empty && drain_global(id)) { idle_rounds = 0; continue; }
            if (gate_.cushion_state() != queue_state::empty && refill()) { idle_rounds = 0; continue; }

            // execution::shared_worker backends have no dedicated OS thread
            // blocking in their native wait call -- this is the only place
            // they ever get polled. A non-blocking pass here costs nothing
            // when no such backend is registered (poll_registry_ is empty
            // and this is skipped entirely), so it's safe to always check.
            // `active_pollers_` is a soft, self-correcting semaphore
            // emulation (classic fetch_add-then-check-ticket pattern) --
            // caps how many workers concurrently attempt a poll pass per
            // execution_policy()'s between_num_threads, so idle workers
            // beyond that limit skip polling this round (they'll get a
            // turn on a later iteration) instead of all piling onto
            // poll_registry_'s per-hook `busy` CAS at once.
            if (!poll_registry_.empty()) {
                const auto ticket = active_pollers_.fetch_add(1, std::memory_order_acq_rel);
                bool progressed = false;
                if (ticket < poll_thread_limit_.load(std::memory_order_relaxed)) {
                    // A slow Reader/Writer runs inline inside this pass, so it counts
                    // as work this worker can be stalled in (one pass == one
                    // `task_type::segment` sweep, or one hook for `individual`).
                    const auto in_poll = registry_.scope(id);
                    progressed = poll_registry_.poll_once(poll_task_type_.load(std::memory_order_relaxed));
                }
                active_pollers_.fetch_sub(1, std::memory_order_acq_rel);
                if (progressed) { idle_rounds = 0; continue; }
            }

            if (idle_rounds < spin_rounds) {
                for (unsigned n = 1u << idle_rounds; n != 0; --n) detail::cpu_relax();
                ++idle_rounds;
                continue;
            }
            if (idle_rounds < spin_rounds + yield_rounds) {
                std::this_thread::yield();
                ++idle_rounds;
                continue;
            }

            // Sampled before anything below, so a wake that lands after this point
            // makes park() decline to sleep instead of being slept through.
            const auto seen = parking_.epoch();

            // Two independent reasons to not wait indefinitely: a shared_worker
            // backend needs re-polling periodically (bounded by timeout_tolerate(),
            // default 1ms -- dedicated_poller has its own OS thread blocking in
            // epoll_wait/etc. instead, so it never needs this), and a pending
            // enqueue_until() timer needs this thread back before its deadline
            // regardless of policy. The tighter of the two (if either applies)
            // bounds the wait; with neither, this degrades to an indefinite park.
            std::optional<std::chrono::nanoseconds> bound;
            if (!poll_registry_.empty()) bound = std::chrono::nanoseconds{poll_tolerance_ns_.load(std::memory_order_relaxed)};
            if (const auto until_timer = time_until_next_timer(); until_timer && (!bound || *until_timer < *bound)) bound = until_timer;

            parking_.park(id, seen, bound, [this] { return !running_.load(std::memory_order_acquire); });

            // Back from a real sleep: after a quiet spell the cached idle estimate
            // describes a population that no longer exists (use-counter TTL can't
            // notice time passing with no steals). One cheap store, off the hot path.
            registry_.invalidate_idle_estimate();
            // Woken (or timed out, or cancelled by someone else's notify): don't redo the
            // whole spin phase if there is still nothing -- a couple of yields, then back to sleep.
            idle_rounds = spin_rounds + yield_rounds - 2;

            // Fan-out: a submit wakes exactly one sleeper, so if work is still
            // queued behind whatever woke us, pass the wake on (two at a time, so a
            // burst ramps up in log steps rather than one worker per wake latency).
            // Self-limiting: each woken worker only continues while work remains.
            if (queued() && parking_.sleepers() != 0) { parking_.notify_one(); parking_.notify_one(); }
        }
        detail::tls_worker_view = {};
    }

    // Any thread: nudges a watchdog that parked for idleness. seq_cst pairs with
    // its park (see watchdog_loop()); on the common not-parked path this is one
    // plain load.
    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::signal_one() noexcept {
        parking_.notify_one();
        if (watchdog_parked_.load(std::memory_order_seq_cst)) (void)watchdog_parker_.unpark();
    }

    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::signal_all() noexcept {
        parking_.notify_all();
        if (watchdog_parked_.load(std::memory_order_seq_cst)) (void)watchdog_parker_.unpark();
    }

    // The monitor: look every `interval` while any worker is inside a task, sleep
    // while the whole pool is idle. It only ever *marks* workers; thieves decide
    // what to do with the mark (worker_registry::steal), so there is no central
    // scheduling decision here.
    template<std::size_t QueueCapacity, typename Traits>
    void pool_service<QueueCapacity, Traits>::watchdog_loop() {
        const auto stopping = [this] { return watchdog_stop_.load(std::memory_order_seq_cst); };
        while (!stopping()) {
            const auto interval = std::chrono::nanoseconds{watchdog_interval_ns_.load(std::memory_order_relaxed)};
            const auto tolerance = std::chrono::nanoseconds{watchdog_tolerance_ns_.load(std::memory_order_relaxed)};
            const auto epoch = parking_.epoch();
            const auto scan = registry_.scan_stalls(std::chrono::steady_clock::now(), tolerance);
            // Thieves that are parked never look at the stalled flag: wake them so
            // the hostage work is actually picked up now, not whenever the next
            // submit happens to rouse them.
            if (scan.newly_stalled != 0) signal_all();

            if (scan.busy != 0) {
                watchdog_parker_.park(interval, stopping);
                continue;
            }
            // Nobody is inside a task: nothing can be stalled. Park until new work
            // is submitted (signal_one/all) -- Dekker-style with it: announce, then
            // re-check the epoch sampled *before* scanning, so a submit that raced
            // the scan is never slept through. The timeout is only a safety net for
            // work that arrives without a submit (a timer firing, a shared_worker
            // poll hook running a Reader) -- those can go unmonitored for at most
            // this long.
            watchdog_parked_.store(true, std::memory_order_seq_cst);
            watchdog_parker_.park(interval * 10, [&] { return stopping() || parking_.epoch() != epoch; });
            watchdog_parked_.store(false, std::memory_order_seq_cst);
        }
    }

    template<std::size_t QueueCapacity, typename Traits>
    bool pool_service<QueueCapacity, Traits>::try_fire_due_timers(std::size_t id) {
        // Lock-free throughout: the common case is one atomic load and no clock read.
        const auto next = timers_.next_deadline();
        if (next == timers_.none) return false;
        const auto now = std::chrono::steady_clock::now();
        if (next > now.time_since_epoch().count()) return false;

        task_type fired;
        if (!timers_.fire_one(now, fired)) return false; // another worker took it
        const auto in_task = registry_.scope(id); // a timer callback is user code too
        fired();
        return true;
    }

    template<std::size_t QueueCapacity, typename Traits>
    std::optional<std::chrono::nanoseconds> pool_service<QueueCapacity, Traits>::time_until_next_timer() const {
        const auto next = timers_.next_deadline();
        if (next == timers_.none) return std::nullopt;
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        // `next`/`now` are raw steady_clock ticks; convert rather than assume 1 tick == 1ns.
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::duration{next > now ? next - now : 0});
    }


}
