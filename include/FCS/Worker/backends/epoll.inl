#pragma once

#include <cerrno>
#include <chrono>
#include <new>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace FCS::Worker::backend::epoll {

    // ---------------------------------------------------------------- lifecycle

    template<typename Pool>
    inline backend<Pool>::backend(Pool& service) : generic_eventlooper<backend<Pool>, int, Pool>(service) {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

        ::epoll_event wake_event{};
        wake_event.events = EPOLLIN;
        wake_event.data.u32 = static_cast<std::uint32_t>(capacity);
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &wake_event);
    }

    template<typename Pool>
    inline backend<Pool>::~backend() {
        stop_backend();
        discard();
        if (epoll_fd_ >= 0) ::close(epoll_fd_);
        if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
    }


    template<typename Pool>
    inline void backend<Pool>::start_backend() {
        generic_eventlooper<backend<Pool>, int, Pool>::start_backend();
        bool expected = false;
        if (!polling_.compare_exchange_strong(expected, true)) return;
        if (this->service().execution_policy() == execution::policy::dedicated_poller) {
            poller_ = std::thread([this] { run_loop(); });
        } else {
            poll_hook_ = this->service().register_poll_hook(this, &backend::poll_once);
        }
    }


    template<typename Pool>
    inline void backend<Pool>::stop_backend() noexcept {
        if (polling_.exchange(false)) {
            if (poller_.joinable()) {
                wake();
                poller_.join();
            } else {
                // Waits out any poll_once() already in flight on another worker.
                this->service().unregister_poll_hook(poll_hook_);
            }
            // From here this thread is the only driver: flush what was registered
            // or cancelled while the last one was shutting down, and free the dead.
            (void)service_commands();
        }
        generic_eventlooper<backend<Pool>, int, Pool>::stop_backend();
    }


    // Everything still registered, plus their commands. Only called once nothing
    // is driving. Every registration is marked dead *first*, so a destructor of
    // user code that cancel()s another one of them can't queue a command nobody
    // will ever service.
    template<typename Pool>
    inline void backend<Pool>::discard() noexcept {
        for (auto& slot : slots_) {
            if (auto* registration_ptr = slot.load(std::memory_order_acquire)) (void)slots_state_.request_cancel(registration_ptr->h);
        }
        for (auto& slot : slots_) {
            if (auto* registration_ptr = slot.load(std::memory_order_acquire)) {
                slot.store(nullptr, std::memory_order_release);
                delete registration_ptr;
            }
        }
        for (command* cmd = cmds_.take_all(); cmd != nullptr;) {
            command* next = cmd->next;
            delete cmd;
            cmd = next;
        }
    }

    // ------------------------------------------------------------- any thread

    template<typename Pool>
    template<typename Tag, std::size_t Size, detail::recv_option Options, typename Reader, typename Source, typename Callback>
    subscription backend<Pool>::register_source(Source&& source, Reader&& reader, Callback&& callback) {
        static_assert(reader_callable<Size, std::remove_cvref_t<Reader>, detail::readiness_source<Options>>,
                      "Reader must be callable with a detail::readiness_source<Options>& and return read_result<Size>");

        const auto fd = detail::resolve_native(source);
        const auto kind = detail::resolve_kind(source);

        const auto claimed = slots_state_.claim();
        if (!claimed) return {};

        auto* registration_ptr = new registration{};
        registration_ptr->h = *claimed;
        registration_ptr->fd = fd;
        registration_ptr->kind = kind;
        registration_ptr->dir = direction::read;
        registration_ptr->channel_subscription = this->service().template subscribe<Tag>(std::forward<Callback>(callback));
        registration_ptr->on_event = [this, reader_fn = std::forward<Reader>(reader)](registration& reg, int arm_error) mutable -> action {
            const auto submitted_at = std::chrono::steady_clock::now();
            completion<Size> c{};
            c.user_data = reg.h.index;
            c.generation = reg.h.generation;
            c.kind = reg.kind;
            c.op = completion_op::read;
            c.submitted_at = submitted_at;

            action next = action::keep;
            if (arm_error != 0) {
                // epoll_ctl() refused the fd on the driver.
                c.status = completion_status::error;
                c.raw_status = arm_error;
                c.error = detail::from_native_error(arm_error);
                next = action::retire;
            } else {
                detail::readiness_source<Options> src{reg.fd, reg.kind};
                const auto result = std::invoke(reader_fn, src);
                c.result = result;
                if (src.failed()) {
                    c.status = completion_status::error;
                    c.raw_status = src.raw_error();
                    c.error = detail::from_native_error(src.raw_error());
                    next = action::retire;
                } else if (result.size == 0) {
                    c.status = completion_status::closed;
                    next = action::retire;
                } else {
                    c.status = completion_status::ok;
                    c.raw_status = static_cast<std::int64_t>(result.size);
                }
            }
            c.completed_at = std::chrono::steady_clock::now();
            (void)this->service().template post<Tag>(c);
            return next;
        };

        return launch(registration_ptr);
    }


    template<typename Pool>
    template<typename Tag, std::size_t Size, typename Writer, typename Sink, typename Callback>
    subscription backend<Pool>::register_sink(Sink&& sink, Writer&& writer, Callback&& callback) {
        static_assert(writer_callable<std::remove_cvref_t<Writer>, detail::readiness_sink>,
                      "Writer must be callable with a detail::readiness_sink& and return a byte count");

        const auto fd = detail::resolve_native(sink);
        const auto kind = detail::resolve_kind(sink);

        const auto claimed = slots_state_.claim();
        if (!claimed) return {};

        auto* registration_ptr = new registration{};
        registration_ptr->h = *claimed;
        registration_ptr->fd = fd;
        registration_ptr->kind = kind;
        registration_ptr->dir = direction::write;
        registration_ptr->channel_subscription = this->service().template subscribe<Tag>(std::forward<Callback>(callback));
        registration_ptr->on_event = [this, writer_fn = std::forward<Writer>(writer)](registration& reg, int arm_error) mutable -> action {
            const auto submitted_at = std::chrono::steady_clock::now();
            completion<Size> c{};
            c.user_data = reg.h.index;
            c.generation = reg.h.generation;
            c.kind = reg.kind;
            c.op = completion_op::write;
            c.submitted_at = submitted_at;

            action next = action::keep;
            if (arm_error != 0) {
                c.status = completion_status::error;
                c.raw_status = arm_error;
                c.error = detail::from_native_error(arm_error);
                next = action::retire;
            } else {
                detail::readiness_sink snk{reg.fd, reg.kind};
                const auto written = std::invoke(writer_fn, snk);
                c.result.size = written; // bytes actually written this call; buffer unused for writes
                if (snk.failed()) {
                    c.status = completion_status::error;
                    c.raw_status = snk.raw_error();
                    c.error = detail::from_native_error(snk.raw_error());
                    next = action::retire;
                } else {
                    c.status = completion_status::ok;
                    c.raw_status = static_cast<std::int64_t>(written);
                }
            }
            c.completed_at = std::chrono::steady_clock::now();
            (void)this->service().template post<Tag>(c);
            return next;
        };

        return launch(registration_ptr);
    }


    // Hands a fully-initialized registration to the driver. Only the two stores
    // below are visible to other threads; everything else about the registration
    // was written before the release store.
    template<typename Pool>
    inline subscription backend<Pool>::launch(registration* registration_ptr) noexcept {
        const auto handle = registration_ptr->h;
        if (auto* cmd = new (std::nothrow) command{}) {
            cmd->what = command::kind::arm;
            cmd->h = handle;
            slots_[handle.index].store(registration_ptr, std::memory_order_release);
            cmds_.push(cmd);
            wake_if_sleeping();
            return subscription{this, &backend::cancel_slot, table::pack(handle)};
        }
        (void)slots_state_.request_cancel(handle);
        slots_state_.release(handle.index);
        delete registration_ptr;
        return {};
    }


    template<typename Pool>
    inline void backend<Pool>::post_command(command::kind what, table::handle h) noexcept {
        auto* cmd = new (std::nothrow) command{};
        if (!cmd) {
            // Out of memory: the slot is already marked cancelled; make the
            // driver find it by scanning instead of by command.
            sweep_needed_.store(true, std::memory_order_seq_cst);
        } else {
            cmd->what = what;
            cmd->h = h;
            cmds_.push(cmd);
        }
        wake_if_sleeping();
    }

    // Never dereferences the registration: it may already have been freed by
    // the driver. All it does is the lock-free slot-state transition; the
    // driver does the rest when it sees the command.
    template<typename Pool>
    inline bool backend<Pool>::cancel_slot(void* owner, std::size_t packed) noexcept {
        auto* self = static_cast<backend*>(owner);
        const auto h = table::unpack(packed);
        if (!self->slots_state_.request_cancel(h)) return false;
        self->post_command(command::kind::cancel, h);
        return true;
    }


    template<typename Pool>
    inline void backend<Pool>::wake() noexcept {
        if (wakeup_fd_ >= 0) { std::uint64_t one{1}; (void)!::write(wakeup_fd_, &one, sizeof(one)); }
    }

    // Pairs with the sequentially-consistent `sleeping_` announcement in
    // run_loop(): either we see that the poller is about to block (and wake
    // it), or the poller sees our command before it blocks. In shared_worker
    // mode nothing ever sleeps, so this costs one load.
    template<typename Pool>
    inline void backend<Pool>::wake_if_sleeping() noexcept {
        if (sleeping_.load(std::memory_order_seq_cst)) wake();
    }

    // ------------------------------------------------------------- driver side

    // Applies registrations and cancellations in the order they were issued.
    template<typename Pool>
    inline bool backend<Pool>::service_commands() noexcept {
        if (sweep_needed_.exchange(false, std::memory_order_acq_rel)) sweep_cancelled();

        bool any = false;
        for (command* cmd = cmds_.take_all(); cmd != nullptr;) {
            command* next = cmd->next;
            any = true;

            auto* registration_ptr = cmd->h.index < capacity ? slots_[cmd->h.index].load(std::memory_order_acquire) : nullptr;
            if (registration_ptr && same_generation(registration_ptr->h.generation, cmd->h.generation)) {
                if (!slots_state_.live(registration_ptr->h)) {
                    finish(registration_ptr);        // cancelled -- possibly before it ever armed
                } else if (cmd->what == command::kind::arm) {
                    arm(*registration_ptr);
                }
            }
            delete cmd;
            cmd = next;
        }
        return any;
    }

    // Fallback for a cancel command that couldn't be allocated.
    template<typename Pool>
    inline void backend<Pool>::sweep_cancelled() noexcept {
        for (auto& slot : slots_) {
            auto* registration_ptr = slot.load(std::memory_order_acquire);
            if (registration_ptr && !slots_state_.live(registration_ptr->h)) finish(registration_ptr);
        }
    }


    // Attaches a registration's direction to its fd's epoll entry, creating the
    // entry if this is the fd's first registration.
    template<typename Pool>
    inline void backend<Pool>::arm(registration& registration_ref) noexcept {
        if (registration_ref.fd < 0) { fail(registration_ref, EBADF); return; }

        std::size_t index = no_entry, free_index = no_entry;
        for (std::size_t i = 0; i < capacity; ++i) {
            if (entries_[i].fd == registration_ref.fd) { index = i; break; }
            if (free_index == no_entry && entries_[i].fd < 0) free_index = i;
        }
        const bool is_new = index == no_entry;
        if (is_new) {
            if (free_index == no_entry) { fail(registration_ref, ENOSPC); return; }
            index = free_index;
            entries_[index].fd = registration_ref.fd;
        }

        auto& e = entries_[index];
        registration*& slot = e.dir[static_cast<std::size_t>(registration_ref.dir)];
        registration* const replaced = std::exchange(slot, &registration_ref);
        if (!sync_interest(index, is_new)) {
            const int error = errno;
            slot = replaced;
            if (is_new) e = entry{};
            fail(registration_ref, error);
            return;
        }
        registration_ref.entry = index;

        if (replaced) {
            // Same direction registered again on this fd: the newest wins.
            replaced->entry = no_entry;
            (void)slots_state_.request_cancel(replaced->h);
            finish(replaced);
        }
    }

    // The fd couldn't be armed: tell the callback why, then drop the registration.
    template<typename Pool>
    inline void backend<Pool>::fail(registration& registration_ref, int error) noexcept {
        (void)registration_ref.on_event(registration_ref, error);
        (void)slots_state_.retire(registration_ref.h);
        finish(&registration_ref, /*deliver_pending=*/true);
    }

    template<typename Pool>
    inline void backend<Pool>::detach(registration& registration_ref) noexcept {
        if (registration_ref.entry == no_entry) return;
        const auto index = std::exchange(registration_ref.entry, no_entry);
        auto& e = entries_[index];
        e.dir[static_cast<std::size_t>(registration_ref.dir)] = nullptr;
        if (!e.dir[0] && !e.dir[1]) {
            (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, e.fd, nullptr);
            e = entry{};
        } else {
            (void)sync_interest(index, false);
        }
    }

    // Programs the kernel with the interest implied by whichever directions are
    // attached to the entry.
    template<typename Pool>
    inline bool backend<Pool>::sync_interest(std::size_t index, bool is_new) noexcept {
        const auto& e = entries_[index];
        ::epoll_event ev{};
        ev.events = (e.dir[0] ? static_cast<std::uint32_t>(EPOLLIN) : 0u)
                  | (e.dir[1] ? static_cast<std::uint32_t>(EPOLLOUT) : 0u);
        ev.data.u32 = static_cast<std::uint32_t>(index);
        if (::epoll_ctl(epoll_fd_, is_new ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, e.fd, &ev) == 0) return true;
        // Our table and the kernel's disagree: the fd was closed (which silently
        // drops it from the epoll set) and its number reused without a cancel() in between.
        if (errno != (is_new ? EEXIST : ENOENT)) return false;
        return ::epoll_ctl(epoll_fd_, is_new ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, e.fd, &ev) == 0;
    }


    // Frees a registration with no lock held, so destructors of user code (the
    // callback, the Reader/Writer captures) may call cancel() or register freely.
    template<typename Pool>
    inline void backend<Pool>::finish(registration* registration_ptr, bool deliver_pending) noexcept {
        detach(*registration_ptr);
        // A registration that ended on its own (EOF/error) has just post()ed its
        // terminal completion; retire() -- not the destructor's cancel() -- so
        // that completion, still queued in the pool, isn't dropped with it.
        if (deliver_pending) (void)registration_ptr->channel_subscription.retire();
        const auto h = registration_ptr->h;
        slots_[h.index].store(nullptr, std::memory_order_release);
        (void)slots_state_.retire(h);
        delete registration_ptr;
        slots_state_.release(h.index);
    }


    // Handles one epoll_event: drops whichever direction was cancelled or ERR/HUP
    // leaves without a clean signal of its own, then runs on_event for whichever
    // direction is both flagged ready and still live.
    template<typename Pool>
    inline void backend<Pool>::dispatch(std::size_t index, std::uint32_t flags) noexcept {
        // Snapshot first: finish() can release the entry, so nothing below touches entries_[index].
        registration* regs[2] = {entries_[index].dir[0], entries_[index].dir[1]};
        constexpr std::uint32_t ready[2] = {EPOLLIN, EPOLLOUT};
        const bool hung_up = (flags & (EPOLLERR | EPOLLHUP)) != 0;

        for (std::size_t d = 0; d < 2; ++d) {
            auto* registration_ptr = regs[d];
            if (!registration_ptr) continue;
            if (!slots_state_.live(registration_ptr->h)) {
                finish(registration_ptr);        // cancelled, command not serviced yet
                regs[d] = nullptr;
            } else if (hung_up && !(flags & ready[d])) {
                (void)slots_state_.retire(registration_ptr->h);
                finish(registration_ptr);
                regs[d] = nullptr;
            }
        }
        for (std::size_t d = 0; d < 2; ++d) {
            auto* registration_ptr = regs[d];
            if (!registration_ptr || !(flags & ready[d])) continue;
            const action next = registration_ptr->on_event(*registration_ptr, 0);
            if (next == action::keep && slots_state_.live(registration_ptr->h)) continue;
            (void)slots_state_.retire(registration_ptr->h);
            finish(registration_ptr, /*deliver_pending=*/next == action::retire);
        }
    }


    template<typename Pool>
    inline void backend<Pool>::run_loop() {
        std::array<::epoll_event, 64> events{};
        for (;;) {
            (void)service_commands();
            if (!polling_.load(std::memory_order_acquire)) break;

            // Announce the sleep *before* the final look at the command queue;
            // see wake_if_sleeping().
            sleeping_.store(true, std::memory_order_seq_cst);
            if (!cmds_.empty() || sweep_needed_.load(std::memory_order_seq_cst) || !polling_.load(std::memory_order_seq_cst)) {
                sleeping_.store(false, std::memory_order_seq_cst);
                continue;
            }
            const auto count = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
            sleeping_.store(false, std::memory_order_seq_cst);
            if (count < 0) { if (errno == EINTR) continue; break; }

            for (std::size_t i{}, n = static_cast<std::size_t>(count); i < n; ++i) {
                const auto index = events[i].data.u32;
                if (index == static_cast<std::uint32_t>(capacity)) {
                    std::uint64_t drained{};
                    (void)!::read(wakeup_fd_, &drained, sizeof(drained));
                    continue;
                }
                dispatch(index, events[i].events);
            }
        }
    }


    // execution::shared_worker path: one non-blocking pass, called from some pool
    // worker's own loop. poll_registry guarantees at most one thread is inside
    // this per backend at a time -- the single-driver contract everything above
    // leans on.
    template<typename Pool>
    inline bool backend<Pool>::poll_once(void* owner) noexcept {
        auto* self = static_cast<backend*>(owner);
        bool progressed = self->service_commands();

        std::array<::epoll_event, 64> events{};
        const auto count = ::epoll_wait(self->epoll_fd_, events.data(), static_cast<int>(events.size()), 0);
        for (std::size_t i{}, n = count > 0 ? static_cast<std::size_t>(count) : 0; i < n; ++i) {
            const auto index = events[i].data.u32;
            if (index == static_cast<std::uint32_t>(capacity)) {
                std::uint64_t drained{};
                (void)!::read(self->wakeup_fd_, &drained, sizeof(drained));
                continue;
            }
            self->dispatch(index, events[i].events);
            progressed = true;
        }
        return progressed;
    }

}
