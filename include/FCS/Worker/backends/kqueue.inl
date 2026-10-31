#pragma once

#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <new>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace FCS::Worker::backend::kqueue {

    namespace detail_kq {
        // kevent's field types differ per OS (udata: void* vs intptr_t; filter/flags: signed vs
        // unsigned, different widths). Assigning through these keeps the backend free of #ifdefs.
        template<typename Event>
        inline void set_udata(Event& event, std::uintptr_t value) noexcept {
            using U = decltype(event.udata);
            if constexpr (std::is_pointer_v<U>) event.udata = reinterpret_cast<U>(value);
            else event.udata = static_cast<U>(value);
        }
        template<typename Event>
        [[nodiscard]] inline std::uintptr_t get_udata(const Event& event) noexcept {
            using U = decltype(event.udata);
            if constexpr (std::is_pointer_v<U>) return reinterpret_cast<std::uintptr_t>(event.udata);
            else return static_cast<std::uintptr_t>(event.udata);
        }
        [[nodiscard]] inline struct kevent make_change(int fd, int filter, int flags, std::uintptr_t udata) noexcept {
            struct kevent change{};
            change.ident = static_cast<decltype(change.ident)>(fd);
            change.filter = static_cast<decltype(change.filter)>(filter);
            change.flags = static_cast<decltype(change.flags)>(flags);
            set_udata(change, udata);
            return change;
        }
        // The wakeup pipe's udata. A packed slot handle is never 0: generations start at 1.
        inline constexpr std::uintptr_t wake_udata = 0;

        [[nodiscard]] inline bool make_nonblocking_cloexec(int fd) noexcept {
            const int fl = ::fcntl(fd, F_GETFL);
            const int fd_flags = ::fcntl(fd, F_GETFD);
            if (fl < 0 || fd_flags < 0) return false;
            return ::fcntl(fd, F_SETFL, fl | O_NONBLOCK) == 0 && ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) == 0;
        }
    }

    // ---------------------------------------------------------------- lifecycle

    template<typename Pool>
    inline backend<Pool>::backend(Pool& service) : generic_eventlooper<backend<Pool>, int, Pool>(service) {
        kqueue_fd_ = ::kqueue();
        if (kqueue_fd_ >= 0) {
            const int fd_flags = ::fcntl(kqueue_fd_, F_GETFD);          // close-on-exec; best effort
            if (fd_flags >= 0) (void)::fcntl(kqueue_fd_, F_SETFD, fd_flags | FD_CLOEXEC);
        }
        int fds[2]{-1, -1};
        if (kqueue_fd_ >= 0 && ::pipe(fds) == 0) {
            if (detail_kq::make_nonblocking_cloexec(fds[0]) && detail_kq::make_nonblocking_cloexec(fds[1])) {
                wake_read_ = fds[0];
                wake_write_ = fds[1];
                const auto change = detail_kq::make_change(wake_read_, EVFILT_READ, EV_ADD | EV_ENABLE, detail_kq::wake_udata);
                if (::kevent(kqueue_fd_, &change, 1, nullptr, 0, nullptr) < 0) { ::close(fds[0]); ::close(fds[1]); wake_read_ = wake_write_ = -1; }
            } else {
                ::close(fds[0]);
                ::close(fds[1]);
            }
        }
    }

    template<typename Pool>
    inline backend<Pool>::~backend() {
        stop_backend();
        discard();
        if (kqueue_fd_ >= 0) ::close(kqueue_fd_);
        if (wake_read_ >= 0) ::close(wake_read_);
        if (wake_write_ >= 0) ::close(wake_write_);
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
                // kevent() refused the fd on the driver.
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
        // Non-blocking: a full pipe already means "a wakeup is pending", so EAGAIN is success.
        if (wake_write_ >= 0) { const char one = 1; (void)!::write(wake_write_, &one, 1); }
    }

    template<typename Pool>
    inline void backend<Pool>::drain_wakeup() noexcept {
        char sink[64];
        while (::read(wake_read_, sink, sizeof(sink)) > 0) {}
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



    // ------------------------------------------------------------- driver side

    // Attaches a registration to the kernel: one knote per (fd, direction).
    template<typename Pool>
    inline void backend<Pool>::arm(registration& registration_ref) noexcept {
        if (registration_ref.fd < 0 || kqueue_fd_ < 0) { fail(registration_ref, EBADF); return; }

        // Same direction already registered on this fd: the newest wins. Found *before* the new
        // knote is added. Replacement is done explicitly (delete the old knote, add the new one)
        // rather than leaning on "EV_ADD of an existing (fd, filter) retargets its udata": that is
        // what the man pages promise, but it is the one corner where kqueue implementations differ
        // (libkqueue keeps the old udata), and an explicit delete+add behaves the same everywhere.
        registration* replaced = nullptr;
        for (auto& slot : slots_) {
            auto* other = slot.load(std::memory_order_acquire);
            if (other && other != &registration_ref && other->attached && other->fd == registration_ref.fd && other->dir == registration_ref.dir) { replaced = other; break; }
        }

#if defined(SO_NOSIGPIPE)
        // BSD/macOS raise SIGPIPE on a send() to a closed peer unless told not to; macOS has no
        // MSG_NOSIGNAL to say it per call. Harmless when the fd isn't a socket (the call just fails).
        if (registration_ref.dir == direction::write && registration_ref.kind != source_kind::io) {
            const int on = 1;
            (void)::setsockopt(registration_ref.fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
        }
#endif

        const int filter = registration_ref.dir == direction::read ? EVFILT_READ : EVFILT_WRITE;
        if (replaced) detach(*replaced);   // clears replaced->attached; the knote is gone until we re-add it
        const auto change = detail_kq::make_change(registration_ref.fd, filter, EV_ADD | EV_ENABLE, table::pack(registration_ref.h));
        if (::kevent(kqueue_fd_, &change, 1, nullptr, 0, nullptr) < 0) {
            const int error = errno;
            if (replaced) {
                // Put the old one back exactly as it was; if even that fails it has no knote
                // any more, so it must hear about it rather than wait forever.
                const auto restore = detail_kq::make_change(replaced->fd, filter, EV_ADD | EV_ENABLE, table::pack(replaced->h));
                if (::kevent(kqueue_fd_, &restore, 1, nullptr, 0, nullptr) == 0) replaced->attached = true;
                else fail(*replaced, errno);
            }
            fail(registration_ref, error);
            return;
        }
        registration_ref.attached = true;

        if (replaced) {                      // its knote is gone (detached above): just retire the registration
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
        if (!registration_ref.attached) return;
        registration_ref.attached = false;
        const int filter = registration_ref.dir == direction::read ? EVFILT_READ : EVFILT_WRITE;
        const auto change = detail_kq::make_change(registration_ref.fd, filter, EV_DELETE, 0);
        // ENOENT/EBADF: the fd was already closed, which removes its knotes by itself.
        (void)::kevent(kqueue_fd_, &change, 1, nullptr, 0, nullptr);
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


    // Handles one kevent: ignores events for registrations that are gone (the batch can
    // outlive them), drops cancelled ones, and otherwise runs on_event.
    template<typename Pool>
    inline void backend<Pool>::dispatch(const struct kevent& event) noexcept {
        const auto packed = detail_kq::get_udata(event);
        if (packed == detail_kq::wake_udata) { drain_wakeup(); return; }

        const auto handle = table::unpack(static_cast<std::size_t>(packed));
        if (handle.index >= capacity) return;
        auto* registration_ptr = slots_[handle.index].load(std::memory_order_acquire);
        if (!registration_ptr || !same_generation(registration_ptr->h.generation, handle.generation)) return;

        if (!slots_state_.live(registration_ptr->h)) {
            finish(registration_ptr);            // cancelled, command not serviced yet
            return;
        }
        // EV_ERROR on a returned event carries the errno in `data`; report it like an arm failure.
        const int error = (event.flags & EV_ERROR) ? static_cast<int>(event.data) : 0;
        const action next = registration_ptr->on_event(*registration_ptr, error);
        if (next == action::keep && slots_state_.live(registration_ptr->h)) return;
        (void)slots_state_.retire(registration_ptr->h);
        finish(registration_ptr, /*deliver_pending=*/next == action::retire);
    }


    template<typename Pool>
    inline void backend<Pool>::run_loop() {
        std::array<struct kevent, 64> events{};
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
            const int count = ::kevent(kqueue_fd_, nullptr, 0, events.data(), static_cast<int>(events.size()), nullptr);
            sleeping_.store(false, std::memory_order_seq_cst);
            if (count < 0) { if (errno == EINTR) continue; break; }

            for (int i = 0; i < count; ++i) dispatch(events[static_cast<std::size_t>(i)]);
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

        std::array<struct kevent, 64> events{};
        const struct timespec none{0, 0};
        const int count = self->kqueue_fd_ >= 0
            ? ::kevent(self->kqueue_fd_, nullptr, 0, events.data(), static_cast<int>(events.size()), &none) : 0;
        for (int i = 0; i < count; ++i) {
            self->dispatch(events[static_cast<std::size_t>(i)]);
            progressed = true;
        }
        return progressed;
    }

}
