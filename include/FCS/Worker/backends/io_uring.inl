#pragma once

#include <cstring>
#include <new>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace FCS::Worker::backend::io_uring {

    // ---------------------------------------------------------------- lifecycle

    template<typename Pool>
    inline backend<Pool>::backend(Pool& service) : generic_eventlooper<backend<Pool>, int, Pool>(service) {
        wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    }

    template<typename Pool>
    inline backend<Pool>::~backend() {
        stop_backend();
        discard_unarmed();
        if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
    }


    template<typename Pool>
    inline void backend<Pool>::start_backend() {
        generic_eventlooper<backend<Pool>, int, Pool>::start_backend();
        bool expected = false;
        if (!polling_.compare_exchange_strong(expected, true)) return;

        const bool dedicated = this->service().execution_policy() == execution::policy::dedicated_poller;
        // Created here rather than in the constructor because the ring's
        // shape depends on the execution policy, which isn't final until now.
        // A fresh ring on every start also means a restarted dedicated poller
        // is a new submitter thread with a ring that has never met another.
        const int rc = wakeup_fd_ < 0 ? -EBADF : ring_.setup(256, static_cast<unsigned>(capacity * 4), dedicated);
        if (rc < 0) {
            setup_error_.store(-rc, std::memory_order_relaxed);
            polling_.store(false, std::memory_order_release);
            return;
        }
        setup_error_.store(0, std::memory_order_relaxed);
        inflight_ = 0;
        wake_armed_ = false;
        woke_ = false;

        if (dedicated) {
            poller_ = std::thread([this] { run_loop(); });
        } else {
            // execution::shared_worker -- no dedicated OS thread, and no
            // eventfd poll either (nothing blocks that a wake would need to
            // interrupt); ordinary pool workers drive the ring via poll_once().
            poll_hook_ = this->service().register_poll_hook(this, &backend::poll_once);
        }
    }


    template<typename Pool>
    inline void backend<Pool>::stop_backend() noexcept {
        const bool was_polling = polling_.exchange(false);
        if (poller_.joinable()) {
            // Joined even if polling_ was already cleared by a failed
            // run_loop() start, or the std::thread would terminate().
            wake();
            poller_.join();       // run_loop() ends with shutdown_ring()
        } else if (was_polling) {
            // Waits out any poll_once() already in flight on another worker
            // before returning -- see poll_registry::unregister(). From here
            // this thread is the only driver, so it may run the shutdown itself.
            this->service().unregister_poll_hook(poll_hook_);
            shutdown_ring();
        }
        generic_eventlooper<backend<Pool>, int, Pool>::stop_backend();
    }

    // ----------------------------------------------------------- any-thread API


    template<typename Pool>
    template<typename Tag, std::size_t Size, common::recv_option Options, typename Reader, typename Source, typename Callback>
    subscription backend<Pool>::register_source(Source&& source, Reader&& reader, Callback&& callback) {
        static_assert(reader_callable<Size, std::remove_cvref_t<Reader>, common::completed_source>,
                      "Reader must be callable with a detail::completed_source& and return read_result<Size>");

        const auto fd = common::resolve_native(source);
        const auto kind = common::resolve_kind(source);

        const auto claimed = slots_state_.claim();
        if (!claimed) return {};

        auto* registration_ptr = new registration{};
        registration_ptr->h = *claimed;
        registration_ptr->fd = fd;
        registration_ptr->kind = kind;
        registration_ptr->dir = direction::read;
        registration_ptr->recv_flags = common::to_native_recv_flags(Options);
        registration_ptr->buffer_size = Size;
        registration_ptr->buffer = std::make_unique<std::byte[]>(Size);
        if (kind == source_kind::datagram) prepare_datagram_control(*registration_ptr);
        registration_ptr->channel_subscription = this->service().template subscribe<Tag>(std::forward<Callback>(callback));
        registration_ptr->on_event = [this, reader_fn = std::forward<Reader>(reader)](registration& reg, std::int32_t res) mutable -> action {
            completion<Size> c{};
            c.user_data = reg.h.index;
            c.generation = reg.h.generation;
            c.kind = reg.kind;
            c.op = completion_op::read;
            c.submitted_at = reg.submitted_at;

            action next = action::keep;
            if (res < 0) {
                c.status = completion_status::error;
                c.raw_status = res;
                c.error = common::from_native_error(static_cast<int>(-res));
                next = action::retire;
            } else if (res == 0) {
                // EOF, exactly as recv()/read() returning 0 would mean for
                // epoll -- the completion already told us, no need to call Reader.
                c.status = completion_status::closed;
                next = action::retire;
            } else {
                const sockaddr_storage* sender = reg.kind == source_kind::datagram ? reg.peer_addr.get() : nullptr;
                common::completed_source src{reg.buffer.get(), static_cast<std::size_t>(res), sender};
                c.result = std::invoke(reader_fn, src);
                c.status = completion_status::ok;
                c.raw_status = res;
            }
            c.completed_at = std::chrono::steady_clock::now();
            (void)this->service().template post<Tag>(c);
            return next;
        };

        const auto handle = registration_ptr->h;
        if (!publish(registration_ptr)) {
            (void)slots_state_.request_cancel(handle);
            slots_state_.release(handle.index);
            delete registration_ptr;
            return {};
        }
        return subscription{this, &backend::cancel_slot, table::pack(handle)};
    }


    template<typename Pool>
    template<typename Tag, std::size_t Size, typename Writer, typename Sink, typename Callback>
    subscription backend<Pool>::register_sink(Sink&& sink, Writer&& writer, Callback&& callback) {
        static_assert(writer_callable<std::remove_cvref_t<Writer>, common::readiness_sink>,
                      "Writer must be callable with a detail::readiness_sink& and return a byte count");

        const auto fd = common::resolve_native(sink);
        const auto kind = common::resolve_kind(sink);

        const auto claimed = slots_state_.claim();
        if (!claimed) return {};

        auto* registration_ptr = new registration{};
        registration_ptr->h = *claimed;
        registration_ptr->fd = fd;
        registration_ptr->kind = kind;
        registration_ptr->dir = direction::write;
        registration_ptr->channel_subscription = this->service().template subscribe<Tag>(std::forward<Callback>(callback));
        registration_ptr->on_event = [this, writer_fn = std::forward<Writer>(writer)](registration& reg, std::int32_t res) mutable -> action {
            completion<Size> c{};
            c.user_data = reg.h.index;
            c.generation = reg.h.generation;
            c.kind = reg.kind;
            c.op = completion_op::write;

            if (res < 0) {
                c.submitted_at = reg.submitted_at;
                c.completed_at = std::chrono::steady_clock::now();
                c.status = completion_status::error;
                c.raw_status = res;
                c.error = common::from_native_error(static_cast<int>(-res));
                (void)this->service().template post<Tag>(c);
                return action::retire;
            }

            const auto revents = static_cast<unsigned>(res);
            if ((revents & static_cast<unsigned>(POLLOUT)) == 0) {
                // ERR/HUP without a clean writable signal: retire silently,
                // exactly as epoll's dispatch() does. Anything else is spurious.
                return (revents & static_cast<unsigned>(POLLERR | POLLHUP)) != 0 ? action::retire : action::keep;
            }

            c.submitted_at = std::chrono::steady_clock::now();
            common::readiness_sink snk{reg.fd, reg.kind};
            const auto written = std::invoke(writer_fn, snk);
            c.completed_at = std::chrono::steady_clock::now();
            c.result.size = written; // bytes actually written this call; buffer unused for writes

            action next = action::keep;
            if (snk.failed()) {
                c.status = completion_status::error;
                c.raw_status = snk.raw_error();
                c.error = common::from_native_error(snk.raw_error());
                next = action::retire;
            } else {
                c.status = completion_status::ok;
                c.raw_status = static_cast<std::int64_t>(written);
            }
            (void)this->service().template post<Tag>(c);
            return next;
        };

        const auto handle = registration_ptr->h;
        if (!publish(registration_ptr)) {
            (void)slots_state_.request_cancel(handle);
            slots_state_.release(handle.index);
            delete registration_ptr;
            return {};
        }
        return subscription{this, &backend::cancel_slot, table::pack(handle)};
    }

    // Allocates the msghdr/iovec/sockaddr_storage triple a datagram-kind
    // registration's RECVMSG needs -- one iovec pointing at the registration's
    // own `buffer`, so payload bytes land exactly where a plain READ would
    // have put them.
    template<typename Pool>
    inline void backend<Pool>::prepare_datagram_control(registration& registration_ref) {
        registration_ref.peer_addr = std::make_unique<sockaddr_storage>();
        registration_ref.iov = std::make_unique<struct iovec>();
        registration_ref.msg = std::make_unique<struct msghdr>();
        registration_ref.iov->iov_base = registration_ref.buffer.get();
        registration_ref.iov->iov_len = registration_ref.buffer_size;
        std::memset(registration_ref.msg.get(), 0, sizeof(struct msghdr));
        registration_ref.msg->msg_name = registration_ref.peer_addr.get();
        registration_ref.msg->msg_namelen = sizeof(sockaddr_storage);
        registration_ref.msg->msg_iov = registration_ref.iov.get();
        registration_ref.msg->msg_iovlen = 1;
    }

    // Hands a fully-initialized registration to the driver. Only the two
    // stores below are visible to other threads; everything else about the
    // registration was written before the release store.
    template<typename Pool>
    inline bool backend<Pool>::publish(registration* registration_ptr) noexcept {
        auto* cmd = new (std::nothrow) command{};
        if (!cmd) return false;
        cmd->what = command::kind::arm;
        cmd->h = registration_ptr->h;
        slots_[registration_ptr->h.index].store(registration_ptr, std::memory_order_release);
        cmds_.push(cmd);
        wake_if_sleeping();
        return true;
    }


    template<typename Pool>
    inline void backend<Pool>::post_command(command::kind what, table::handle h) noexcept {
        auto* cmd = new (std::nothrow) command{};
        if (!cmd) {
            // Out of memory: the slot is already marked cancelled; make the
            // driver find it by scanning instead of by command.
            sweep_needed_.store(true, std::memory_order_release);
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
    // mode nothing ever sleeps, so this costs one relaxed-ish load.
    template<typename Pool>
    inline void backend<Pool>::wake_if_sleeping() noexcept {
        if (sleeping_.load(std::memory_order_seq_cst)) wake();
    }

    // ------------------------------------------------------------- driver side


    template<typename Pool>
    inline io_uring_sqe* backend<Pool>::acquire_sqe() noexcept {
        for (int attempt = 0; attempt < 4; ++attempt) {
            if (auto* sqe = ring_.get_sqe()) return sqe;
            (void)ring_.submit_and_wait(0); // SQ full: hand what's queued to the kernel and try again
        }
        return nullptr;
    }


    template<typename Pool>
    inline bool backend<Pool>::queue_operation(registration& registration_ref) noexcept {
        auto* sqe = acquire_sqe();
        if (!sqe) return false;
        registration_ref.submitted_at = std::chrono::steady_clock::now();
        const auto user_data = registration_ref.user_data();
        if (registration_ref.dir == direction::write) {
            detail::prep_poll_add(sqe, registration_ref.fd, static_cast<short>(POLLOUT), user_data);
        } else if (registration_ref.kind == source_kind::datagram && registration_ref.msg) {
            // msg_namelen must be reset before each RECVMSG: the kernel
            // overwrites it with the actual address length written, which
            // would otherwise shrink on repeat and truncate a later, larger sender.
            registration_ref.msg->msg_namelen = sizeof(sockaddr_storage);
            detail::prep_recvmsg(sqe, registration_ref.fd, registration_ref.msg.get(), registration_ref.recv_flags, user_data);
        } else if (registration_ref.kind == source_kind::network && registration_ref.recv_flags != 0) {
            detail::prep_recv(sqe, registration_ref.fd, registration_ref.buffer.get(),
                              static_cast<unsigned>(registration_ref.buffer_size), registration_ref.recv_flags, user_data);
        } else {
            // Plain read(): also what a recv_option on an io-kind fd degrades
            // to, matching readiness_source ("idempotent on unsupported").
            detail::prep_read(sqe, registration_ref.fd, registration_ref.buffer.get(),
                              static_cast<unsigned>(registration_ref.buffer_size), user_data);
        }
        registration_ref.in_flight = true;
        ++inflight_;
        if (!batch_.load(std::memory_order_relaxed)) (void)ring_.submit_and_wait(0);
        return true;
    }


    template<typename Pool>
    inline void backend<Pool>::queue_cancel_of(std::uint64_t target) noexcept {
        auto* sqe = acquire_sqe();
        if (!sqe) return;
        detail::prep_cancel(sqe, target, ack_user_data);
        ++inflight_;
        // Cancellation should be prompt, not wait for whatever batch flushes next.
        (void)ring_.submit_and_wait(0);
    }


    template<typename Pool>
    inline void backend<Pool>::arm_wakeup() noexcept {
        if (wake_armed_ || wakeup_fd_ < 0) return;
        if (auto* sqe = acquire_sqe()) {
            detail::prep_poll_add(sqe, wakeup_fd_, static_cast<short>(POLLIN), wake_user_data);
            ++inflight_;
            wake_armed_ = true;
        }
    }

    // Frees a registration. Callers guarantee no SQE for it is outstanding --
    // that guarantee is what makes it safe to release the buffer the kernel
    // was writing into, and to let the slot be claimed again.
    template<typename Pool>
    inline void backend<Pool>::finish(registration* registration_ptr, bool deliver_pending) noexcept {
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


    template<typename Pool>
    inline bool backend<Pool>::service_commands() noexcept {
        if (sweep_needed_.exchange(false, std::memory_order_acq_rel)) sweep_cancelled();

        bool any = false;
        for (command* cmd = cmds_.take_all(); cmd != nullptr;) {
            command* next = cmd->next;
            any = true;

            auto* registration_ptr = cmd->h.index < capacity ? slots_[cmd->h.index].load(std::memory_order_acquire) : nullptr;
            if (registration_ptr && same_generation(registration_ptr->h.generation, cmd->h.generation)) {
                if (cmd->what == command::kind::arm) {
                    if (!slots_state_.live(registration_ptr->h)) {
                        finish(registration_ptr);            // cancelled before it ever armed
                    } else if (!queue_operation(*registration_ptr)) {
                        (void)slots_state_.retire(registration_ptr->h); // no SQE available: can't ever fire
                        finish(registration_ptr);
                    }
                } else if (registration_ptr->in_flight) {
                    if (!registration_ptr->cancel_sent) {
                        registration_ptr->cancel_sent = true;
                        queue_cancel_of(registration_ptr->user_data()); // its -ECANCELED completion frees it
                    }
                } else {
                    finish(registration_ptr);
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
            if (!registration_ptr || slots_state_.live(registration_ptr->h)) continue;
            if (!registration_ptr->in_flight) {
                finish(registration_ptr);
            } else if (!registration_ptr->cancel_sent) {
                registration_ptr->cancel_sent = true;
                queue_cancel_of(registration_ptr->user_data());
            }
        }
    }


    template<typename Pool>
    inline unsigned backend<Pool>::reap(unsigned max) {
        return ring_.drain(max, [this](std::uint64_t user_data, std::int32_t res) { handle_cqe(user_data, res); });
    }


    template<typename Pool>
    inline void backend<Pool>::handle_cqe(std::uint64_t user_data, std::int32_t res) {
        if (inflight_ != 0) --inflight_; // every SQE yields exactly one CQE
        if (user_data == wake_user_data) { wake_armed_ = false; woke_ = true; return; }
        if (user_data == ack_user_data) return; // just an ack, not a real completion

        const auto index = static_cast<std::size_t>(user_data & 0xFFFFFFFFu);
        const auto generation = static_cast<std::uint32_t>(user_data >> 32);
        if (index >= capacity) return;
        auto* registration_ptr = slots_[index].load(std::memory_order_acquire);
        if (!registration_ptr || registration_ptr->h.generation != generation) return; // stale

        registration_ptr->in_flight = false;
        if (!slots_state_.live(registration_ptr->h)) {
            // Cancelled: this is its terminal completion (one op in flight at
            // a time, and nothing is resubmitted once cancelled), so the
            // kernel is done with the buffer and it's safe to free.
            finish(registration_ptr);
            return;
        }

        const action next = registration_ptr->on_event(*registration_ptr, res);
        if (next == action::keep && slots_state_.live(registration_ptr->h) && queue_operation(*registration_ptr)) return;

        (void)slots_state_.retire(registration_ptr->h);
        finish(registration_ptr, /*deliver_pending=*/next == action::retire);
    }


    template<typename Pool>
    inline void backend<Pool>::run_loop() {
        if (const int rc = ring_.enable(); rc < 0) {
            // Couldn't become the ring's submitter: nothing can ever run.
            setup_error_.store(-rc, std::memory_order_relaxed);
            polling_.store(false, std::memory_order_release);
            return;
        }
        arm_wakeup();

        for (;;) {
            (void)service_commands();
            if (!polling_.load(std::memory_order_acquire)) break;

            // Submit and wait in ONE io_uring_enter(). Skip the wait entirely
            // if completions are already sitting in the CQ.
            const unsigned wait = ring_.completions_ready() ? 0u : 1u;
            if (wait != 0) {
                // Announce the sleep *before* the final look at the command
                // queue; see wake_if_sleeping().
                sleeping_.store(true, std::memory_order_seq_cst);
                if (!cmds_.empty() || !polling_.load(std::memory_order_seq_cst)) {
                    sleeping_.store(false, std::memory_order_seq_cst);
                    continue;
                }
            }
            const int rc = ring_.submit_and_wait(wait);
            if (wait != 0) sleeping_.store(false, std::memory_order_seq_cst);
            if (rc < 0) std::this_thread::yield(); // transient (EBUSY/EAGAIN...): don't spin hot

            while (reap(256) == 256) {}

            if (woke_) {
                woke_ = false;
                std::uint64_t drained{};
                (void)!::read(wakeup_fd_, &drained, sizeof(drained));
                arm_wakeup(); // one-shot, unlike epoll's persistent registration
            }
        }

        shutdown_ring();
    }

    // execution::shared_worker path: one non-blocking pass, called from some
    // pool worker's own loop. poll_registry guarantees at most one thread is
    // inside this per backend at a time, which is exactly the single-driver
    // contract the ring needs. Makes no syscall when there is nothing to do.
    template<typename Pool>
    inline bool backend<Pool>::poll_once(void* owner) noexcept {
        auto* self = static_cast<backend*>(owner);
        if (!self->ring_.valid()) return false;

        const bool commanded = self->service_commands();
        (void)self->ring_.submit_and_wait(0);
        // Bounded, like epoll's fixed events-per-pass cap, so one busy backend
        // can't keep a worker from returning to its own queues.
        const bool reaped = self->reap(64) != 0;
        // Rearms queued while reaping must reach the kernel now, not on the
        // next poll (which may be a full poll-tolerance away): until then the
        // registration has no read outstanding and incoming data just waits.
        if (self->ring_.pending() != 0) (void)self->ring_.submit_and_wait(0);
        return commanded || reaped;
    }

    // Cancels everything still in flight and reaps until every SQE has
    // produced its CQE. Only then is anything freed, so the kernel can never
    // complete into memory we've released.
    template<typename Pool>
    inline void backend<Pool>::shutdown_ring() noexcept {
        if (!ring_.valid()) return;
        (void)service_commands();

        for (auto& slot : slots_) {
            auto* registration_ptr = slot.load(std::memory_order_acquire);
            if (!registration_ptr) continue;
            (void)slots_state_.request_cancel(registration_ptr->h); // dead: its terminal completion just frees it
            if (!registration_ptr->in_flight) {
                finish(registration_ptr);
            } else if (!registration_ptr->cancel_sent) {
                registration_ptr->cancel_sent = true;
                queue_cancel_of(registration_ptr->user_data());
            }
        }
        if (wake_armed_) queue_cancel_of(wake_user_data);

        for (unsigned failures = 0; inflight_ != 0 && failures < 10000;) {
            if (ring_.submit_and_wait(1) < 0) { ++failures; std::this_thread::yield(); }
            (void)reap(256);
        }
        // If the ring itself went bad (failures hit the cap) registrations that
        // are still in flight are deliberately leaked rather than freed under the kernel.
    }

    // Registrations that never reached the ring (registered but never
    // driven), plus their commands. Anything still in flight is skipped.
    template<typename Pool>
    inline void backend<Pool>::discard_unarmed() noexcept {
        for (command* cmd = cmds_.take_all(); cmd != nullptr;) {
            command* next = cmd->next;
            delete cmd;
            cmd = next;
        }
        for (auto& slot : slots_) {
            auto* registration_ptr = slot.load(std::memory_order_acquire);
            if (registration_ptr && !registration_ptr->in_flight) {
                slot.store(nullptr, std::memory_order_release);
                delete registration_ptr;
            }
        }
    }

}
