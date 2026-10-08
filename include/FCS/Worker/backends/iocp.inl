#pragma once

#include <chrono>
#include <new>

namespace FCS::Worker::backend::iocp {

    // ---------------------------------------------------------------- lifecycle

    template<typename Pool>
    inline backend<Pool>::backend(Pool& service) : generic_eventlooper<backend<Pool>, HANDLE, Pool>(service) {
        // Created up front, not lazily: a registration may be made before
        // start() (datagram subscriptions aren't accept-gated), and every one
        // must land on the one port the driver will later watch.
        port_ = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);
        if (!port_) setup_error_.store(static_cast<int>(::GetLastError()), std::memory_order_relaxed);
    }

    template<typename Pool>
    inline backend<Pool>::~backend() {
        stop_backend();
        discard_unarmed();
        if (port_) ::CloseHandle(port_);
    }


    template<typename Pool>
    inline void backend<Pool>::start_backend() {
        generic_eventlooper<backend<Pool>, HANDLE, Pool>::start_backend();
        bool expected = false;
        if (!polling_.compare_exchange_strong(expected, true)) return;
        if (!port_) { polling_.store(false, std::memory_order_release); return; }

        inflight_ = 0;
        stop_seen_ = false;
        if (this->service().execution_policy() == execution::policy::dedicated_poller) {
            poller_ = std::thread([this] { run_loop(); });
        } else {
            // execution::shared_worker -- no dedicated OS thread; ordinary
            // pool workers drive the port via poll_once() instead.
            poll_hook_ = this->service().register_poll_hook(this, &backend::poll_once);
        }
    }


    template<typename Pool>
    inline void backend<Pool>::stop_backend() noexcept {
        const bool was_polling = polling_.exchange(false);
        if (poller_.joinable()) {
            // The definitive stop signal. run_loop() ends with shutdown_port(),
            // which cancels and drains everything before anything is freed.
            if (port_) ::PostQueuedCompletionStatus(port_, 0, key_stop, nullptr);
            poller_.join();
        } else if (was_polling) {
            // Waits out any poll_once() already in flight on another worker
            // before returning -- see poll_registry::unregister(). From here
            // this thread is the only driver, so it may run the shutdown
            // itself (pool workers may already be gone by now anyway).
            this->service().unregister_poll_hook(poll_hook_);
            shutdown_port();
        }
        generic_eventlooper<backend<Pool>, HANDLE, Pool>::stop_backend();
    }

    // ----------------------------------------------------------- any-thread API


    template<typename Pool>
    template<typename Tag, std::size_t Size, common::recv_option Options, typename Reader, typename Source, typename Callback>
    subscription backend<Pool>::register_source(Source&& source, Reader&& reader, Callback&& callback) {
        static_assert(reader_callable<Size, std::remove_cvref_t<Reader>, common::completed_source>,
                      "Reader must be callable with a detail::completed_source& and return read_result<Size>");

        const auto native = common::resolve_native(source);
        using native_t = std::remove_cvref_t<decltype(native)>;

        const auto claimed = slots_state_.claim();
        if (!claimed) return {};

        auto* registration_ptr = new registration{};
        registration_ptr->h = *claimed;
        registration_ptr->kind = common::resolve_kind(source);
        registration_ptr->dir = direction::read;
        if constexpr (std::is_same_v<native_t, SOCKET>) registration_ptr->socket = native;
        registration_ptr->handle = common::to_iocp_handle(native);
        registration_ptr->recv_options = common::to_native_recv_flags(Options);
        registration_ptr->buffer_size = Size;
        registration_ptr->buffer = std::make_unique<std::byte[]>(Size);
        registration_ptr->op.owner = registration_ptr;
        registration_ptr->channel_subscription = this->service().template subscribe<Tag>(std::forward<Callback>(callback));
        registration_ptr->on_event = [this, reader_fn = std::forward<Reader>(reader)](registration& reg, DWORD transferred, bool ok, DWORD error_code) mutable -> action {
            completion<Size> c{};
            c.user_data = reg.h.index;
            c.generation = reg.h.generation;
            c.kind = reg.kind;
            c.op = completion_op::read;
            c.submitted_at = reg.submitted_at;

            action next = action::keep;
            if (!ok) {
                // A pipe's EOF surfaces as an error on ReadFile; epoll's read()==0
                // is "closed", so report it the same way.
                if (reg.kind == source_kind::io && (error_code == ERROR_BROKEN_PIPE || error_code == ERROR_HANDLE_EOF)) {
                    c.status = completion_status::closed;
                } else {
                    c.status = completion_status::error;
                    c.raw_status = static_cast<std::int64_t>(error_code);
                    c.error = common::from_native_error(error_code);
                }
                next = action::retire;
            } else if (transferred == 0) {
                // A successful completion with zero bytes transferred is the
                // standard graceful-close signal for both WSARecv and ReadFile,
                // exactly like recv()/read() returning 0 on POSIX.
                c.status = completion_status::closed;
                next = action::retire;
            } else {
                common::completed_source src{reg.buffer.get(), static_cast<std::size_t>(transferred),
                                             reg.kind == source_kind::datagram ? &reg.peer_addr : nullptr};
                c.result = std::invoke(reader_fn, src);
                c.status = completion_status::ok;
                c.raw_status = static_cast<std::int64_t>(transferred);
            }
            c.completed_at = std::chrono::steady_clock::now();
            (void)this->service().template post<Tag>(c);
            return next;
        };

        const auto handle = registration_ptr->h;
        if (!associate(registration_ptr->handle) || !publish(registration_ptr)) {
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

        const auto native = common::resolve_native(sink);
        using native_t = std::remove_cvref_t<decltype(native)>;
        // Only a Winsock SOCKET has the zero-byte-send readiness signal this
        // relies on (see the class comment); refuse anything else outright
        // rather than hand back a subscription that can never fire.
        if constexpr (!std::is_same_v<native_t, SOCKET>) {
            return {};
        } else {
            const auto kind = common::resolve_kind(sink);
            if (kind == source_kind::io) return {};

            const auto claimed = slots_state_.claim();
            if (!claimed) return {};

            auto* registration_ptr = new registration{};
            registration_ptr->h = *claimed;
            registration_ptr->kind = kind;
            registration_ptr->dir = direction::write;
            registration_ptr->socket = native;
            registration_ptr->handle = common::to_iocp_handle(native);
            registration_ptr->op.owner = registration_ptr;
            registration_ptr->channel_subscription = this->service().template subscribe<Tag>(std::forward<Callback>(callback));
            registration_ptr->on_event = [this, writer_fn = std::forward<Writer>(writer)](registration& reg, DWORD, bool ok, DWORD error_code) mutable -> action {
                completion<Size> c{};
                c.user_data = reg.h.index;
                c.generation = reg.h.generation;
                c.kind = reg.kind;
                c.op = completion_op::write;

                if (!ok) {
                    c.submitted_at = reg.submitted_at;
                    c.completed_at = std::chrono::steady_clock::now();
                    c.status = completion_status::error;
                    c.raw_status = static_cast<std::int64_t>(error_code);
                    c.error = common::from_native_error(error_code);
                    (void)this->service().template post<Tag>(c);
                    return action::retire;
                }

                c.submitted_at = std::chrono::steady_clock::now();
                common::readiness_sink snk{reg.socket, reg.kind};
                const auto written = std::invoke(writer_fn, snk);
                c.completed_at = std::chrono::steady_clock::now();
                c.result.size = written; // bytes actually written this call; buffer unused for writes

                action next = action::keep;
                if (snk.failed()) {
                    c.status = completion_status::error;
                    c.raw_status = snk.raw_error();
                    c.error = common::from_native_error(static_cast<DWORD>(snk.raw_error()));
                    next = action::retire;
                } else {
                    c.status = completion_status::ok;
                    c.raw_status = static_cast<std::int64_t>(written);
                }
                (void)this->service().template post<Tag>(c);
                return next;
            };

            const auto handle = registration_ptr->h;
            if (!associate(registration_ptr->handle) || !publish(registration_ptr)) {
                (void)slots_state_.request_cancel(handle);
                slots_state_.release(handle.index);
                delete registration_ptr;
                return {};
            }
            return subscription{this, &backend::cancel_slot, table::pack(handle)};
        }
    }

    // Associates a handle with the port. A second registration on the same
    // socket (the other duplex direction) is already associated; Windows
    // reports that as ERROR_INVALID_PARAMETER -- see the class comment.
    template<typename Pool>
    inline bool backend<Pool>::associate(HANDLE native) noexcept {
        if (!port_) return false;
        if (::CreateIoCompletionPort(native, port_, 0, 0) != nullptr) return true;
        return ::GetLastError() == ERROR_INVALID_PARAMETER;
    }

    // Hands a fully-initialized registration to the driver. Only the store and
    // the posted command are visible to other threads.
    template<typename Pool>
    inline bool backend<Pool>::publish(registration* registration_ptr) noexcept {
        auto* cmd = new (std::nothrow) command{};
        if (!cmd) return false;
        cmd->what = command::kind::arm;
        cmd->h = registration_ptr->h;
        slots_[registration_ptr->h.index].store(registration_ptr, std::memory_order_release);
        pending_commands_.fetch_add(1, std::memory_order_relaxed);
        if (!::PostQueuedCompletionStatus(port_, 0, key_command, cmd)) {
            pending_commands_.fetch_sub(1, std::memory_order_relaxed);
            slots_[registration_ptr->h.index].store(nullptr, std::memory_order_release);
            delete cmd;
            return false;
        }
        return true;
    }


    template<typename Pool>
    inline void backend<Pool>::post_command(command::kind what, table::handle h) noexcept {
        auto* cmd = new (std::nothrow) command{};
        if (cmd) {
            cmd->what = what;
            cmd->h = h;
            pending_commands_.fetch_add(1, std::memory_order_relaxed);
            if (port_ && ::PostQueuedCompletionStatus(port_, 0, key_command, cmd)) return;
            pending_commands_.fetch_sub(1, std::memory_order_relaxed);
            delete cmd;
        }
        // Out of memory (or the port is gone): the slot is already marked
        // cancelled; make the driver find it by scanning instead of by command.
        sweep_needed_.store(true, std::memory_order_release);
        if (port_) ::PostQueuedCompletionStatus(port_, 0, key_command, nullptr); // wake-only
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

    // ------------------------------------------------------------- driver side


    template<typename Pool>
    inline bool backend<Pool>::arm(registration& registration_ref) noexcept {
        registration_ref.op = op_block{};
        registration_ref.op.owner = &registration_ref;
        registration_ref.submitted_at = std::chrono::steady_clock::now();
        registration_ref.in_flight = true;
        ++inflight_;

        bool pending{};
        if (registration_ref.dir == direction::write) {
            // Zero-byte overlapped send: its completion is the "writable" signal.
            WSABUF empty{0, nullptr};
            DWORD sent{};
            const auto rc = ::WSASend(registration_ref.socket, &empty, 1, &sent, 0, &registration_ref.op, nullptr);
            pending = !(rc == SOCKET_ERROR && ::WSAGetLastError() != WSA_IO_PENDING);
        } else if (registration_ref.kind == source_kind::datagram) {
            // WSARecvFrom captures the sender into peer_addr/peer_len -- reset
            // peer_len first, since the API overwrites it in place with the
            // actual address length, and a stale shorter value from a previous
            // call would truncate a later, larger sender address.
            registration_ref.wsa_buffer.buf = reinterpret_cast<CHAR*>(registration_ref.buffer.get());
            registration_ref.wsa_buffer.len = static_cast<ULONG>(registration_ref.buffer_size);
            registration_ref.wsa_flags = static_cast<DWORD>(registration_ref.recv_options);
            registration_ref.peer_len = sizeof(sockaddr_storage);
            DWORD received{};
            const auto rc = ::WSARecvFrom(registration_ref.socket, &registration_ref.wsa_buffer, 1, &received,
                                          &registration_ref.wsa_flags, reinterpret_cast<sockaddr*>(&registration_ref.peer_addr),
                                          &registration_ref.peer_len, &registration_ref.op, nullptr);
            pending = !(rc == SOCKET_ERROR && ::WSAGetLastError() != WSA_IO_PENDING);
        } else if (registration_ref.kind == source_kind::network) {
            // Sockets aren't guaranteed to support ReadFile (only IFS Winsock
            // providers do); WSARecv is the portable way to arm a real,
            // buffer-backed overlapped read on a SOCKET.
            registration_ref.wsa_buffer.buf = reinterpret_cast<CHAR*>(registration_ref.buffer.get());
            registration_ref.wsa_buffer.len = static_cast<ULONG>(registration_ref.buffer_size);
            registration_ref.wsa_flags = static_cast<DWORD>(registration_ref.recv_options);
            DWORD received{};
            const auto rc = ::WSARecv(registration_ref.socket, &registration_ref.wsa_buffer, 1, &received,
                                      &registration_ref.wsa_flags, &registration_ref.op, nullptr);
            pending = !(rc == SOCKET_ERROR && ::WSAGetLastError() != WSA_IO_PENDING);
        } else {
            DWORD received{};
            const auto ok = ::ReadFile(registration_ref.handle, registration_ref.buffer.get(),
                                       static_cast<DWORD>(registration_ref.buffer_size), &received, &registration_ref.op);
            pending = ok || ::GetLastError() == ERROR_IO_PENDING;
        }

        if (!pending) {
            // A genuine synchronous failure: the operation never started, so
            // no completion packet will ever arrive for it.
            registration_ref.in_flight = false;
            --inflight_;
        }
        return pending;
    }


    template<typename Pool>
    inline void backend<Pool>::request_io_cancel(registration& registration_ref) noexcept {
        if (registration_ref.cancel_sent) return;
        registration_ref.cancel_sent = true;
        (void)::CancelIoEx(registration_ref.handle, &registration_ref.op); // ERROR_NOT_FOUND = already completing; its packet is queued
    }

    // Frees a registration. Callers guarantee no operation for it is
    // outstanding -- that is what makes it safe to release the OVERLAPPED and
    // buffer the kernel was using, and to let the slot be claimed again.
    template<typename Pool>
    inline void backend<Pool>::finish(registration* registration_ptr, bool deliver_pending) noexcept {
        // See io_uring::backend::finish(): keep a just-posted terminal completion alive.
        if (deliver_pending) (void)registration_ptr->channel_subscription.retire();
        const auto h = registration_ptr->h;
        slots_[h.index].store(nullptr, std::memory_order_release);
        (void)slots_state_.retire(h);
        delete registration_ptr;
        slots_state_.release(h.index);
    }


    template<typename Pool>
    inline void backend<Pool>::handle_command(command* cmd) noexcept {
        pending_commands_.fetch_sub(1, std::memory_order_relaxed);
        auto* registration_ptr = cmd->h.index < capacity ? slots_[cmd->h.index].load(std::memory_order_acquire) : nullptr;
        if (registration_ptr && same_generation(registration_ptr->h.generation, cmd->h.generation)) {
            if (cmd->what == command::kind::arm) {
                if (!slots_state_.live(registration_ptr->h)) {
                    finish(registration_ptr);                         // cancelled before it ever armed
                } else if (!arm(*registration_ptr)) {
                    (void)slots_state_.retire(registration_ptr->h);   // can never fire
                    finish(registration_ptr);
                }
            } else if (registration_ptr->in_flight) {
                request_io_cancel(*registration_ptr);                 // its aborted completion frees it
            } else {
                finish(registration_ptr);
            }
        }
        delete cmd;
    }

    // Fallback for a cancel command that couldn't be allocated or posted.
    template<typename Pool>
    inline void backend<Pool>::sweep_cancelled() noexcept {
        for (auto& slot : slots_) {
            auto* registration_ptr = slot.load(std::memory_order_acquire);
            if (!registration_ptr || slots_state_.live(registration_ptr->h)) continue;
            if (!registration_ptr->in_flight) finish(registration_ptr);
            else request_io_cancel(*registration_ptr);
        }
    }


    template<typename Pool>
    inline void backend<Pool>::handle_completion(op_block* op, DWORD transferred, LONG status) noexcept {
        auto* registration_ptr = op->owner;
        if (inflight_ != 0) --inflight_;
        registration_ptr->in_flight = false;

        if (!slots_state_.live(registration_ptr->h)) {
            // Cancelled: this is its terminal completion (one operation in
            // flight at a time, nothing is re-armed once cancelled), so the
            // kernel is done with the OVERLAPPED and buffer -- safe to free.
            finish(registration_ptr);
            return;
        }

        bool ok = status == 0;
        DWORD error_code = 0;
        if (!ok) {
            // The entry only carries an NTSTATUS; ask for the Win32/Winsock
            // code (a reset connection is WSAECONNRESET, not NETNAME_DELETED).
            DWORD ignored{};
            DWORD flags{};
            const BOOL fetched = registration_ptr->kind == source_kind::io
                ? ::GetOverlappedResult(registration_ptr->handle, &registration_ptr->op, &ignored, FALSE)
                : ::WSAGetOverlappedResult(registration_ptr->socket, &registration_ptr->op, &ignored, FALSE, &flags);
            if (fetched) ok = true; // raced to success; treat as such
            else error_code = ::GetLastError();
        }

        const action next = registration_ptr->on_event(*registration_ptr, transferred, ok, error_code);
        if (next == action::keep && slots_state_.live(registration_ptr->h) && arm(*registration_ptr)) return;

        (void)slots_state_.retire(registration_ptr->h);
        finish(registration_ptr, /*deliver_pending=*/next == action::retire);
    }

    // Returns true if it did real work (a command or an I/O completion).
    template<typename Pool>
    inline bool backend<Pool>::handle_entry(const OVERLAPPED_ENTRY& entry) noexcept {
        if (entry.lpCompletionKey == key_stop) { stop_seen_ = true; return false; }
        if (entry.lpCompletionKey == key_command) {
            if (!entry.lpOverlapped) { // wake-only packet from a failed command allocation
                if (sweep_needed_.exchange(false, std::memory_order_acq_rel)) sweep_cancelled();
                return true;
            }
            handle_command(static_cast<command*>(entry.lpOverlapped));
            return true;
        }
        if (!entry.lpOverlapped) return false;
        handle_completion(static_cast<op_block*>(entry.lpOverlapped), entry.dwNumberOfBytesTransferred,
                          static_cast<LONG>(entry.Internal));
        return true;
    }


    template<typename Pool>
    inline void backend<Pool>::run_loop() {
        std::array<OVERLAPPED_ENTRY, batch> entries{};
        unsigned failures = 0;
        while (!stop_seen_) {
            ULONG count = 0;
            // One syscall drains up to `batch` packets, and blocks until at
            // least one is available -- the IOCP analogue of epoll_wait with a
            // 64-event buffer.
            if (!::GetQueuedCompletionStatusEx(port_, entries.data(), batch, &count, INFINITE, FALSE)) {
                if (++failures > 1000) break; // the port itself is broken; stop rather than spin
                std::this_thread::yield();
                continue;
            }
            failures = 0;
            for (ULONG i = 0; i < count; ++i) (void)handle_entry(entries[i]);
            if (sweep_needed_.load(std::memory_order_acquire) && sweep_needed_.exchange(false, std::memory_order_acq_rel)) sweep_cancelled();
        }
        shutdown_port();
    }

    // execution::shared_worker path: one bounded, non-blocking pass, called
    // from some pool worker's own loop. poll_registry guarantees at most one
    // thread is inside this per backend at a time -- exactly the single-driver
    // contract the registrations need.
    template<typename Pool>
    inline bool backend<Pool>::poll_once(void* owner) noexcept {
        auto* self = static_cast<backend*>(owner);
        if (!self->port_) return false;

        std::array<OVERLAPPED_ENTRY, batch> entries{};
        ULONG count = 0;
        if (!::GetQueuedCompletionStatusEx(self->port_, entries.data(), batch, &count, 0, FALSE)) return false;
        bool progressed = false;
        for (ULONG i = 0; i < count; ++i) progressed = self->handle_entry(entries[i]) || progressed;
        return progressed;
    }

    // Cancels everything still in flight and reaps until every started
    // operation has produced its completion. Only then is anything freed, so
    // the kernel can never complete into memory we've released.
    template<typename Pool>
    inline void backend<Pool>::shutdown_port() noexcept {
        if (!port_) return;

        for (auto& slot : slots_) {
            auto* registration_ptr = slot.load(std::memory_order_acquire);
            if (!registration_ptr) continue;
            (void)slots_state_.request_cancel(registration_ptr->h); // dead: its terminal completion just frees it
            if (!registration_ptr->in_flight) finish(registration_ptr);
            else request_io_cancel(*registration_ptr);
        }

        std::array<OVERLAPPED_ENTRY, batch> entries{};
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while ((inflight_ != 0 || pending_commands_.load(std::memory_order_relaxed) != 0) && std::chrono::steady_clock::now() < deadline) {
            ULONG count = 0;
            if (!::GetQueuedCompletionStatusEx(port_, entries.data(), batch, &count, 100, FALSE)) continue; // timeout: re-check, re-cancel nothing
            for (ULONG i = 0; i < count; ++i) (void)handle_entry(entries[i]);
        }
        // If an operation never completed (a driver that ignores cancellation),
        // its registration is deliberately leaked by discard_unarmed() rather
        // than freed under the kernel.
        stop_seen_ = false;
    }

    // Registrations that never reached the port (registered but never
    // driven). Anything still in flight is skipped.
    template<typename Pool>
    inline void backend<Pool>::discard_unarmed() noexcept {
        for (auto& slot : slots_) {
            auto* registration_ptr = slot.load(std::memory_order_acquire);
            if (registration_ptr && !registration_ptr->in_flight) {
                slot.store(nullptr, std::memory_order_release);
                delete registration_ptr;
            }
        }
    }

}
