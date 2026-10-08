#pragma once

#include <cerrno>
#include <chrono>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace FCS::Worker::backend::epoll {

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
        for (auto& slot : slots_) delete slot.load(std::memory_order_relaxed);
        for (auto* retired : retired_handlers_) delete retired;
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
                if (wakeup_fd_ >= 0) { std::uint64_t one{1}; (void)!::write(wakeup_fd_, &one, sizeof(one)); }
                poller_.join();
            } else {
                this->service().unregister_poll_hook(poll_hook_);
            }
            reclaim();
        }
        generic_eventlooper<backend<Pool>, int, Pool>::stop_backend();
    }


    template<typename Pool>
    inline typename backend<Pool>::registration* backend<Pool>::find_or_create_slot_locked(int fd, source_kind kind, std::size_t& index_out, bool& is_new) {
        for (std::size_t i = 0; i < capacity; ++i) {
            auto* ptr = slots_[i].load(std::memory_order_acquire);
            if (ptr && ptr->fd == fd && ptr->active.load(std::memory_order_acquire)) {
                index_out = i;
                is_new = false;
                return ptr;
            }
        }

        auto* registration_ptr = new registration{};
        registration_ptr->fd = fd;
        registration_ptr->kind = kind;
        for (std::size_t i = 0; i < capacity; ++i) {
            registration* expected = nullptr;
            if (slots_[i].compare_exchange_strong(expected, registration_ptr, std::memory_order_release, std::memory_order_relaxed)) {
                registration_ptr->index = i;
                registration_ptr->generation = generations_[i].fetch_add(1, std::memory_order_relaxed) + 1;
                index_out = i;
                is_new = true;
                return registration_ptr;
            }
        }
        delete registration_ptr;
        return nullptr;
    }


    template<typename Pool>
    inline bool backend<Pool>::update_interest(registration& reg, bool is_new) noexcept {
        ::epoll_event ev{};
        ev.events = (reg.read_active.load(std::memory_order_relaxed) ? static_cast<std::uint32_t>(EPOLLIN) : 0u)
                  | (reg.write_active.load(std::memory_order_relaxed) ? static_cast<std::uint32_t>(EPOLLOUT) : 0u);
        ev.data.u32 = static_cast<std::uint32_t>(reg.index);
        return ::epoll_ctl(epoll_fd_, is_new ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, reg.fd, &ev) == 0;
    }


    template<typename Pool>
    inline void backend<Pool>::publish_handler(std::atomic<handler*>& slot, ::FCS::Worker::detail::callback_function<void()> fn) {
        auto* fresh = new handler{std::move(fn)};
        if (auto* old = slot.exchange(fresh, std::memory_order_acq_rel)) {
            retired_handlers_.push_back(old);
            retired_handler_count_.store(retired_handlers_.size(), std::memory_order_relaxed);
        }
    }


    template<typename Pool>
    inline void backend<Pool>::retire_direction(registration& reg, bool read) noexcept {
        std::lock_guard lock{slots_mutex_};
        (read ? reg.read_active : reg.write_active).store(false, std::memory_order_release);
        detach_direction(reg);
    }


    template<typename Pool>
    inline void backend<Pool>::detach_direction(registration& reg) noexcept {
        if (!reg.read_active.load(std::memory_order_acquire) && !reg.write_active.load(std::memory_order_acquire)) {
            reg.active.store(false, std::memory_order_release);
            scan_needed_.store(true, std::memory_order_release);
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, reg.fd, nullptr);
        } else {
            ::epoll_event ev{};
            ev.events = (reg.read_active.load(std::memory_order_relaxed) ? static_cast<std::uint32_t>(EPOLLIN) : 0u)
                      | (reg.write_active.load(std::memory_order_relaxed) ? static_cast<std::uint32_t>(EPOLLOUT) : 0u);
            ev.data.u32 = static_cast<std::uint32_t>(reg.index);
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, reg.fd, &ev);
        }
    }


    template<typename Pool>
    template<typename Tag, std::size_t Size, detail::recv_option Options, typename Reader, typename Source, typename Callback>
    subscription backend<Pool>::register_source(Source&& source, Reader&& reader, Callback&& callback) {
        static_assert(reader_callable<Size, std::remove_cvref_t<Reader>, detail::readiness_source<Options>>,
                      "Reader must be callable with a detail::readiness_source<Options>& and return read_result<Size>");

        const auto fd = detail::resolve_native(source);
        const auto kind = detail::resolve_kind(source);

        std::size_t index{};
        bool is_new{};
        // Held from find through the direction flag being set -- see the
        // threading-model comment in epoll.hpp.
        std::lock_guard setup_lock{slots_mutex_};
        auto* registration_ptr = find_or_create_slot_locked(fd, kind, index, is_new);
        if (!registration_ptr) return {};

        registration_ptr->read_channel_subscription = this->service().template subscribe<Tag>(std::forward<Callback>(callback));
        publish_handler(registration_ptr->on_readable, [this, registration_ptr, reader_fn = std::forward<Reader>(reader)]() mutable {
            const auto submitted_at = std::chrono::steady_clock::now();
            detail::readiness_source<Options> src{registration_ptr->fd, registration_ptr->kind};
            auto result = std::invoke(reader_fn, src);

            completion<Size> c{};
            c.user_data = registration_ptr->index;
            c.generation = registration_ptr->generation;
            c.kind = registration_ptr->kind;
            c.op = completion_op::read;
            c.submitted_at = submitted_at;
            c.completed_at = std::chrono::steady_clock::now();
            c.result = result;

            bool retire = false;
            if (src.failed()) {
                c.status = completion_status::error;
                c.raw_status = src.raw_error();
                c.error = detail::from_native_error(src.raw_error());
                retire = true;
            } else if (result.size == 0) {
                c.status = completion_status::closed;
                retire = true;
            } else {
                c.status = completion_status::ok;
                c.raw_status = static_cast<std::int64_t>(result.size);
            }

            // Post first, then retire: retire_direction() can make the
            // registration reclaimable, and reclaim() would otherwise be able to
            // free it (and its channel subscription) before this completion is
            // even queued. read_ended makes that eventual free retire() rather
            // than cancel() the subscription.
            if (retire) registration_ptr->read_ended.store(true, std::memory_order_release);
            (void)this->service().template post<Tag>(c);
            if (retire) retire_direction(*registration_ptr, /*read=*/true);
        });

        registration_ptr->read_active.store(true, std::memory_order_release);
        if (!update_interest(*registration_ptr, is_new)) {
            registration_ptr->read_active.store(false, std::memory_order_release);
            if (is_new) { slots_[index].store(nullptr, std::memory_order_release); delete registration_ptr; }
            return {};
        }

        return subscription{this, &backend::cancel_read_slot, index};
    }


    template<typename Pool>
    template<typename Tag, std::size_t Size, typename Writer, typename Sink, typename Callback>
    subscription backend<Pool>::register_sink(Sink&& sink, Writer&& writer, Callback&& callback) {
        static_assert(writer_callable<std::remove_cvref_t<Writer>, detail::readiness_sink>,
                      "Writer must be callable with a detail::readiness_sink& and return a byte count");

        const auto fd = detail::resolve_native(sink);
        const auto kind = detail::resolve_kind(sink);

        std::size_t index{};
        bool is_new{};
        std::lock_guard setup_lock{slots_mutex_};
        auto* registration_ptr = find_or_create_slot_locked(fd, kind, index, is_new);
        if (!registration_ptr) return {};

        registration_ptr->write_channel_subscription = this->service().template subscribe<Tag>(std::forward<Callback>(callback));
        publish_handler(registration_ptr->on_writable, [this, registration_ptr, writer_fn = std::forward<Writer>(writer)]() mutable {
            const auto submitted_at = std::chrono::steady_clock::now();
            detail::readiness_sink snk{registration_ptr->fd, registration_ptr->kind};
            const auto written = std::invoke(writer_fn, snk);

            completion<Size> c{};
            c.user_data = registration_ptr->index;
            c.generation = registration_ptr->generation;
            c.kind = registration_ptr->kind;
            c.op = completion_op::write;
            c.submitted_at = submitted_at;
            c.completed_at = std::chrono::steady_clock::now();
            c.result.size = written; // bytes actually written this call; buffer unused for writes

            bool retire = false;
            if (snk.failed()) {
                c.status = completion_status::error;
                c.raw_status = snk.raw_error();
                c.error = detail::from_native_error(snk.raw_error());
                retire = true;
            } else {
                c.status = completion_status::ok;
                c.raw_status = static_cast<std::int64_t>(written);
            }

            if (retire) registration_ptr->write_ended.store(true, std::memory_order_release);
            (void)this->service().template post<Tag>(c);
            if (retire) retire_direction(*registration_ptr, /*read=*/false);
        });

        registration_ptr->write_active.store(true, std::memory_order_release);
        if (!update_interest(*registration_ptr, is_new)) {
            registration_ptr->write_active.store(false, std::memory_order_release);
            if (is_new) { slots_[index].store(nullptr, std::memory_order_release); delete registration_ptr; }
            return {};
        }

        return subscription{this, &backend::cancel_write_slot, index};
    }


    template<typename Pool>
    inline bool backend<Pool>::cancel_read_slot(void* owner, std::size_t slot) noexcept {
        auto* self = static_cast<backend*>(owner);
        // Same reasoning as find_or_create_slot(): guards this dereference
        // against a concurrent reclaim() freeing the same registration.
        // cancel() can be called from any thread, not just a poller one.
        std::lock_guard lock{self->slots_mutex_};
        auto* registration_ptr = self->slots_[slot].load(std::memory_order_acquire);
        if (!registration_ptr) return false;
        bool expected = true;
        if (!registration_ptr->read_active.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) return false;
        self->detach_direction(*registration_ptr);
        if (self->wakeup_fd_ >= 0) { std::uint64_t one{1}; (void)!::write(self->wakeup_fd_, &one, sizeof(one)); }
        return true;
    }


    template<typename Pool>
    inline bool backend<Pool>::cancel_write_slot(void* owner, std::size_t slot) noexcept {
        auto* self = static_cast<backend*>(owner);
        std::lock_guard lock{self->slots_mutex_};
        auto* registration_ptr = self->slots_[slot].load(std::memory_order_acquire);
        if (!registration_ptr) return false;
        bool expected = true;
        if (!registration_ptr->write_active.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) return false;
        self->detach_direction(*registration_ptr);
        if (self->wakeup_fd_ >= 0) { std::uint64_t one{1}; (void)!::write(self->wakeup_fd_, &one, sizeof(one)); }
        return true;
    }

    // Handles one epoll_event: retires whichever direction ERR/HUP leaves
    // without a clean signal of its own, then runs on_readable/on_writable
    // for whichever direction is both flagged ready and still active.
    template<typename Pool>
    inline void backend<Pool>::dispatch(registration& reg, std::uint32_t flags) noexcept {
        if ((flags & (EPOLLERR | EPOLLHUP)) && !(flags & EPOLLIN) && reg.read_active.load(std::memory_order_acquire))
            retire_direction(reg, /*read=*/true);
        if ((flags & (EPOLLERR | EPOLLHUP)) && !(flags & EPOLLOUT) && reg.write_active.load(std::memory_order_acquire))
            retire_direction(reg, /*read=*/false);
        // The handler pointer is loaded once and used for the whole call: a user
        // thread re-registering this direction publishes a *new* handler and
        // retires this one, which reclaim() can only free after this batch.
        if ((flags & EPOLLIN) && reg.read_active.load(std::memory_order_acquire))
            if (auto* h = reg.on_readable.load(std::memory_order_acquire)) h->fn();
        if ((flags & EPOLLOUT) && reg.write_active.load(std::memory_order_acquire))
            if (auto* h = reg.on_writable.load(std::memory_order_acquire)) h->fn();
    }


    template<typename Pool>
    inline void backend<Pool>::run_loop() {
        std::array<::epoll_event, 64> events{};
        for (;;) {
            const auto count = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
            if (count < 0) { if (errno == EINTR) continue; break; }

            bool woke = false;
            for (std::size_t i{}, n = static_cast<std::size_t>(count); i < n; ++i) {
                const auto index = events[i].data.u32;
                if (index == static_cast<std::uint32_t>(capacity)) {
                    std::uint64_t drained{};
                    (void)!::read(wakeup_fd_, &drained, sizeof(drained));
                    woke = true;
                    continue;
                }

                auto* registration_ptr = slots_[index].load(std::memory_order_acquire);
                if (!registration_ptr || !registration_ptr->active.load(std::memory_order_acquire)) continue;
                dispatch(*registration_ptr, events[i].events);
            }

            if (!woke) continue;
            reclaim();
            if (!polling_.load(std::memory_order_acquire)) break;
        }
    }


    template<typename Pool>
    inline bool backend<Pool>::poll_once(void* owner) noexcept {
        auto* self = static_cast<backend*>(owner);
        std::array<::epoll_event, 64> events{};
        const auto count = ::epoll_wait(self->epoll_fd_, events.data(), static_cast<int>(events.size()), 0);
        if (count <= 0) return false;

        bool progressed = false;
        for (std::size_t i{}, n = static_cast<std::size_t>(count); i < n; ++i) {
            const auto index = events[i].data.u32;
            if (index == static_cast<std::uint32_t>(capacity)) {
                std::uint64_t drained{};
                (void)!::read(self->wakeup_fd_, &drained, sizeof(drained));
                continue;
            }

            auto* registration_ptr = self->slots_[index].load(std::memory_order_acquire);
            if (!registration_ptr || !registration_ptr->active.load(std::memory_order_acquire)) continue;
            self->dispatch(*registration_ptr, events[i].events);
            progressed = true;
        }

        self->reclaim();
        return progressed;
    }


    template<typename Pool>
    inline void backend<Pool>::reclaim() noexcept {
        // Only ever called by the single thread that's dispatching for this
        // backend, *between* batches (or after it has stopped), so nothing freed
        // here can be mid-dispatch. The early-out keeps the common case -- called
        // once per non-empty poll -- to two relaxed loads: no mutex, no slot scan.
        if (!scan_needed_.load(std::memory_order_relaxed) && retired_handler_count_.load(std::memory_order_relaxed) == 0) return;

        std::lock_guard lock{slots_mutex_};
        if (scan_needed_.exchange(false, std::memory_order_acq_rel)) {
            for (auto& slot : slots_) {
                auto* registration_ptr = slot.load(std::memory_order_acquire);
                if (registration_ptr && !registration_ptr->active.load(std::memory_order_acquire)) {
                    slot.store(nullptr, std::memory_order_release);
                    delete registration_ptr;
                }
            }
        }
        for (auto* retired : retired_handlers_) delete retired;
        retired_handlers_.clear();
        retired_handler_count_.store(0, std::memory_order_relaxed);
    }

}
