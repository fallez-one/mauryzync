#pragma once

#include "../completion.hpp"
#include "../eventloop.hpp"
#include "../execution.hpp"
#include "detail/io_classifier.hpp"
#include "detail/source_types.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace FCS::Worker::backend::epoll {

    // epoll only reports readiness; the poller does the real read/write itself
    // the instant it fires, wrapping the result into a uniform completion<Size>.
    //
    // Full duplex: register_source() and register_sink() on the same fd share
    // one registration and epoll entry (EPOLLIN | EPOLLOUT) instead of
    // colliding on EPOLL_CTL_ADD. Each direction has its own flag, callback and
    // subscription, torn down independently; the epoll entry itself is removed
    // once neither is active. Registering both directions of one not-yet-known
    // fd from two threads at once is unsupported -- call both from the same
    // accepting site, same as register_source()'s existing assumption.
    //
    // Threading model -- one invariant everything below leans on: at most one
    // thread is ever inside dispatch() for a given backend. That's the
    // dedicated poller thread, or (shared_worker) whichever pool worker holds
    // this backend's poll_registry `busy` flag; `between_num_threads` lets
    // several workers *try* to poll, but they serialize per backend. reclaim()
    // is only ever called by that same thread between batches (or after it has
    // stopped), so anything reclaim() frees can't be mid-dispatch.
    //
    // slots_mutex_ guards the cold paths against user threads: registration
    // setup (find_or_create_slot_locked() through the direction flag being
    // set), cancel_*_slot(), retire_direction(), and reclaim(). It is never
    // taken on the per-event dispatch()/handler path. It serializes user-thread
    // registration/cancellation against reclaim() freeing a registration
    // (TSan-caught use-after-free), and makes "both directions inactive =>
    // registration retired" atomic with a registration re-attaching a direction.
    //
    // Handlers (the on_readable/on_writable closures) are immutable heap objects
    // behind an atomic pointer, never reassigned in place: re-registering a
    // direction on a live registration (full duplex keeps it alive) publishes a
    // fresh handler and retires the old one to retired_handlers_, which only
    // reclaim() frees. Assigning over a std::function the poller was still
    // executing (previously: register_sink() reassigning on_writable) was a
    // reproducible heap-use-after-free.
    class backend final : public generic_eventlooper<backend, int> {
    public:
        explicit backend(pool_service<>& service);
        ~backend();

        void start_backend();
        void stop_backend() noexcept;

        template<typename Tag, std::size_t Size, detail::recv_option Options = detail::recv_option::none,
                 typename Reader, typename Source, typename Callback>
        [[nodiscard]] subscription register_source(Source&& source, Reader&& reader, Callback&& callback);

        template<typename Tag, std::size_t Size, typename Writer, typename Sink, typename Callback>
        [[nodiscard]] subscription register_sink(Sink&& sink, Writer&& writer, Callback&& callback);

    private:
        // Immutable once published; see the threading-model comment above.
        struct handler {
            ::FCS::Worker::detail::callback_function<void()> fn;
        };

        struct registration {
            int fd{-1};
            std::size_t index{};
            std::uint32_t generation{};
            source_kind kind{source_kind::io};
            std::atomic_bool active{true};        // false once neither direction is active -- reclaimable
            std::atomic_bool read_active{false};
            std::atomic_bool write_active{false};
            std::atomic<handler*> on_readable{nullptr};
            std::atomic<handler*> on_writable{nullptr};
            // Set by a handler just before it retires its direction because the
            // source ended on its own (EOF/error) and it already post()ed that
            // terminal completion. ~registration() then retire()s the channel
            // subscription instead of cancel()ing it, so that completion --
            // still queued in the pool -- is delivered, not dropped (see
            // subscription::retire()).
            std::atomic_bool read_ended{false};
            std::atomic_bool write_ended{false};
            subscription read_channel_subscription;
            subscription write_channel_subscription;

            registration() = default;
            registration(const registration&) = delete;
            registration& operator=(const registration&) = delete;
            ~registration() {
                if (read_ended.load(std::memory_order_relaxed)) (void)read_channel_subscription.retire();
                if (write_ended.load(std::memory_order_relaxed)) (void)write_channel_subscription.retire();
                delete on_readable.load(std::memory_order_relaxed);
                delete on_writable.load(std::memory_order_relaxed);
            }
        };

        static constexpr std::size_t capacity = 1024;

        // Scan for an existing active registration on `fd`; claims a fresh slot
        // if none is found. Caller must hold slots_mutex_ and keep holding it
        // until its direction flag is set, so a concurrent retire can't mark the
        // registration inactive (and reclaim() free it) in between. Doesn't touch
        // epoll itself -- the caller sets its direction's flag, then calls
        // update_interest() (ADD if `is_new`, MOD otherwise).
        [[nodiscard]] registration* find_or_create_slot_locked(int fd, source_kind kind, std::size_t& index_out, bool& is_new);
        [[nodiscard]] bool update_interest(registration& reg, bool is_new) noexcept;
        // Swaps in a fresh handler and retires the previous one (never freed
        // here). Caller holds slots_mutex_.
        void publish_handler(std::atomic<handler*>& slot, ::FCS::Worker::detail::callback_function<void()> fn);
        // Recomputes epoll interest from whichever direction flag is still
        // set (caller already cleared its own); removes the epoll entry and
        // marks the registration reclaimable once both are clear. Caller holds
        // slots_mutex_.
        void detach_direction(registration& reg) noexcept;
        // Clears one direction and detaches, taking slots_mutex_ itself -- for
        // the poller's own retire paths (EOF/error), which don't hold it.
        void retire_direction(registration& reg, bool read) noexcept;
        void dispatch(registration& reg, std::uint32_t flags) noexcept;

        [[nodiscard]] static bool cancel_read_slot(void* owner, std::size_t slot) noexcept;
        [[nodiscard]] static bool cancel_write_slot(void* owner, std::size_t slot) noexcept;
        void run_loop();                                     // execution::dedicated_poller
        [[nodiscard]] static bool poll_once(void* owner) noexcept; // execution::shared_worker
        void reclaim() noexcept;

        int epoll_fd_{-1};
        int wakeup_fd_{-1};
        std::thread poller_;
        std::atomic_bool polling_{};
        std::size_t poll_hook_{};
        std::array<std::atomic<registration*>, capacity> slots_{};
        std::array<std::atomic<std::uint32_t>, capacity> generations_{};
        std::mutex slots_mutex_;
        // Guarded by slots_mutex_. The two atomics are lock-free hints so the
        // per-poll reclaim() call is a couple of relaxed loads when there is
        // nothing to do, instead of a mutex plus a full slot scan every time.
        std::vector<handler*> retired_handlers_;
        std::atomic<std::size_t> retired_handler_count_{0};
        std::atomic_bool scan_needed_{false};
    };

    using eventlooper = backend;

}

#include "epoll.inl"
