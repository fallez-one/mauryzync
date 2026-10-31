#pragma once

#include "../completion.hpp"
#include "../eventloop.hpp"
#include "../execution.hpp"
#include "detail/io_classifier.hpp"
#include "detail/mpsc_stack.hpp"
#include "detail/slot_table.hpp"
#include "detail/source_types.hpp"

#include <sys/types.h>
#include <sys/event.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>
#include <type_traits>
#include <utility>

namespace FCS::Worker::backend::kqueue {

    // kqueue (FreeBSD, macOS, NetBSD, OpenBSD, DragonFly) -- the same readiness model, threading
    // model and observable contract as epoll::backend; see epoll.hpp for the long version. The
    // Reader/Writer, Source/Sink types, recv_option and completion<Size> are the very same
    // objects (readiness_source / readiness_sink), so code written against one backend runs
    // unchanged on the other.
    //
    // What differs underneath, and why none of it leaks into the API:
    //  * A knote is keyed by (fd, filter), so a read registration and a write registration on one
    //    fd are two independent knotes. There is no shared per-fd entry to maintain: one
    //    registration == one knote. Registering a direction that is already registered on the fd
    //    replaces it (newest wins), exactly as on epoll.
    //  * Level-triggered (no EV_CLEAR): the Reader runs again while data remains, like epoll.
    //  * EVFILT_READ reports EOF as EV_EOF *with* any remaining bytes still readable. We just run
    //    the Reader: it drains the bytes, then recv()==0 becomes the `closed` completion.
    //  * The wakeup is a self-pipe, not EVFILT_USER: OpenBSD has no EVFILT_USER, a pipe works on
    //    every kqueue, and it is the same "one fd the driver can sleep on" shape as epoll's eventfd.
    //  * A knote lives in the kernel's per-process kqueue and goes away when its fd is closed;
    //    `udata` carries the packed slot handle (index + generation), never a pointer, so an event
    //    that outlives its registration inside one batch is recognised as stale and dropped.
    //  * kevent()'s struct fields differ by OS (udata is `void*` or `intptr_t`, filter is signed
    //    or not); the two tiny helpers in kqueue.inl absorb that with `if constexpr`, not #ifdef.
    template<typename Pool = pool_service<>>
    class backend final : public generic_eventlooper<backend<Pool>, int, Pool> {
    public:
        explicit backend(Pool& service);
        ~backend();

        backend(const backend&) = delete;
        backend& operator=(const backend&) = delete;

        void start_backend();
        void stop_backend() noexcept;

        template<typename Tag, std::size_t Size, detail::recv_option Options = detail::recv_option::none,
                 typename Reader, typename Source, typename Callback>
        [[nodiscard]] subscription register_source(Source&& source, Reader&& reader, Callback&& callback);

        template<typename Tag, std::size_t Size, typename Writer, typename Sink, typename Callback>
        [[nodiscard]] subscription register_sink(Sink&& sink, Writer&& writer, Callback&& callback);

    private:
        static constexpr std::size_t capacity = 1024;
        using table = detail::slot_table<capacity>;

        enum class direction : std::uint8_t { read, write };
        enum class action : std::uint8_t { keep, retire };

        // One per register_source()/register_sink(). Everything but `h` is driver-owned once published.
        struct registration {
            table::handle h{};
            int fd{-1};
            source_kind kind{source_kind::io};
            direction dir{direction::read};
            bool attached{false};          // driver-only: a knote for (fd, dir) currently points at this
            ::FCS::Worker::detail::callback_function<action(registration&, int)> on_event;
            subscription channel_subscription;
        };

        struct command {
            enum class kind : std::uint8_t { arm, cancel };
            command* next{};
            kind what{};
            table::handle h{};
        };

        [[nodiscard]] static constexpr bool same_generation(std::uint32_t a, std::uint32_t b) noexcept {
            return (a & table::generation_mask) == (b & table::generation_mask);
        }

        // --- any thread ---
        [[nodiscard]] static bool cancel_slot(void* owner, std::size_t packed) noexcept;
        [[nodiscard]] subscription launch(registration* registration_ptr) noexcept;
        void post_command(command::kind what, table::handle h) noexcept;
        void wake() noexcept;
        void wake_if_sleeping() noexcept;

        // --- driver only ---
        void run_loop();                                           // execution::dedicated_poller
        [[nodiscard]] static bool poll_once(void* owner) noexcept; // execution::shared_worker
        [[nodiscard]] bool service_commands() noexcept;
        void sweep_cancelled() noexcept;
        void arm(registration& registration_ref) noexcept;
        void fail(registration& registration_ref, int error) noexcept;
        void detach(registration& registration_ref) noexcept;
        void dispatch(const struct kevent& event) noexcept;
        void drain_wakeup() noexcept;
        void finish(registration* registration_ptr, bool deliver_pending = false) noexcept;
        void discard() noexcept;

        int kqueue_fd_{-1};
        int wake_read_{-1};
        int wake_write_{-1};
        std::thread poller_;
        std::atomic_bool polling_{};
        std::atomic_bool sleeping_{};      // dedicated poller only: "I'm about to block in kevent()"
        std::atomic_bool sweep_needed_{};  // a cancel command couldn't be allocated; rescan for cancelled slots
        std::size_t poll_hook_{};

        table slots_state_;
        std::array<std::atomic<registration*>, capacity> slots_{};
        detail::mpsc_stack<command> cmds_;
    };

    using eventlooper = backend<pool_service<>>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;

}

#include "kqueue.inl"
