#pragma once

#include "../completion.hpp"
#include "../eventloop.hpp"
#include "../execution.hpp"
#include "detail/io_classifier.hpp"
#include "detail/mpsc_stack.hpp"
#include "detail/slot_table.hpp"
#include "detail/source_types.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>
#include <type_traits>
#include <utility>

namespace FCS::Worker::backend::epoll {

    // epoll only reports readiness; the driver does the real read/write itself
    // the instant it fires, wrapping the result into a uniform completion<Size>.
    //
    // Full duplex: register_source() and register_sink() on the same fd are two
    // independent registrations (own handle, callback, subscription, torn down
    // independently) that share one epoll entry (EPOLLIN | EPOLLOUT) instead of
    // colliding on EPOLL_CTL_ADD; the entry is removed once neither is attached.
    // Registering a direction that is already registered on the fd replaces it:
    // the newest registration wins and the older one is cancelled.
    //
    // Threading model -- the same one io_uring::backend uses, and no mutex:
    //  * One *driver* at a time -- the dedicated poller thread, or whichever pool
    //    worker holds this backend's poll_registry `busy` flag -- owns every
    //    registration's memory, the fd->epoll-entry table and every epoll_ctl().
    //    Only the driver dispatches, attaches, detaches and frees.
    //  * Any thread may register/cancel, but never touches a registration that is
    //    already published (it may be freed at any moment): registering claims a
    //    slot in `slots_state_`, publishes the registration and pushes an `arm`
    //    command onto `cmds_` (lock-free MPSC); cancelling flips the slot's state
    //    word and pushes a `cancel` command. cancel() takes effect immediately for
    //    dispatch (it checks the state word); the driver does the rest.
    //  * Because every epoll_ctl() is issued by the driver in command order, a
    //    cancel() followed by close() and a new registration of the recycled fd
    //    number can't have the old entry's DEL land on the new one.
    //  * Registrations are freed by the driver with no lock held, so destructors
    //    of user code (callback / Reader / Writer captures) may freely call
    //    cancel() or register on this backend. That re-entrancy is what used to
    //    deadlock when registrations were freed under a mutex.
    //  * epoll_ctl() runs on the driver, so a refused fd (EBADF, EPERM for a
    //    regular file, ...) is reported through the registration's callback as a
    //    completion_status::error rather than as an empty subscription.
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
        static constexpr std::size_t no_entry = ~std::size_t{0};
        using table = detail::slot_table<capacity>;

        enum class direction : std::uint8_t { read, write };
        enum class action : std::uint8_t { keep, retire };

        // One per register_source()/register_sink(). Everything but `h` is
        // driver-owned once published.
        struct registration {
            table::handle h{};
            int fd{-1};
            source_kind kind{source_kind::io};
            direction dir{direction::read};
            std::size_t entry{no_entry};   // driver-only: the epoll entry this is attached to
            // Called by the driver with 0 when the fd is ready, or with the errno
            // epoll_ctl() refused it with. Posts the completion; says whether to keep going.
            ::FCS::Worker::detail::callback_function<action(registration&, int)> on_event;
            subscription channel_subscription;
        };

        // Driver-only. data.u32 of the epoll event is the index into entries_.
        struct entry {
            int fd{-1};
            registration* dir[2]{};
        };

        struct command {
            enum class kind : std::uint8_t { arm, cancel };
            command* next{};
            kind what{};
            table::handle h{};
        };

        // A handle unpacked from a subscription only carries as many
        // generation bits as fit in a std::size_t next to the index.
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
        [[nodiscard]] bool sync_interest(std::size_t index, bool is_new) noexcept;
        void dispatch(std::size_t index, std::uint32_t flags) noexcept;
        void finish(registration* registration_ptr, bool deliver_pending = false) noexcept;
        void discard() noexcept;

        int epoll_fd_{-1};
        int wakeup_fd_{-1};
        std::thread poller_;
        std::atomic_bool polling_{};
        std::atomic_bool sleeping_{};      // dedicated poller only: "I'm about to block in epoll_wait"
        std::atomic_bool sweep_needed_{};  // a cancel command couldn't be allocated; rescan for cancelled slots
        std::size_t poll_hook_{};

        // Driver-only (no synchronization needed beyond the hand-off between
        // successive drivers, which poll_registry's busy flag already provides).
        std::array<entry, capacity> entries_{};

        table slots_state_;
        std::array<std::atomic<registration*>, capacity> slots_{};
        detail::mpsc_stack<command> cmds_;
    };

    using eventlooper = backend<pool_service<>>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;

}

#include "epoll.inl"