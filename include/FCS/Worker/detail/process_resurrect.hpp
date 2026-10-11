#pragma once

#include "../experimental.hpp"
#include "../../expected.hpp"

#include <cstdint>
#include <cstdlib>
#include <type_traits>

#if defined(_WIN32)
#  include "nt_clone_process.hpp"
#  include <iostream>
#else
#  include <cerrno>
#  include <sys/types.h>
#  include <unistd.h>
#endif

namespace FCS::Worker::detail {

    // Which side of a successful clone this thread woke up on.
    enum class fork_role : unsigned char { parent, child };

    // errno on POSIX, the raw NTSTATUS bits on NT. Opaque: for logging / reporting.
    using resurrect_error = std::uint32_t;

    // "Resurrection": clone the running process (copy-on-write), carry on in the copy, let the
    // original exit. Platform-agnostic front; the platform lives in the Derived class.
    //
    // A Derived implements
    //     Expected<fork_role, resurrect_error> try_fork_impl() const noexcept;
    // and may implement
    //     [[noreturn]] void exit_impl(int code) const noexcept;      // default: std::exit
    //
    // What a clone is (both platforms): ONLY the calling thread exists in the child. Every other
    // thread -- including one wedged forever inside a task -- is simply absent there, with
    // whatever it held (mutexes, half-finished queue operations) frozen in the copied memory.
    // That is the point, and also the hazard: the child must not wait on anything another
    // thread of the parent could have been holding (see pool_service::reset_after_clone()).
    template<typename Derived>
    class process_resurrect {
    public:
        [[nodiscard]] Expected<fork_role, resurrect_error> try_fork() const noexcept {
            return static_cast<const Derived&>(*this).try_fork_impl();
        }

        // Ends THIS process. For the parent of a clone, after it has handed over.
        [[noreturn]] void exit(int code = 0) const noexcept {
            const auto& self = static_cast<const Derived&>(*this);
            if constexpr (requires { self.exit_impl(code); }) self.exit_impl(code);
            else std::exit(code);
            std::abort(); // unreachable: keeps [[noreturn]] honest if an exit_impl ever returned
        }

    protected:
        // Public default construction (a Derived that is a plain `T x{}` must stay constructible,
        // and with a protected constructor C++20 aggregate-init would try the base's); protected
        // destruction is the CRTP guard against deleting through the base.
        ~process_resurrect() = default;
    };

#if defined(_WIN32)

    // NT: RtlCloneUserProcess. The clone has no connection to the console subsystem (csrss), so
    // the child re-attaches to its parent's console or its standard streams stay dead.
    class winnt_resurrect final : public process_resurrect<winnt_resurrect> {
        friend class process_resurrect<winnt_resurrect>;

        [[nodiscard]] Expected<fork_role, resurrect_error> try_fork_impl() const noexcept {
            const auto clone = nt::rtl_clone_user_process();
            if (!clone) return Unexpected<resurrect_error>{static_cast<resurrect_error>(nt::status_not_supported)};

            nt::user_process_information info{};
            const LONG status = clone(nt::clone_inherit_handles, nullptr, nullptr, nullptr, &info);
            if (status == nt::status_process_cloned) {
                // Child. Checked first: STATUS_PROCESS_CLONED is a positive "success" code.
                ::FreeConsole();
                if (::AttachConsole(ATTACH_PARENT_PROCESS)) {
                    std::cout.clear();
                    std::cerr.clear();
                }
                return fork_role::child;
            }
            if (status < 0) return Unexpected<resurrect_error>{static_cast<resurrect_error>(status)};
            // Parent. The clone is already running (no CREATE_SUSPENDED); we only hold two
            // handles to it that nobody needs.
            if (info.thread_handle) ::CloseHandle(info.thread_handle);
            if (info.process_handle) ::CloseHandle(info.process_handle);
            return fork_role::parent;
        }
    };

    using native_process_resurrect = winnt_resurrect;

#else

    // POSIX: fork(). (Also what a kqueue/BSD build gets.)
    class posix_resurrect final : public process_resurrect<posix_resurrect> {
        friend class process_resurrect<posix_resurrect>;

        [[nodiscard]] Expected<fork_role, resurrect_error> try_fork_impl() const noexcept {
            const pid_t pid = ::fork();
            if (pid < 0) return Unexpected<resurrect_error>{static_cast<resurrect_error>(errno)};
            return pid == 0 ? fork_role::child : fork_role::parent;
        }
    };

    using native_process_resurrect = posix_resurrect;

#endif

    // A pool picks its resurrector from Traits::resurrector when the traits name one (tests use
    // this to inject a double), the native one otherwise -- so custom traits written before this
    // existed keep compiling.
    template<typename Traits, typename = void>
    struct resurrector_of { using type = native_process_resurrect; };
    template<typename Traits>
    struct resurrector_of<Traits, std::void_t<typename Traits::resurrector>> { using type = typename Traits::resurrector; };
    template<typename Traits> using resurrector_of_t = typename resurrector_of<Traits>::type;

}
