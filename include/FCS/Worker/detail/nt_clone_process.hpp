#pragma once

// Windows-only: RtlCloneUserProcess, the closest NT has to fork(). Not exported by any import
// library, so it is looked up in ntdll at first use (a function-local static -- no dynamic
// initializer at namespace scope, no per-TU copy of the pointer).
#ifdef _WIN32

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

#include <cstdint>

namespace FCS::Worker::detail::nt {

    // <ntstatus.h> cannot be included after <windows.h> without macro redefinitions, so the two
    // values used here are spelled out.
    inline constexpr LONG status_process_cloned = 0x00000129L;      // STATUS_PROCESS_CLONED: "you are the child"
    inline constexpr LONG status_not_supported = static_cast<LONG>(0xC00000BBL);
    inline constexpr ULONG clone_inherit_handles = 0x00000002UL;    // RTL_CLONE_PROCESS_FLAGS_INHERIT_HANDLES

    struct client_id {
        HANDLE unique_process;
        HANDLE unique_thread;
    };

    // Layout of SECTION_IMAGE_INFORMATION / RTL_USER_PROCESS_INFORMATION (winternl-style). Only
    // the size matters to this library; the process and thread handles are what it reads.
    struct section_image_information {
        PVOID transfer_address;
        ULONG zero_bits;
        SIZE_T maximum_stack_size;
        SIZE_T committed_stack_size;
        ULONG sub_system_type;
        ULONG sub_system_version;
        ULONG operating_system_version;
        USHORT image_characteristics;
        USHORT dll_characteristics;
        USHORT machine;
        BOOLEAN image_contains_code;
        UCHAR image_flags;
        ULONG loader_flags;
        ULONG image_file_size;
        ULONG check_sum;
    };

    struct user_process_information {
        ULONG length;
        HANDLE process_handle;
        HANDLE thread_handle;
        client_id client;
        section_image_information image;
    };

    using rtl_clone_user_process_fn = LONG(NTAPI*)(ULONG process_flags,
                                                   PSECURITY_DESCRIPTOR process_security_descriptor,
                                                   PSECURITY_DESCRIPTOR thread_security_descriptor,
                                                   HANDLE debug_port,
                                                   user_process_information* process_information);

    // nullptr when this Windows has no RtlCloneUserProcess (too old, or stripped).
    [[nodiscard]] inline rtl_clone_user_process_fn rtl_clone_user_process() noexcept {
        static const rtl_clone_user_process_fn fn = [] () noexcept -> rtl_clone_user_process_fn {
            const HMODULE ntdll = ::GetModuleHandleA("ntdll.dll");
            if (!ntdll) return nullptr;
            // FARPROC -> specific function pointer: the cast the Win32 API requires.
            return reinterpret_cast<rtl_clone_user_process_fn>(reinterpret_cast<void*>(::GetProcAddress(ntdll, "RtlCloneUserProcess")));
        }();
        return fn;
    }

}

#endif
