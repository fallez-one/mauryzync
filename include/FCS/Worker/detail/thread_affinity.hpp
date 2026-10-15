#pragma once

// Pin the CALLING thread to one logical CPU. Best effort: false when the platform has no hard
// affinity (macOS), the CPU index is out of range, or the OS refused (a cgroup/cpuset that does
// not allow that CPU, which on Android is common).
#if defined(__linux__) && !defined(_GNU_SOURCE)
#  define _GNU_SOURCE 1            // CPU_SET / sched_setaffinity; a no-op if a system header already ran
#endif

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <windows.h>
#elif defined(__linux__)           // glibc, musl and bionic (Android)
#  include <pthread.h>
#  include <sched.h>
#endif

namespace FCS::Worker::detail {

    [[nodiscard]] inline bool pin_current_thread(unsigned cpu) noexcept {
#if defined(_WIN32)
        // One processor group (64 CPUs): enough for a worker pool, and the mask API's own limit.
        if (cpu >= 8 * sizeof(DWORD_PTR)) return false;
        return ::SetThreadAffinityMask(::GetCurrentThread(), static_cast<DWORD_PTR>(1) << cpu) != 0;
#elif defined(__linux__)
        if (cpu >= CPU_SETSIZE) return false;
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
#  if defined(__ANDROID__)
        // bionic has no pthread_setaffinity_np. sched_setaffinity takes a thread id, and 0 means the caller.
        return ::sched_setaffinity(0, sizeof set, &set) == 0;
#  else
        return ::pthread_setaffinity_np(::pthread_self(), sizeof set, &set) == 0;
#  endif
#else
        (void)cpu;
        return false;
#endif
    }

}
