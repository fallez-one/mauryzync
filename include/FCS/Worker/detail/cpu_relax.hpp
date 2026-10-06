#pragma once

#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
#  include <intrin.h>
#endif

namespace FCS::Worker::detail {

    // Spin-wait hint: tells the core this is a busy-wait loop (saves power, avoids the
    // memory-order mis-speculation penalty on loop exit, and on SMT lets the sibling
    // hyperthread use the slot).
    inline void cpu_relax() noexcept {
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
        _mm_pause();
#elif defined(_MSC_VER) && defined(_M_ARM64)
        __yield();
#elif defined(__x86_64__) || defined(__i386__)
        __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
        __asm__ __volatile__("yield" ::: "memory");
#else
        // no hint available: a plain compiler barrier keeps the loop from being elided
        __asm__ __volatile__("" ::: "memory");
#endif
    }

}
