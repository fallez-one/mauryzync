// io_uring availability probe.
//
// Exit codes (the contract cmake/ProbeIoUring.cmake relies on):
//    0  io_uring_setup() succeeded -- usable
//   72  kernel/headers have no io_uring (ENOSYS, or SYS_io_uring_setup undefined)
//   77  present but denied (EPERM/EACCES: seccomp, sysctl kernel.io_uring_disabled, LSM)
//    1  any other unexpected failure
//  255  not Linux (-1)
//
// Usage: io_uring_probe [human]     -- "human" prints a one-line diagnosis to stderr/clog

#include <cstring>
#include <iostream>

#if defined(__linux__)

#include <cerrno>
#include <sys/syscall.h>
#include <unistd.h>

#if defined(SYS_io_uring_setup)

#include <linux/io_uring.h>

int main(int argc, const char* argv[]) {
    bool human = false;
    if (argc > 1) {
        human = ::strncmp(argv[1], "human", 6) == 0;
    }

    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));

    const int ring_fd = static_cast<int>(::syscall(SYS_io_uring_setup, 2, &params));

    if (ring_fd < 0) {
        const int err = errno;
        if (err == ENOSYS) {
            if (human) std::cerr << "unsupported kernel" << '\n';
            return 72; // critical OS component to support io_uring failed
        }
        if (err == EPERM || err == EACCES) {
            if (human) std::cerr << "io_uring disabled by security" << '\n';
            return 77; // permission denied
        }
        if (human) std::cerr << "unexpected error: " << ::strerror(err) << '\n';
        return 1;
    }

    if (human) std::clog << "ok" << '\n';
    ::close(ring_fd);
    return 0;
}

#else // !SYS_io_uring_setup

int main(int argc, const char* argv[]) {
    bool human = false;
    if (argc > 1) {
        human = ::strncmp(argv[1], "human", 6) == 0;
    }
    if (human) std::cerr << "kernel too old" << '\n';
    return 72;
}

#endif /* SYS_io_uring_setup */

#else // !__linux__

int main(int argc, const char* argv[]) {
    bool human = false;
    if (argc > 1) {
        human = ::strncmp(argv[1], "human", 6) == 0;
    }
    if (human) std::cerr << "not linux" << '\n';
    return -1;
}

#endif /* __linux__ */
