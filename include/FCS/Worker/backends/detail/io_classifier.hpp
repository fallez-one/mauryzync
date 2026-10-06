#pragma once

#include "../../types.hpp"

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
   // winsock2.h must precede windows.h, or windows.h silently drags in the
   // legacy winsock.h and every Winsock 2 symbol (WSARecv, SOCKET, ...) below
   // collides with it.
#  include <winsock2.h>
#  include <windows.h>
#else
#  include <sys/socket.h>
#  include <sys/stat.h>
#  include <unistd.h>
#endif

namespace FCS::Worker::backend::detail {

#if defined(_WIN32)

    [[nodiscard]] inline source_kind classify_native(HANDLE handle) noexcept {
        const auto type = ::GetFileType(handle);
        if (type == FILE_TYPE_PIPE || type == FILE_TYPE_DISK || type == FILE_TYPE_CHAR) return source_kind::io;
        return source_kind::network;
    }

    // GetFileType() is documented only for file/pipe/char handles; passing it a
    // Winsock SOCKET is unsupported. SOCKET (UINT_PTR) and HANDLE (PVOID) are
    // distinct types on Windows, so this overload is picked unambiguously for
    // sources registered as sockets and never touches GetFileType.
    [[nodiscard]] inline source_kind classify_native(SOCKET) noexcept { return source_kind::network; }

    // IOCP association (CreateIoCompletionPort) and cancellation (CancelIoEx) both
    // take a HANDLE. A SOCKET is a legitimate kernel handle for those two calls,
    // but the two types don't implicitly convert to one another, so callers need
    // an explicit, single place to bridge them.
    [[nodiscard]] inline HANDLE to_iocp_handle(HANDLE handle) noexcept { return handle; }
    [[nodiscard]] inline HANDLE to_iocp_handle(SOCKET socket) noexcept { return reinterpret_cast<HANDLE>(socket); }

#else

    [[nodiscard]] inline source_kind classify_native(int fd) noexcept {
        struct stat info{};
        if (::fstat(fd, &info) != 0) return source_kind::io;
        int socket_type{};
        socklen_t length = sizeof(socket_type);
        if (S_ISSOCK(info.st_mode) || ::getsockopt(fd, SOL_SOCKET, SO_TYPE, &socket_type, &length) == 0) return source_kind::network;
        return source_kind::io;
    }

#endif

    template<typename Native>
    [[nodiscard]] source_kind resolve_kind(const source_ref<Native>& source) noexcept { return source.kind; }

    template<typename Native>
    [[nodiscard]] source_kind resolve_kind(const Native& native) noexcept { return classify_native(native); }

    template<typename Native>
    [[nodiscard]] Native resolve_native(const source_ref<Native>& source) noexcept { return source.native; }

    template<typename Native>
    [[nodiscard]] Native resolve_native(const Native& native) noexcept { return native; }

}
