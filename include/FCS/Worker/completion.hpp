#pragma once

#include "types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace FCS::Worker {

    // How the operation this completion represents actually ended. Distinct
    // from read_result<Size>::size == 0 being ambiguous on its own (nothing
    // yet? EOF? an error?) -- status disambiguates it explicitly.
    enum class completion_status : std::uint8_t {
        ok,         // data (possibly zero bytes for a zero-length read) was produced normally
        closed,     // peer/EOF -- zero bytes is expected and final, not an error
        error,      // something went wrong; see error/raw_status
        cancelled,  // the operation was cancelled out from under it (POLL_REMOVE / CancelIoEx)
    };

    // What kind of operation completed. Only `read` is produced by anything
    // today -- reserved so a write/accept/connect/timeout completion doesn't
    // have to be retrofitted into every existing consumer's assumptions later.
    enum class completion_op : std::uint8_t { read, write, accept, connect, timeout };

    enum completion_flag : std::uint32_t {
        completion_flag_none = 0,
        completion_flag_more_pending = 1u << 0, // the source likely has more queued right now
        completion_flag_partial = 1u << 1,      // result is a partial read, not a full message/frame
    };

    // Delivered to whatever subscribed via subscribe<Tag, Size>(source, reader,
    // callback) — uniformly, regardless of whether the backend underneath is
    // readiness-based (epoll/kqueue) or genuinely completion-based
    // (io_uring/IOCP). `result` is always what Reader produced; everything
    // else here is metadata Reader itself doesn't (and shouldn't need to)
    // know about.
    template<std::size_t Size>
    struct completion {
        std::uint64_t user_data{};   // correlation token -- the registration's slot index
        std::uint32_t generation{};  // bumped each time that slot is reused; lets a caller
                                      // detect a completion arriving for a registration that
                                      // isn't the one they think occupies this user_data anymore
        completion_status status{completion_status::ok};
        completion_op op{completion_op::read};
        source_kind kind{source_kind::io};
        std::error_code error{};     // portable, backend-normalized (set only when status == error)
        std::int64_t raw_status{};   // the untranslated platform value: cqe->res, revents, GetLastError()
        std::uint64_t offset{};      // meaningful for file-like sources; 0 for sockets
        std::uint32_t flags{completion_flag_none};
        std::chrono::steady_clock::time_point submitted_at{};
        std::chrono::steady_clock::time_point completed_at{};
        read_result<Size> result{};
    };

}
