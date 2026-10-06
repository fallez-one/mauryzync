#pragma once

#include <sys/syscall.h>

#if defined(SYS_io_uring_setup)

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <linux/io_uring.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

namespace FCS::Worker::backend::io_uring::detail {

    // Kernel ABI values, spelled out so this still builds against uapi headers
    // that predate them; setup() probes support at runtime regardless.
    namespace uapi {
        inline constexpr unsigned setup_cqsize = 1u << 3;
        inline constexpr unsigned setup_clamp = 1u << 4;
        inline constexpr unsigned setup_r_disabled = 1u << 6;
        inline constexpr unsigned setup_submit_all = 1u << 7;
        inline constexpr unsigned setup_single_issuer = 1u << 12;
        inline constexpr unsigned setup_defer_taskrun = 1u << 13;
        inline constexpr unsigned enter_getevents = 1u << 0;
        inline constexpr unsigned register_enable_rings = 12;
        inline constexpr unsigned sq_cq_overflow = 1u << 1;
        inline constexpr unsigned sq_taskrun = 1u << 2;
    }

    // Thin RAII wrapper around one io_uring instance. Deliberately knows
    // nothing about registrations or dispatch -- see io_uring.hpp/.inl.
    //
    // Threading contract -- the whole point of this rewrite: a ring has exactly
    // ONE owner at a time, the "driver" (the backend's poller thread, or
    // whichever pool worker currently holds its poll hook; poll_registry's
    // `busy` flag serializes those with acquire/release). Every method below is
    // driver-only. Nothing here takes a lock; user threads never touch the
    // ring -- they hand work to the driver through backend::cmds_ instead.
    //
    // Why that matters beyond removing the old submit mutex:
    //  * io_uring attributes every in-flight request to the task that submitted
    //    it and cancels them when that task exits. Submitting from arbitrary
    //    user threads meant a registration made on a short-lived thread was
    //    silently cancelled when the thread went away.
    //  * It lets a dedicated poller use IORING_SETUP_SINGLE_ISSUER +
    //    DEFER_TASKRUN (no cross-CPU task_work interruptions, completions are
    //    only processed when the driver asks for them).
    //  * Submission batches naturally: SQEs queued while draining a batch of
    //    completions go to the kernel in one io_uring_enter() that also waits.
    class ring {
    public:
        ring() = default;
        ~ring() { cleanup(); }

        ring(const ring&) = delete;
        ring& operator=(const ring&) = delete;

        // Returns 0, or -errno. `single_issuer` asks for the dedicated-poller
        // configuration (created R_DISABLED; the poller thread calls enable()
        // and thereby becomes the one submitter). Degrades gracefully: if the
        // kernel rejects the flags (EINVAL) it retries with fewer of them.
        [[nodiscard]] int setup(unsigned entries, unsigned cq_entries, bool single_issuer) noexcept {
            cleanup();

            const unsigned base = uapi::setup_clamp | uapi::setup_submit_all | uapi::setup_cqsize;
            std::array<unsigned, 3> attempts{};
            std::size_t attempt_count = 0;
            if (single_issuer) {
                attempts[attempt_count++] = base | uapi::setup_r_disabled | uapi::setup_single_issuer | uapi::setup_defer_taskrun;
                attempts[attempt_count++] = base | uapi::setup_r_disabled | uapi::setup_single_issuer;
            }
            attempts[attempt_count++] = base;

            io_uring_params params{};
            long fd = -1;
            unsigned used_flags = 0;
            for (std::size_t i = 0; i < attempt_count; ++i) {
                std::memset(&params, 0, sizeof(params));
                params.flags = attempts[i];
                params.cq_entries = cq_entries;
                fd = ::syscall(SYS_io_uring_setup, entries, &params);
                if (fd >= 0) { used_flags = attempts[i]; break; }
                if (errno != EINVAL) return -errno; // EPERM/ENOSYS/ENOMEM...: retrying with fewer flags won't help
            }
            if (fd < 0) return -EINVAL;

            ring_fd_ = static_cast<int>(fd);
            disabled_ = (used_flags & uapi::setup_r_disabled) != 0;
            single_issuer_ = (used_flags & uapi::setup_single_issuer) != 0;

            sq_size_ = params.sq_off.array + params.sq_entries * sizeof(__u32);
            cq_size_ = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
            shared_mapping_ = (params.features & IORING_FEAT_SINGLE_MMAP) != 0;
            if (shared_mapping_) {
                sq_size_ = sq_size_ < cq_size_ ? cq_size_ : sq_size_;
                cq_size_ = sq_size_;
            }

            sq_map_ = ::mmap(nullptr, sq_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_SQ_RING);
            if (sq_map_ == MAP_FAILED) { const int err = errno; cleanup(); return -err; }
            cq_map_ = shared_mapping_ ? sq_map_
                : ::mmap(nullptr, cq_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_CQ_RING);
            if (cq_map_ == MAP_FAILED) { const int err = errno; cleanup(); return -err; }
            sqe_size_ = params.sq_entries * sizeof(io_uring_sqe);
            sqes_map_ = ::mmap(nullptr, sqe_size_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_SQES);
            if (sqes_map_ == MAP_FAILED) { const int err = errno; cleanup(); return -err; }

            auto* sq = static_cast<std::byte*>(sq_map_);
            sq_head_ = reinterpret_cast<unsigned*>(sq + params.sq_off.head);
            sq_tail_ = reinterpret_cast<unsigned*>(sq + params.sq_off.tail);
            sq_flags_ = reinterpret_cast<unsigned*>(sq + params.sq_off.flags);
            sq_mask_ = *reinterpret_cast<unsigned*>(sq + params.sq_off.ring_mask);
            sq_entries_ = *reinterpret_cast<unsigned*>(sq + params.sq_off.ring_entries);
            auto* sq_array = reinterpret_cast<unsigned*>(sq + params.sq_off.array);
            for (unsigned i = 0; i < sq_entries_; ++i) sq_array[i] = i; // identity mapping, never changes
            sqes_ = static_cast<io_uring_sqe*>(sqes_map_);

            auto* cq = static_cast<std::byte*>(cq_map_);
            cq_head_ = reinterpret_cast<unsigned*>(cq + params.cq_off.head);
            cq_tail_ = reinterpret_cast<unsigned*>(cq + params.cq_off.tail);
            cq_mask_ = *reinterpret_cast<unsigned*>(cq + params.cq_off.ring_mask);
            cq_entries_ = *reinterpret_cast<unsigned*>(cq + params.cq_off.ring_entries);
            cqes_ = reinterpret_cast<io_uring_cqe*>(cq + params.cq_off.cqes);

            sqe_tail_ = load(sq_tail_);
            return 0;
        }

        [[nodiscard]] bool valid() const noexcept { return ring_fd_ >= 0; }
        [[nodiscard]] bool single_issuer() const noexcept { return single_issuer_; }
        [[nodiscard]] unsigned cq_capacity() const noexcept { return cq_entries_; }
        void close() noexcept { cleanup(); }

        // For a ring created R_DISABLED: the *calling thread* becomes its
        // submitter task. No-op (returns 0) for any other ring.
        [[nodiscard]] int enable() noexcept {
            if (!disabled_) return 0;
            if (::syscall(SYS_io_uring_register, ring_fd_, uapi::register_enable_rings, nullptr, 0) < 0) return -errno;
            disabled_ = false;
            return 0;
        }

        // A zeroed SQE slot, or nullptr if the submission ring is full (the
        // caller flushes with submit_and_wait(0) and retries).
        [[nodiscard]] io_uring_sqe* get_sqe() noexcept {
            const unsigned head = load(sq_head_);
            if (sqe_tail_ - head >= sq_entries_) return nullptr;
            auto* sqe = &sqes_[sqe_tail_ & sq_mask_];
            ++sqe_tail_;
            std::memset(sqe, 0, sizeof(*sqe));
            return sqe;
        }

        [[nodiscard]] unsigned pending() const noexcept { return sqe_tail_ - load(sq_head_); }

        // Publishes everything queued and hands it to the kernel in ONE
        // io_uring_enter(), optionally also waiting for `wait_nr` completions
        // (submit + wait in a single syscall is what halves the syscall count
        // against an epoll_wait-plus-read design). Makes no syscall at all if
        // there is nothing to submit, nothing to wait for and the kernel isn't
        // asking for a flush. Returns entries consumed, or -errno.
        [[nodiscard]] int submit_and_wait(unsigned wait_nr) noexcept {
            store(sq_tail_, sqe_tail_);
            const unsigned to_submit = sqe_tail_ - load(sq_head_);
            unsigned flags = 0;
            if (wait_nr != 0 || (load(sq_flags_) & (uapi::sq_cq_overflow | uapi::sq_taskrun)) != 0) flags |= uapi::enter_getevents;
            if (to_submit == 0 && flags == 0) return 0;
            for (;;) {
                const long rc = ::syscall(SYS_io_uring_enter, ring_fd_, to_submit, wait_nr, flags, nullptr, 0);
                if (rc >= 0) return static_cast<int>(rc);
                if (errno == EINTR) { if (wait_nr != 0) return 0; continue; } // a signal just means "re-evaluate"
                return -errno;
            }
        }

        [[nodiscard]] bool completions_ready() const noexcept { return load(cq_head_) != load(cq_tail_); }

        // Invokes f(user_data, res) for up to `max` completions, then returns
        // how many it handled. `f` may queue new SQEs (rearms) but must not
        // call drain() recursively.
        template<typename F>
        unsigned drain(unsigned max, F&& f) {
            unsigned head = load(cq_head_);
            const unsigned tail = load(cq_tail_);
            unsigned handled = 0;
            while (head != tail && handled < max) {
                const io_uring_cqe cqe = cqes_[head & cq_mask_];
                ++head;
                ++handled;
                f(static_cast<std::uint64_t>(cqe.user_data), static_cast<std::int32_t>(cqe.res));
            }
            if (handled != 0) store(cq_head_, head);
            return handled;
        }

    private:
        [[nodiscard]] static unsigned load(const unsigned* p) noexcept {
            return std::atomic_ref<const unsigned>(*p).load(std::memory_order_acquire);
        }
        static void store(unsigned* p, unsigned v) noexcept {
            std::atomic_ref<unsigned>(*p).store(v, std::memory_order_release);
        }

        void cleanup() noexcept {
            if (sqes_map_ != MAP_FAILED) ::munmap(sqes_map_, sqe_size_);
            if (!shared_mapping_ && cq_map_ != MAP_FAILED) ::munmap(cq_map_, cq_size_);
            if (sq_map_ != MAP_FAILED) ::munmap(sq_map_, sq_size_);
            if (ring_fd_ >= 0) ::close(ring_fd_);
            ring_fd_ = -1;
            sq_map_ = cq_map_ = sqes_map_ = MAP_FAILED;
            sq_size_ = cq_size_ = sqe_size_ = 0;
            shared_mapping_ = disabled_ = single_issuer_ = false;
            sq_head_ = sq_tail_ = sq_flags_ = cq_head_ = cq_tail_ = nullptr;
            sqes_ = nullptr;
            cqes_ = nullptr;
            sq_mask_ = sq_entries_ = cq_mask_ = cq_entries_ = sqe_tail_ = 0;
        }

        int ring_fd_{-1};
        void* sq_map_{MAP_FAILED};
        void* cq_map_{MAP_FAILED};
        void* sqes_map_{MAP_FAILED};
        std::size_t sq_size_{};
        std::size_t cq_size_{};
        std::size_t sqe_size_{};
        bool shared_mapping_{};
        bool disabled_{};
        bool single_issuer_{};

        unsigned* sq_head_{};
        unsigned* sq_tail_{};
        unsigned* sq_flags_{};
        unsigned* cq_head_{};
        unsigned* cq_tail_{};
        io_uring_sqe* sqes_{};
        io_uring_cqe* cqes_{};
        unsigned sq_mask_{};
        unsigned sq_entries_{};
        unsigned cq_mask_{};
        unsigned cq_entries_{};
        unsigned sqe_tail_{}; // driver-local; published to the kernel by submit_and_wait()
    };

    // SQE preparation helpers -- pure field setters, no ring access.
    inline void prep_read(io_uring_sqe* sqe, int fd, void* buffer, unsigned length, std::uint64_t user_data) noexcept {
        sqe->opcode = IORING_OP_READ;
        sqe->fd = fd;
        sqe->addr = reinterpret_cast<std::uintptr_t>(buffer);
        sqe->len = length;
        sqe->off = ~std::uint64_t{0}; // "current file position": ignored by sockets/pipes, correct for seekable files
        sqe->user_data = user_data;
    }
    inline void prep_recv(io_uring_sqe* sqe, int fd, void* buffer, unsigned length, int msg_flags, std::uint64_t user_data) noexcept {
        sqe->opcode = IORING_OP_RECV;
        sqe->fd = fd;
        sqe->addr = reinterpret_cast<std::uintptr_t>(buffer);
        sqe->len = length;
        sqe->msg_flags = static_cast<__u32>(msg_flags);
        sqe->user_data = user_data;
    }
    inline void prep_recvmsg(io_uring_sqe* sqe, int fd, msghdr* msg, int msg_flags, std::uint64_t user_data) noexcept {
        sqe->opcode = IORING_OP_RECVMSG;
        sqe->fd = fd;
        sqe->addr = reinterpret_cast<std::uintptr_t>(msg);
        sqe->len = 1;
        sqe->msg_flags = static_cast<__u32>(msg_flags);
        sqe->user_data = user_data;
    }
    inline void prep_poll_add(io_uring_sqe* sqe, int fd, short mask, std::uint64_t user_data) noexcept {
        sqe->opcode = IORING_OP_POLL_ADD;
        sqe->fd = fd;
        sqe->poll_events = static_cast<decltype(sqe->poll_events)>(mask);
        sqe->user_data = user_data;
    }
    // Cancels the one in-flight request whose user_data equals `target`. Its own
    // completion (user_data = `ack`) just acknowledges the request; the target
    // separately completes with -ECANCELED (or, if it raced, normally).
    inline void prep_cancel(io_uring_sqe* sqe, std::uint64_t target, std::uint64_t ack) noexcept {
        sqe->opcode = IORING_OP_ASYNC_CANCEL;
        sqe->fd = -1;
        sqe->addr = target;
        sqe->user_data = ack;
    }

}

#else

// This Linux's uapi headers predate io_uring entirely (no SYS_io_uring_setup
// at all) -- nothing above this line is even declarable, let alone usable.
// io_uring.hpp checks for this and falls back to the epoll backend wholesale
// rather than failing the build.
#define FCS_UNSUPPORTED_FALLBACK_EPOLL 1

#endif
