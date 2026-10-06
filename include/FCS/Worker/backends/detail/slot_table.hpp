#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace FCS::Worker::backend::detail {

    // Lock-free lifetime bookkeeping for a fixed table of registration slots,
    // shared by the completion-based backends (io_uring, IOCP).
    //
    // The problem it solves: a `subscription` carries only a slot number, can
    // be cancelled from *any* thread at any time, and must never touch a
    // registration's memory -- the driver (poller thread / whichever worker
    // holds the poll hook) is the sole owner of that memory and frees it the
    // moment the kernel delivers its terminal completion. So user threads
    // never dereference a registration at all: they only flip this small
    // per-slot state word, and the driver does everything else.
    //
    // Each slot is one 64-bit word: [ generation:32 | unused:30 | phase:2 ].
    //
    //   free --claim()-->  live  --request_cancel() (any thread)-->  dead
    //                        \---retire()  (driver: EOF / error) -->  dead
    //   dead --release()  (driver, only after the kernel is done with it)--> free
    //
    // `generation` is bumped on every claim(), so a stale subscription (its
    // registration already retired and the slot since reused) can never cancel
    // the wrong registration: its handle no longer matches.
    template<std::size_t Capacity>
    class slot_table {
        static_assert(Capacity > 0 && Capacity <= 0xFFFFu, "slot index must fit the packed subscription handle");

    public:
        struct handle {
            std::size_t index{};
            std::uint32_t generation{};
        };

        // A `subscription` stores a single std::size_t. On 64-bit targets that
        // is [generation:32 | index:32]; on 32-bit it degrades to 16/16, which
        // only shortens the stale-handle detection window, never breaks it.
        static constexpr unsigned index_bits = sizeof(std::size_t) >= 8 ? 32u : 16u;
        static constexpr std::uint64_t index_mask = (std::uint64_t{1} << index_bits) - 1;
        static constexpr std::uint32_t generation_mask = sizeof(std::size_t) >= 8 ? 0xFFFFFFFFu : 0xFFFFu;

        [[nodiscard]] static std::size_t pack(handle h) noexcept {
            return static_cast<std::size_t>(((std::uint64_t{h.generation & generation_mask}) << index_bits) | (h.index & index_mask));
        }
        [[nodiscard]] static handle unpack(std::size_t packed) noexcept {
            return {static_cast<std::size_t>(packed & index_mask), static_cast<std::uint32_t>((std::uint64_t{packed} >> index_bits) & generation_mask)};
        }

        // Any thread. Claims a free slot (live) or returns nullopt if the
        // table is full. Scans from a rotating start so concurrent claimers
        // don't all hammer slot 0.
        [[nodiscard]] std::optional<handle> claim() noexcept {
            const auto start = cursor_.fetch_add(1, std::memory_order_relaxed);
            for (std::size_t n = 0; n < Capacity; ++n) {
                const auto i = (start + n) % Capacity;
                auto word = state_[i].load(std::memory_order_relaxed);
                if (phase_of(word) != phase_free) continue;
                const auto gen = static_cast<std::uint32_t>(generation_of(word) + 1u);
                if (state_[i].compare_exchange_strong(word, make(gen, phase_live), std::memory_order_acq_rel, std::memory_order_relaxed))
                    return handle{i, gen};
            }
            return std::nullopt;
        }

        // Any thread (this is what subscription::cancel() lands on). True iff
        // *this call* is the one that moved the slot live -> dead; false for a
        // stale handle, an already-cancelled one, or one the driver retired first.
        [[nodiscard]] bool request_cancel(handle h) noexcept {
            if (h.index >= Capacity) return false;
            auto word = state_[h.index].load(std::memory_order_acquire);
            if (phase_of(word) != phase_live || !matches(word, h)) return false;
            return state_[h.index].compare_exchange_strong(word, make(generation_of(word), phase_dead), std::memory_order_acq_rel, std::memory_order_acquire);
        }

        // Driver only. The registration ended on its own (EOF/error) or was
        // found cancelled; make sure no later request_cancel() can succeed.
        // Returns whether it was still live.
        bool retire(handle h) noexcept {
            if (h.index >= Capacity) return false;
            auto word = state_[h.index].load(std::memory_order_acquire);
            if (phase_of(word) != phase_live || !matches(word, h)) return false;
            return state_[h.index].compare_exchange_strong(word, make(generation_of(word), phase_dead), std::memory_order_acq_rel, std::memory_order_acquire);
        }

        // Driver only, and only once the kernel can no longer touch this
        // registration's buffers (terminal completion seen). Makes the slot
        // claimable again; the generation is preserved so the next claim()
        // bumps it.
        void release(std::size_t index) noexcept {
            if (index >= Capacity) return;
            const auto word = state_[index].load(std::memory_order_relaxed);
            state_[index].store(make(generation_of(word), phase_free), std::memory_order_release);
        }

        [[nodiscard]] bool live(handle h) const noexcept {
            if (h.index >= Capacity) return false;
            const auto word = state_[h.index].load(std::memory_order_acquire);
            return phase_of(word) == phase_live && matches(word, h);
        }

        [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity; }

    private:
        static constexpr std::uint64_t phase_free = 0;
        static constexpr std::uint64_t phase_live = 1;
        static constexpr std::uint64_t phase_dead = 2;

        [[nodiscard]] static constexpr std::uint64_t make(std::uint32_t generation, std::uint64_t phase) noexcept { return (std::uint64_t{generation} << 32) | phase; }
        [[nodiscard]] static constexpr std::uint64_t phase_of(std::uint64_t word) noexcept { return word & 3u; }
        [[nodiscard]] static constexpr std::uint32_t generation_of(std::uint64_t word) noexcept { return static_cast<std::uint32_t>(word >> 32); }
        [[nodiscard]] static constexpr bool matches(std::uint64_t word, handle h) noexcept { return (generation_of(word) & generation_mask) == (h.generation & generation_mask); }

        std::array<std::atomic<std::uint64_t>, Capacity> state_{};
        std::atomic_size_t cursor_{};
    };

}
