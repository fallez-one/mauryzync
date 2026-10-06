#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace FCS::Worker::detail {

    // Shared, lock-free, counter-TTL cache of "how many workers look idle right now".
    //
    // Why it exists: every thief used to answer that question for itself by
    // scanning all N local depths on every steal() call -- O(N) relaxed loads
    // across N cache lines, repeated by *every* idle worker, each throwing its
    // answer away. When many workers go idle together (exactly when
    // redistribution matters most) they all pay the same scan at the same
    // moment, against the same victims' cache lines, and their reads of those
    // lines compete with the owners' own pushes and pops.
    //
    // The estimate is only ever used to size a steal ("reserve roughly a fair
    // share"), so it is already best-effort; a slightly stale value changes how
    // big a batch is, never correctness. That makes it a good cache candidate.
    //
    //  * TTL is a *use counter*, not a clock: a published value may serve at
    //    most `ttl` calls to get(), then it is stale. Cache lookups are driven by
    //    steal attempts, so staleness scales with how much stealing is actually
    //    happening, and a clock read (which costs about as much as the scan on
    //    small pools) is avoided.
    //  * One refresher per window. When the budget hits zero exactly one thief
    //    wins a CAS to rescan; everyone else arriving meanwhile reuses the
    //    previous value instead of scanning too -- no duplicated recomputation,
    //    no waiting. ("Unilateral" no longer: the answer is shared.)
    //  * invalidate() zeroes the budget so the next get() rescans. The pool
    //    calls it when a worker parks or wakes, i.e. when the idle population
    //    changed in a way a use counter alone wouldn't notice after a quiet spell.
    //
    // One 64-bit word, so a lookup is one load and (on a hit) one CAS:
    //   [63] refreshing | [62:32] version | [31:16] budget | [15:0] idle count
    // `version` changes on every publish, so a CAS prepared against an old
    // window can never succeed against a new one (ABA).
    class idle_estimate {
    public:
        enum class source : std::uint8_t { cached, scanned, stale_while_refreshing };

        struct reading {
            std::size_t idle{};
            source from{source::cached};
        };

        // ttl == 0 disables caching: every get() scans (the old behaviour).
        void configure(std::size_t ttl) noexcept {
            ttl_ = ttl > 0xFFFFu ? 0xFFFFu : static_cast<std::uint32_t>(ttl);
            invalidate();
        }
        [[nodiscard]] std::size_t ttl() const noexcept { return ttl_; }

        // Marks the cached value stale; the next get() rescans. Cheap and
        // contention-free to call often (it only ever stores a zero-budget word).
        void invalidate() noexcept {
            auto w = word_.load(std::memory_order_relaxed);
            for (;;) {
                if (budget_of(w) == 0 && !refreshing(w)) return; // already stale
                if (refreshing(w)) return;                       // a refresh is already underway; it publishes fresh data
                if (word_.compare_exchange_weak(w, make(idle_of(w), 0, version_of(w), false), std::memory_order_relaxed)) return;
            }
        }

        // `scan` returns the current idle count (called by at most one thread
        // per refresh window).
        template<typename Scan>
        [[nodiscard]] reading get(Scan&& scan) noexcept {
            if (ttl_ == 0) return {clamp(scan()), source::scanned};

            auto w = word_.load(std::memory_order_relaxed);
            for (;;) {
                if (refreshing(w)) return {idle_of(w), source::stale_while_refreshing};
                const auto budget = budget_of(w);
                if (budget == 0) break;
                // Spend one use. A failed CAS reloads `w`, so the loop is
                // lock-free and the TTL accounting stays exact under contention.
                if (word_.compare_exchange_weak(w, make(idle_of(w), budget - 1, version_of(w), false), std::memory_order_relaxed))
                    return {idle_of(w), source::cached};
            }

            // Budget exhausted: try to become the one refresher.
            if (word_.compare_exchange_strong(w, w | refresh_bit, std::memory_order_acquire, std::memory_order_relaxed)) {
                const auto idle = clamp(scan());
                word_.store(make(idle, ttl_, (version_of(w) + 1) & version_mask, false), std::memory_order_release);
                return {idle, source::scanned};
            }
            // Lost the race: someone else is refreshing, or a refresh just
            // landed. Either way their value is at least as fresh as what we'd
            // compute a moment from now -- use it, don't scan.
            return {idle_of(w), refreshing(w) ? source::stale_while_refreshing : source::cached};
        }

    private:
        static constexpr std::uint64_t refresh_bit = std::uint64_t{1} << 63;
        static constexpr std::uint64_t version_mask = 0x7FFFFFFFu;

        [[nodiscard]] static constexpr bool refreshing(std::uint64_t w) noexcept { return (w & refresh_bit) != 0; }
        [[nodiscard]] static constexpr std::size_t idle_of(std::uint64_t w) noexcept { return static_cast<std::size_t>(w & 0xFFFFu); }
        [[nodiscard]] static constexpr std::uint32_t budget_of(std::uint64_t w) noexcept { return static_cast<std::uint32_t>((w >> 16) & 0xFFFFu); }
        [[nodiscard]] static constexpr std::uint64_t version_of(std::uint64_t w) noexcept { return (w >> 32) & version_mask; }
        [[nodiscard]] static constexpr std::uint64_t make(std::size_t idle, std::uint32_t budget, std::uint64_t version, bool is_refreshing) noexcept {
            return (is_refreshing ? refresh_bit : 0) | ((version & version_mask) << 32) | (std::uint64_t{budget} << 16) | (idle & 0xFFFFu);
        }
        [[nodiscard]] static constexpr std::size_t clamp(std::size_t n) noexcept { return n > 0xFFFFu ? 0xFFFFu : n; }

        alignas(64) std::atomic<std::uint64_t> word_{0};
        std::uint32_t ttl_{8}; // written only by configure(), before workers start
        char pad_[64 - sizeof(std::uint32_t)]{};
    };

}
