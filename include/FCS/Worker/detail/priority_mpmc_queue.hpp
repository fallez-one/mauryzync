#pragma once

#include "mpmc_queue.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace FCS::Worker::detail {

    // Checkpoint granularity for starvation_policy -- how often try_pop()'s
    // "served high again" counter actually advances. `task` advances it
    // every pop; `segment` batches segment_size pops into one advance, for
    // lower bookkeeping overhead at the cost of coarser aging response.
    enum class aging_granularity : unsigned char { task, segment };

    // Runtime knobs for priority_mpmc_queue's anti-starvation aging --
    // see pool_service::age_starvation(). Defaults reproduce this queue's
    // original fixed fairness-window behavior exactly (hard cutoff at
    // tasks_count checkpoints, no gradual easing) since that's the one
    // already exercised in testing; base_growth/max are there for whoever
    // wants a softer ramp instead of a cliff.
    struct starvation_policy {
        aging_granularity clamp_when{aging_granularity::task};
        std::uint64_t tasks_count{8};  // checkpoints a normal-lane pop can be skipped before aging starts
        std::uint8_t base_growth{1};   // ticket growth per checkpoint once aging has started
        std::uint8_t max{0};           // ceiling on total growth; 0 = forced normal-first right at tasks_count
    };

    // Two lock-free bounded_mpmc_queue lanes (high/normal) behind the exact
    // try_push(T&&)/try_pop(T&) surface of a plain one, so it drops straight
    // into fast_ with no caller changes. try_push reads T::priority to pick
    // a lane; try_pop favors high but ages normal in once it's been skipped
    // past tasks_count checkpoints, so a burst on the hot path can't starve
    // the cold one indefinitely -- see starvation_policy and configure().
    //
    // All the bookkeeping here (skipped_, the policy fields) is a soft
    // heuristic, not a correctness invariant -- relaxed ops and a benign
    // race across pushers/configure() are fine, since aging only needs to
    // be roughly honored, not exact.
    template<typename T, std::size_t Capacity>
    class priority_mpmc_queue {
    public:
        // Not on the try_push/try_pop hot path -- expected to be called
        // rarely (once, at setup, via pool_service::age_starvation()) --
        // so torn reads of a config caught mid-update by a concurrent
        // try_pop are an acceptable, self-correcting race, same spirit as
        // skipped_ below.
        void configure(starvation_policy policy) noexcept {
            clamp_when_.store(policy.clamp_when, std::memory_order_relaxed);
            tasks_count_.store(policy.tasks_count, std::memory_order_relaxed);
            base_growth_.store(policy.base_growth, std::memory_order_relaxed);
            max_.store(policy.max, std::memory_order_relaxed);
        }

        [[nodiscard]] bool try_push(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>) {
            return value.priority == task_priority::high ? high_.try_push(std::move(value)) : normal_.try_push(std::move(value));
        }

        [[nodiscard]] bool try_pop(T& value) noexcept(std::is_nothrow_move_assignable_v<T>) {
            const auto growth = base_growth_.load(std::memory_order_relaxed);
            const auto cap = max_.load(std::memory_order_relaxed);
            const auto threshold = tasks_count_.load(std::memory_order_relaxed);
            // Total checkpoints tolerated before a forced normal-first pull:
            // the base threshold plus however many checkpoints it takes the
            // ticket to grow from 0 to cap at `growth` per checkpoint.
            const std::uint64_t forced_at = threshold + (growth == 0 ? 0 : (static_cast<std::uint64_t>(cap) + growth - 1) / growth);

            if (skipped_.load(std::memory_order_relaxed) < forced_at) {
                if (high_.try_pop(value)) {
                    if (is_checkpoint()) skipped_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
                skipped_.store(0, std::memory_order_relaxed);
                return normal_.try_pop(value);
            }
            // Aged out: normal gets first refusal so a steady stream of
            // high-priority pushes can't starve it indefinitely.
            skipped_.store(0, std::memory_order_relaxed);
            return normal_.try_pop(value) || high_.try_pop(value);
        }

    private:
        static constexpr std::uint64_t segment_size = 32;

        [[nodiscard]] bool is_checkpoint() noexcept {
            if (clamp_when_.load(std::memory_order_relaxed) == aging_granularity::task) return true;
            return segment_counter_.fetch_add(1, std::memory_order_relaxed) % segment_size == segment_size - 1;
        }

        bounded_mpmc_queue<T, Capacity> high_;
        bounded_mpmc_queue<T, Capacity> normal_;
        std::atomic_uint64_t skipped_{};
        std::atomic_uint64_t segment_counter_{};

        std::atomic<aging_granularity> clamp_when_{aging_granularity::task};
        std::atomic_uint64_t tasks_count_{8};
        std::atomic<std::uint8_t> base_growth_{1};
        std::atomic<std::uint8_t> max_{0};
    };

}
