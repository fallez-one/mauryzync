#pragma once

#include <cstdint>

namespace FCS::Worker::detail {

    template<typename T, std::size_t Capacity>
        requires (Capacity != 0 && (Capacity & (Capacity - 1)) == 0)
    bounded_mpmc_queue<T, Capacity>::bounded_mpmc_queue() noexcept {
        for (std::size_t i{}; i < Capacity; ++i) sequence_[i].store(i, std::memory_order_relaxed);
    }

    template<typename T, std::size_t Capacity>
        requires (Capacity != 0 && (Capacity & (Capacity - 1)) == 0)
    bool bounded_mpmc_queue<T, Capacity>::try_push(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>) {
        auto position = enqueue_.load(std::memory_order_relaxed);
        for (;;) {
            auto& sequence = sequence_[position & mask];
            const auto observed = sequence.load(std::memory_order_acquire);
            const auto delta = static_cast<std::intptr_t>(observed) - static_cast<std::intptr_t>(position);
            if (delta == 0) {
                if (enqueue_.compare_exchange_weak(position, position + 1, std::memory_order_relaxed)) {
                    storage_[position & mask].emplace(std::move(value));
                    sequence.store(position + 1, std::memory_order_release);
                    return true;
                }
            } else if (delta < 0) {
                return false;
            } else {
                position = enqueue_.load(std::memory_order_relaxed);
            }
        }
    }

    template<typename T, std::size_t Capacity>
        requires (Capacity != 0 && (Capacity & (Capacity - 1)) == 0)
    bool bounded_mpmc_queue<T, Capacity>::try_pop(T& value) noexcept(std::is_nothrow_move_assignable_v<T>) {
        auto position = dequeue_.load(std::memory_order_relaxed);
        for (;;) {
            auto& sequence = sequence_[position & mask];
            const auto observed = sequence.load(std::memory_order_acquire);
            const auto delta = static_cast<std::intptr_t>(observed) - static_cast<std::intptr_t>(position + 1);
            if (delta == 0) {
                if (dequeue_.compare_exchange_weak(position, position + 1, std::memory_order_relaxed)) {
                    value = std::move(*storage_[position & mask]);
                    storage_[position & mask].reset();
                    sequence.store(position + Capacity, std::memory_order_release);
                    return true;
                }
            } else if (delta < 0) {
                return false;
            } else {
                position = dequeue_.load(std::memory_order_relaxed);
            }
        }
    }

}
