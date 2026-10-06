#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <optional>
#include <type_traits>

namespace FCS::Worker::detail {

    template<typename T, std::size_t Capacity>
        requires (Capacity != 0 && (Capacity & (Capacity - 1)) == 0)
    class bounded_mpmc_queue {
    public:
        bounded_mpmc_queue() noexcept;

        [[nodiscard]] bool try_push(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>);
        [[nodiscard]] bool try_pop(T& value) noexcept(std::is_nothrow_move_assignable_v<T>);

    private:
        static constexpr std::size_t mask = Capacity - 1;

        alignas(64) std::atomic_size_t enqueue_{};
        alignas(64) std::atomic_size_t dequeue_{};
        alignas(64) std::array<std::atomic_size_t, Capacity> sequence_{};
        std::array<std::optional<T>, Capacity> storage_{};
    };

}

#include "mpmc_queue.inl"
