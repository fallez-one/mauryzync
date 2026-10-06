#pragma once

#include "../types.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace FCS::Worker::detail {

    [[nodiscard]] inline queue_state classify_queue_state(std::uint64_t depth, std::size_t available_threshold, std::size_t full_threshold) noexcept {
        if (depth == 0) return queue_state::empty;
        if (depth <= std::min(available_threshold, full_threshold - 1)) return queue_state::available;
        if (depth >= full_threshold) return queue_state::full;
        return queue_state::occupied;
    }

}
