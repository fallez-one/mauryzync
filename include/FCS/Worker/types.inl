#pragma once

namespace FCS::Worker {

    inline subscription::subscription(void* owner, cancel_fn cancel, std::size_t slot) noexcept
        : owner_(owner), cancel_(cancel), slot_(slot) {}

    inline subscription::subscription(subscription&& other) noexcept
        : owner_(std::exchange(other.owner_, nullptr))
        , cancel_(std::exchange(other.cancel_, nullptr))
        , slot_(other.slot_) {}

    inline subscription& subscription::operator=(subscription&& other) noexcept {
        if (this == &other) return *this;
        (void)cancel();
        owner_ = std::exchange(other.owner_, nullptr);
        cancel_ = std::exchange(other.cancel_, nullptr);
        slot_ = other.slot_;
        return *this;
    }

    inline subscription::~subscription() { (void)cancel(); }

    inline bool subscription::cancel() noexcept {
        if (!cancel_) return false;
        const auto owner = std::exchange(owner_, nullptr);
        const auto fn = std::exchange(cancel_, nullptr);
        return fn(owner, slot_);
    }

    inline bool subscription::retire() noexcept {
        if (!cancel_) return false;
        const auto owner = std::exchange(owner_, nullptr);
        const auto fn = std::exchange(cancel_, nullptr);
        return fn(owner, slot_ | retire_flag);
    }

    inline bool subscription::active() const noexcept { return cancel_ != nullptr; }
    inline subscription::operator bool() const noexcept { return active(); }

}
