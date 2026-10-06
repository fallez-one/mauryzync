#pragma once

#include <utility>

namespace FCS::Worker::detail {

    template<typename T>
    intrusive_ptr<T> intrusive_ptr<T>::adopt(T* raw) noexcept { intrusive_ptr p; p.ptr_ = raw; return p; }

    template<typename T>
    intrusive_ptr<T>::intrusive_ptr(const intrusive_ptr& other) noexcept : ptr_(other.ptr_) { if (ptr_) ptr_->retain(); }

    template<typename T>
    intrusive_ptr<T>& intrusive_ptr<T>::operator=(const intrusive_ptr& other) noexcept {
        if (this == &other) return *this;
        if (other.ptr_) other.ptr_->retain();
        if (ptr_) ptr_->release();
        ptr_ = other.ptr_;
        return *this;
    }

    template<typename T>
    intrusive_ptr<T>::intrusive_ptr(intrusive_ptr&& other) noexcept : ptr_(std::exchange(other.ptr_, nullptr)) {}

    template<typename T>
    intrusive_ptr<T>& intrusive_ptr<T>::operator=(intrusive_ptr&& other) noexcept {
        if (this == &other) return *this;
        if (ptr_) ptr_->release();
        ptr_ = std::exchange(other.ptr_, nullptr);
        return *this;
    }

    template<typename T>
    intrusive_ptr<T>::~intrusive_ptr() { if (ptr_) ptr_->release(); }

    template<typename T>
    scheduled_state<T>::scheduled_state(int owners) noexcept : owners_(owners) {}

    template<typename T>
    void scheduled_state<T>::retain() noexcept { owners_.fetch_add(1, std::memory_order_relaxed); }

    template<typename T>
    void scheduled_state<T>::release() noexcept {
        if (owners_.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this;
    }

    template<typename T>
    void scheduled_state<T>::set_value(T value) noexcept {
        value_.emplace(std::move(value));
        const auto prev = continuation_.exchange(ready_flag, std::memory_order_acq_rel);
        if (prev != 0 && prev != ready_flag) std::coroutine_handle<>::from_address(reinterpret_cast<void*>(prev)).resume();
    }

    template<typename T>
    void scheduled_state<T>::set_exception(std::exception_ptr error) noexcept {
        error_ = std::move(error);
        const auto prev = continuation_.exchange(ready_flag, std::memory_order_acq_rel);
        if (prev != 0 && prev != ready_flag) std::coroutine_handle<>::from_address(reinterpret_cast<void*>(prev)).resume();
    }

    template<typename T>
    bool scheduled_state<T>::is_ready() const noexcept {
        return continuation_.load(std::memory_order_acquire) == ready_flag;
    }

    template<typename T>
    bool scheduled_state<T>::attach(std::coroutine_handle<> handle) noexcept {
        const auto prev = continuation_.exchange(reinterpret_cast<std::uintptr_t>(handle.address()), std::memory_order_acq_rel);
        return prev != ready_flag;
    }

    template<typename T>
    T scheduled_state<T>::take_value() {
        if (error_) std::rethrow_exception(error_);
        return std::move(*value_);
    }

}
