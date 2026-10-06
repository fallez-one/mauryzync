#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <optional>

namespace FCS::Worker::detail {

    // Lightweight stand-in for shared_ptr: no control-block indirection, no weak count,
    // just a raw pointer to a type exposing retain()/release(). Copy-friendly for closures.
    template<typename T>
    class intrusive_ptr {
    public:
        intrusive_ptr() noexcept = default;
        [[nodiscard]] static intrusive_ptr adopt(T* raw) noexcept;

        intrusive_ptr(const intrusive_ptr& other) noexcept;
        intrusive_ptr& operator=(const intrusive_ptr& other) noexcept;
        intrusive_ptr(intrusive_ptr&& other) noexcept;
        intrusive_ptr& operator=(intrusive_ptr&& other) noexcept;
        ~intrusive_ptr();

        [[nodiscard]] T* operator->() const noexcept { return ptr_; }
        [[nodiscard]] T& operator*() const noexcept { return *ptr_; }
        [[nodiscard]] T* get() const noexcept { return ptr_; }

    private:
        T* ptr_{};
    };

    // A minimal, intrusively-refcounted single-producer/single-consumer promise cell.
    // Replaces a shared_ptr<T>+mutex pairing with one handoff atomic and one owner count.
    template<typename T>
    class scheduled_state {
    public:
        explicit scheduled_state(int owners) noexcept;

        void retain() noexcept;
        void release() noexcept;

        void set_value(T value) noexcept;
        void set_exception(std::exception_ptr error) noexcept;

        [[nodiscard]] bool is_ready() const noexcept;
        [[nodiscard]] bool attach(std::coroutine_handle<> handle) noexcept;
        [[nodiscard]] T take_value();

    private:
        static constexpr std::uintptr_t ready_flag = 1;

        std::optional<T> value_{};
        std::exception_ptr error_{};
        std::atomic<std::uintptr_t> continuation_{};
        std::atomic<int> owners_;
    };

}

#include "scheduled_result.inl"
