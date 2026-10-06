#ifndef FCS_SIGNATURE_EXPECTED
#define FCS_SIGNATURE_EXPECTED

// FCS::Expected<T, E> / FCS::Unexpected<E> -- verbatim from FCS-Servant's
// result.hpp (uploaded), just with this project's #pragma once convention
// swapped in for the include-guard macro. Authoritative source; do not
// hand-edit here without updating result.hpp upstream too.

#include <variant>
#include <utility>

namespace FCS {

    // Helper to wrap error values so they are distinguishable from T
    template<typename E>
    class Unexpected {
        E err_;
    public:
        constexpr explicit Unexpected(E err) noexcept(std::is_nothrow_move_constructible_v<E>) : err_(std::move(err)) {}

        constexpr const E& error() const & noexcept { return err_; }
        constexpr E& error() & noexcept { return err_; }
        constexpr E&& error() && noexcept { return std::move(err_); }
    };

    // Deduction guide
    template<typename E>
    Unexpected(E) -> Unexpected<E>;

    template<typename T, typename E>
    class Expected {
        std::variant<T, Unexpected<E>> data_;

    public:
        // Restore default construction (default-initializes T in std::variant)
        constexpr Expected() noexcept(std::is_nothrow_default_constructible_v<T>) = default;
        // Construct Success
        constexpr Expected(T value) noexcept(std::is_nothrow_move_constructible_v<T>)
            : data_(std::move(value)) {}

        // Construct Failure
        constexpr Expected(Unexpected<E> unex) noexcept(std::is_nothrow_move_constructible_v<Unexpected<E>>)
            : data_(std::move(unex)) {}

        // Observers
        [[nodiscard]] constexpr bool has_value() const noexcept {
            return std::holds_alternative<T>(data_);
        }

        [[nodiscard]] constexpr explicit operator bool() const noexcept {
            return has_value();
        }

        // Accessors (Value)
        [[nodiscard]] constexpr T& value() & noexcept {
            return std::get<T>(data_);
        }

        [[nodiscard]] constexpr const T& value() const & noexcept {
            return std::get<T>(data_);
        }

        [[nodiscard]] constexpr T&& value() && noexcept {
            return std::get<T>(std::move(data_));
        }

        // Pointer-based Safe Accessors
        [[nodiscard]] constexpr T* value_ptr() noexcept {
            return std::get_if<T>(&data_);
        }

        [[nodiscard]] constexpr const T* value_ptr() const noexcept {
            return std::get_if<T>(&data_);
        }

        [[nodiscard]] constexpr E* error_ptr() noexcept {
            auto* unex = std::get_if<Unexpected<E>>(&data_);
            return unex ? &unex->error() : nullptr;
        }

        [[nodiscard]] constexpr const E* error_ptr() const noexcept {
            auto* unex = std::get_if<Unexpected<E>>(&data_);
            return unex ? &unex->error() : nullptr;
        }

        // Accessors (Error)
        [[nodiscard]] constexpr E& error() & noexcept {
            return std::get<Unexpected<E>>(data_).error();
        }

        [[nodiscard]] constexpr const E& error() const & noexcept {
            return std::get<Unexpected<E>>(data_).error();
        }

        [[nodiscard]] constexpr E&& error() && noexcept {
            return std::get<Unexpected<E>>(std::move(data_)).error();
        }

        // Dereference operators
        [[nodiscard]] constexpr T& operator*() & noexcept { return value(); }
        [[nodiscard]] constexpr const T& operator*() const & noexcept { return value(); }
        [[nodiscard]] constexpr T* operator->() noexcept { return &value(); }
        [[nodiscard]] constexpr const T* operator->() const noexcept { return &value(); }
    };

} // namespace FCS

#endif