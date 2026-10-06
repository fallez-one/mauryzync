#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace FCS::Worker::detail {

    // Defaults only. The real capacities are template parameters: a pool's task size is
    // pool_traits::task_bytes, its callback size pool_traits::callback_bytes.
    //  task: must hold a posted completion<Size> (about Size + 96 bytes), so 384 fits reads of up to ~256 bytes.
    inline constexpr std::size_t default_task_bytes = 384;
    inline constexpr std::size_t default_callback_bytes = 192;

    // A move-only, type-erased callable stored *inline*: Bytes of aligned storage inside the
    // object itself, no heap, ever. This is what replaces std::function in the scheduler's
    // hot path, for three reasons:
    //  * std::function heap-allocates any callable that does not fit its (implementation-
    //    defined, 16-32 byte) small buffer -- so whether a task allocates depends on what the
    //    caller happened to capture, on which standard library, and in which version. Here the
    //    capacity is a compile-time constant and exceeding it is a COMPILE ERROR, not a latent
    //    allocation. Every queue slot has a fixed, known size.
    //  * std::function requires its target to be copy-constructible. Tasks are run exactly
    //    once and only ever moved, so this accepts move-only callables (unique_ptr captures,
    //    move-only argument packs) that std::function rejects.
    //  * Invocation, move and destruction are three plain function pointers in a static
    //    per-type table -- no virtual dispatch, no RTTI, nothing to allocate or throw.
    //
    // A callable too large for Bytes can be passed through FCS::Worker::boxed(), which
    // heap-allocates it once, explicitly, at the call site (the pointer is what is stored).
    template<typename Signature, std::size_t Bytes = default_task_bytes, std::size_t Align = alignof(std::max_align_t)>
    class inline_function;

    template<typename Signature>
    using callback_function = inline_function<Signature, default_callback_bytes>;

    template<typename R, typename... Args, std::size_t Bytes, std::size_t Align>
    class inline_function<R(Args...), Bytes, Align> {
    public:
        static constexpr std::size_t capacity = Bytes;

        inline_function() noexcept = default;
        inline_function(std::nullptr_t) noexcept {}

        template<typename F, typename D = std::decay_t<F>>
            requires(!std::is_same_v<D, inline_function> && std::is_invocable_r_v<R, D&, Args...>)
        inline_function(F&& f) noexcept(std::is_nothrow_constructible_v<D, F>) {
            static_assert(sizeof(D) <= Bytes,
                "callable is too large for the inline buffer: capture less, capture by reference, "
                "raise the pool's traits::task_bytes (or callback_bytes), or wrap it in FCS::Worker::boxed(...)");
            static_assert(alignof(D) <= Align, "callable is over-aligned for the inline task buffer");
            static_assert(std::is_nothrow_move_constructible_v<D>,
                "tasks are relocated between queues and must be nothrow-move-constructible");
            ::new (static_cast<void*>(storage_)) D(std::forward<F>(f));
            ops_ = &table<D>;
        }

        inline_function(const inline_function&) = delete;
        inline_function& operator=(const inline_function&) = delete;

        inline_function(inline_function&& other) noexcept { take(std::move(other)); }
        inline_function& operator=(inline_function&& other) noexcept {
            if (this != &other) { reset(); take(std::move(other)); }
            return *this;
        }
        inline_function& operator=(std::nullptr_t) noexcept { reset(); return *this; }

        ~inline_function() { reset(); }

        explicit operator bool() const noexcept { return ops_ != nullptr; }

        // const like std::function::operator(): the stored object is a mutable lambda's state,
        // not part of this wrapper's logical value.
        R operator()(Args... args) const { return ops_->invoke(const_cast<unsigned char*>(storage_), std::forward<Args>(args)...); }

        void reset() noexcept {
            if (ops_ != nullptr) { ops_->destroy(storage_); ops_ = nullptr; }
        }

    private:
        struct vtable {
            R (*invoke)(unsigned char*, Args&&...);
            void (*relocate)(unsigned char* dst, unsigned char* src) noexcept; // move-construct into dst, destroy src
            void (*destroy)(unsigned char*) noexcept;
        };

        template<typename D>
        static constexpr vtable table{
            [](unsigned char* p, Args&&... a) -> R { return std::invoke(*std::launder(reinterpret_cast<D*>(p)), std::forward<Args>(a)...); },
            [](unsigned char* dst, unsigned char* src) noexcept {
                auto* from = std::launder(reinterpret_cast<D*>(src));
                ::new (static_cast<void*>(dst)) D(std::move(*from));
                std::destroy_at(from);
            },
            [](unsigned char* p) noexcept { std::destroy_at(std::launder(reinterpret_cast<D*>(p))); },
        };

        void take(inline_function&& other) noexcept {
            if (other.ops_ != nullptr) {
                other.ops_->relocate(storage_, other.storage_);
                ops_ = other.ops_;
                other.ops_ = nullptr;
            }
        }

        alignas(Align) unsigned char storage_[Bytes];
        const vtable* ops_{nullptr};
    };

}

namespace FCS::Worker {

    // Escape hatch for a callable that does not fit the inline buffer: allocates it once, here,
    // visibly, and hands the scheduler a pointer-sized wrapper. Nothing else in the scheduler
    // allocates for a task.
    template<typename F>
    [[nodiscard]] auto boxed(F&& f) {
        using D = std::decay_t<F>;
        return [owned = std::make_unique<D>(std::forward<F>(f))](auto&&... args) mutable -> decltype(auto) {
            return std::invoke(*owned, std::forward<decltype(args)>(args)...);
        };
    }

}
