#pragma once

#include "inline_function.hpp"

#include "../types.hpp"
#include "self_registering_hazards.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <functional>
#include <tuple>
#include <type_traits>

namespace FCS::Worker::detail {

    template<typename> struct callable_args;
    template<typename R, typename... Args> struct callable_args<R(*)(Args...)> { using type = std::tuple<Args...>; };
    template<typename R, typename... Args> struct callable_args<std::function<R(Args...)>> { using type = std::tuple<Args...>; };
    template<typename R, typename... Args, std::size_t B, std::size_t A> struct callable_args<inline_function<R(Args...), B, A>> { using type = std::tuple<Args...>; };
    template<typename C, typename R, typename... Args> struct callable_args<R(C::*)(Args...) const> { using type = std::tuple<Args...>; };
    template<typename C, typename R, typename... Args> struct callable_args<R(C::*)(Args...)> { using type = std::tuple<Args...>; };

    template<typename F>
    using callable_args_t = typename callable_args<decltype(&std::remove_reference_t<F>::operator())>::type;

    // A fixed-capacity, lock-free broadcast channel keyed by a compile-time Tag type.
    //
    // post() can be called from any thread — not just the pool's own bounded
    // worker set (a backend's poller thread, or arbitrary external callers of
    // pool_service::post(), both go through here too) — so subscriber
    // lifetime is managed with a hazard-protected refcount rather than a
    // fixed per-worker-id scheme: post() hazard-pins a subscriber just long
    // enough to safely take its own independent reference (try_retain()),
    // then hands that reference to the dispatched closure via subscriber_guard,
    // which releases it exactly once whether the closure runs normally, is
    // rejected, or is still queued unrun when the pool shuts down. A
    // subscriber is only actually freed once its refcount reaches zero *and*
    // no hazard slot still names it (see release_subscriber()) — cancellation
    // no longer leaks the subscriber object, only the slow, degrading trade-off
    // the old "retire in place, never delete" design made to stay safe.
    template<typename Tag, std::size_t CallbackBytes, typename... Args>
    class static_channel {
    public:
        template<typename F>
        [[nodiscard]] subscription subscribe(F&& function);

        template<typename Enqueue>
        [[nodiscard]] std::size_t post(Enqueue&& enqueue, const Args&... args);

    private:
        using callback_type = inline_function<void(Args...), CallbackBytes>; // inline, move-only, never allocates

        struct subscriber {
            std::atomic_bool active{true};
            callback_type callback;
            std::atomic_size_t refs{1}; // 1 for the slot's own reference, dropped by cancel_slot()

            // Takes an additional reference, but refuses (returns false)
            // once refs has already reached zero — i.e. never resurrects an
            // object on its way to being freed. Only safe to call while
            // holding a hazard pin on `this` (see static_channel::post()):
            // that guarantees release_subscriber()'s delete can't be racing
            // ahead of us unseen.
            [[nodiscard]] bool try_retain() noexcept {
                auto current = refs.load(std::memory_order_relaxed);
                while (current != 0) {
                    if (refs.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel, std::memory_order_relaxed)) return true;
                }
                return false;
            }

            // Unconditionally takes an additional reference. Only safe when
            // the caller already holds a live reference of its own (e.g.
            // copying an existing subscriber_guard) — unlike try_retain(),
            // there is no protection against a concurrent drop to zero here,
            // because there doesn't need to be: an already-live reference
            // guarantees refs can't reach zero out from under this call.
            void retain() noexcept { refs.fetch_add(1, std::memory_order_relaxed); }
        };

        // Shared-ownership handle: extends a subscriber's life for as long as
        // any copy of this is alive. release_subscriber() runs exactly once
        // per reference this hands out, whenever that copy is destroyed —
        // regardless of whether the dispatched closure it lives inside ran
        // normally, was rejected by the pool, or was discarded unrun at
        // shutdown. Copyable (not just movable) because std::function
        // requires its target type to be copy-constructible, even though the
        // dispatch closure itself is only ever actually moved in practice.
        struct subscriber_guard {
            static_channel* owner{};
            subscriber* entry{};

            subscriber_guard() = default;
            subscriber_guard(static_channel* o, subscriber* e) noexcept : owner(o), entry(e) {}
            subscriber_guard(const subscriber_guard& other) noexcept : owner(other.owner), entry(other.entry) { if (entry) entry->retain(); }
            subscriber_guard& operator=(const subscriber_guard& other) noexcept {
                if (this != &other) {
                    reset();
                    owner = other.owner;
                    entry = other.entry;
                    if (entry) entry->retain();
                }
                return *this;
            }
            subscriber_guard(subscriber_guard&& other) noexcept : owner(other.owner), entry(other.entry) { other.entry = nullptr; }
            subscriber_guard& operator=(subscriber_guard&& other) noexcept {
                if (this != &other) {
                    reset();
                    owner = other.owner;
                    entry = other.entry;
                    other.entry = nullptr;
                }
                return *this;
            }
            ~subscriber_guard() { reset(); }
            void reset() noexcept { if (entry) { owner->release_subscriber(entry); entry = nullptr; } }
        };

        static constexpr std::size_t capacity = 64;
        static constexpr std::size_t hazard_slots = 128;

        static bool cancel_slot(void* owner, std::size_t index) noexcept;
        void release_subscriber(subscriber* entry) noexcept;

        std::array<std::atomic<subscriber*>, capacity> slots_{};
        self_registering_hazard_domain<hazard_slots> hazards_{};
    };

    template<typename Tag, std::size_t CallbackBytes, typename Tuple> struct channel_from_tuple;
    template<typename Tag, std::size_t CallbackBytes, typename... Args> struct channel_from_tuple<Tag, CallbackBytes, std::tuple<Args...>> { using type = static_channel<Tag, CallbackBytes, std::decay_t<Args>...>; };

}

#include "channel.inl"

