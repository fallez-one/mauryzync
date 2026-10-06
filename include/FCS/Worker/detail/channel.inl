#pragma once

#include <thread>
#include <utility>

namespace FCS::Worker::detail {

    template<typename Tag, std::size_t CallbackBytes, typename... Args>
    template<typename F>
    subscription static_channel<Tag, CallbackBytes, Args...>::subscribe(F&& function) {
        auto* entry = new subscriber{true, callback_type{std::forward<F>(function)}};
        for (std::size_t index{}; index < capacity; ++index) {
            subscriber* expected = nullptr;
            if (slots_[index].compare_exchange_strong(expected, entry, std::memory_order_release, std::memory_order_relaxed)) {
                return subscription{this, &static_channel::cancel_slot, index};
            }
        }
        delete entry;
        return {};
    }

    template<typename Tag, std::size_t CallbackBytes, typename... Args>
    template<typename Enqueue>
    std::size_t static_channel<Tag, CallbackBytes, Args...>::post(Enqueue&& enqueue, const Args&... args) {
        std::size_t accepted{};
        for (auto& slot : slots_) {
            auto* entry = hazards_.protect(slot);
            if (!entry) { hazards_.clear(); continue; }

            if (!entry->active.load(std::memory_order_acquire) || !entry->try_retain()) {
                // Either cancelled, or a concurrent release_subscriber() has
                // already brought its refcount to zero (which, once it
                // happens, is permanent — see subscriber::try_retain()).
                // Either way there's nothing to dispatch to.
                hazards_.clear();
                continue;
            }
            // Safe to drop our hazard pin now: try_retain() succeeding means
            // we hold our own independent reference, which alone keeps
            // `entry` alive regardless of what cancel_slot() does next.
            hazards_.clear();

            auto dispatch = [guard = subscriber_guard{this, entry}, payload = std::tuple<std::decay_t<Args>...>{args...}]() mutable {
                if (guard.entry->active.load(std::memory_order_acquire)) std::apply(guard.entry->callback, std::move(payload));
            };
            if (std::forward<Enqueue>(enqueue)(std::move(dispatch))) ++accepted;
            // Whether accepted or rejected, `dispatch` (and its guard member)
            // is destroyed exactly once — either later, after running (or
            // sitting unrun in a queue until shutdown), or right here if the
            // pool rejected it — releasing our reference in every case.
        }
        return accepted;
    }

    template<typename Tag, std::size_t CallbackBytes, typename... Args>
    void static_channel<Tag, CallbackBytes, Args...>::release_subscriber(subscriber* entry) noexcept {
        if (entry->refs.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
        // We are provably the unique caller that brought refs to zero:
        // try_retain() can never take it from zero back to nonzero, so
        // nothing can resurrect it after this point. Any post() call still
        // mid-hazard-pin on this entry either has its pin visible to the
        // scan below already (in which case we wait, and its own
        // try_retain() call is guaranteed to observe refs == 0 and fail —
        // it read `entry` while it was genuinely still alive, it just won't
        // get a reference out of it), or its pin isn't visible to us *yet*,
        // in which case the same guarantee holds once it is. Once no hazard
        // slot names this entry, nothing can still be reading it.
        while (hazards_.protected_by_someone(entry)) std::this_thread::yield();
        delete entry;
    }

    template<typename Tag, std::size_t CallbackBytes, typename... Args>
    bool static_channel<Tag, CallbackBytes, Args...>::cancel_slot(void* owner, std::size_t index) noexcept {
        auto* self = static_cast<static_channel*>(owner);
        // subscription::retire(): unlink so no *new* post() reaches us, but leave
        // `active` set so dispatches already queued (each holding its own
        // reference) still run -- see subscription::retire().
        const bool retire_only = (index & subscription::retire_flag) != 0;
        index &= ~subscription::retire_flag;
        auto* entry = self->slots_[index].exchange(nullptr, std::memory_order_acq_rel);
        if (!entry) return false;
        if (!retire_only) entry->active.store(false, std::memory_order_release);
        self->release_subscriber(entry); // drops the slot's own reference
        return true;
    }

}
