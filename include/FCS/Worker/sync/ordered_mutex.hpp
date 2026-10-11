#ifndef FCS_SYNC_ORDERED_MUTEX
#define FCS_SYNC_ORDERED_MUTEX

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "mutex_traits.hpp"

namespace FCS::synchronization {

    // What happens when more threads queue than the ticket type has numbers for.
    enum class capacity_policy {
        // The caller guarantees fewer than 2^N threads are ever in lock()/lock_interruptible() at once
        // (255 for uint8_t). Fastest: taking a ticket is one fetch_add. Break the promise and ticket ==
        // serving again while threads are still queued, so the queue looks empty: two threads end up inside.
        unchecked,
        // A thread that would take the last distinguishable ticket waits (inside the same word, on the same
        // notification) until one is free instead. Safe for any number of threads, at the price of taking
        // tickets with a CAS loop rather than a fetch_add. Pick this when the thread count is not under your control.
        block,
    };

    // A FIFO (ticket) mutex whose queued acquirers can be cancelled all at once.
    //
    // EVERYTHING lives in one atomic word -- two counters, three flags and a generation -- so every state
    // change is a single linearisable RMW, nothing needs a second variable to agree with it, and the word
    // is exactly what futex / WaitOnAddress wait on (32 bits for uint8_t tickets, 64 for uint16_t):
    //
    //   [ ticket : N | serving : N | WAITERS | INTERRUPTED | LOCKED | generation ]      (high -> low)
    //
    //   ticket      next number to hand out (the TOP field: its carry falls off the end of the word,
    //               so a plain fetch_add takes a ticket and wraps for free)
    //   serving     whose turn it is
    //   LOCKED      a thread has ENTERED the critical section. Not derivable from the counters: a waiter
    //               whose turn has come but who has not yet woken is "served" without holding anything,
    //               and interrupt() must be able to tell the two apart.
    //   WAITERS     somebody may be asleep: the next state change must notify
    //   INTERRUPTED sticky until reset_interrupt(); lock_interruptible() fails while it is set
    //   generation  bumped by interrupt(); a waiter that finds a different generation than the one it
    //               took its ticket in has been cancelled
    //
    // Cancellation is therefore just "reset the counters": interrupt() sets ticket = serving = 0 (or
    // ticket = 1 when somebody is inside, who becomes ticket 0) and bumps the generation. No holes to
    // skip, no per-ticket bookkeeping. Queued lock_interruptible() calls return false; queued plain
    // lock() calls (which mask the interrupt) transparently take a fresh ticket -- so under an interrupt
    // they re-queue at the back instead of keeping their place.
    //
    // Lost wake-ups: waiters sleep on the whole word and every state change alters it, so a change
    // cannot be missed (a stale expected value just returns immediately), and the word cannot repeat
    // while a waiter is asleep (that would need serving to lap the waiter's own turn).
    //
    // Capacity: see capacity_policy. With `unchecked` (the default) fewer than 2^N threads may be in
    // lock()/lock_interruptible() at once; with `block` there is no limit.
    template<std::unsigned_integral EntrySizeType, capacity_policy Policy = capacity_policy::unchecked>
    class ordered_mutex {
        static_assert(!std::is_same_v<EntrySizeType, bool>);
        static_assert(sizeof(EntrySizeType) <= 2, "two counters plus flags and a generation must fit one 32- or 64-bit atomic");

        static constexpr unsigned N = 8u * sizeof(EntrySizeType);
        using word_t = std::conditional_t<(N == 8), std::uint32_t, std::uint64_t>;
        static constexpr unsigned W = 8u * sizeof(word_t);
        static constexpr unsigned gen_bits = W - 2 * N - 3;
        // Storage budget: one state byte (3 flags + a generation of at least 5 bits) plus the two counters, i.e. 1 + 2*N
        // bytes of information. A std::atomic can only be 4 or 8 bytes wide, so the word is rounded up to that; the
        // slack is not wasted, it is simply a wider generation. The capacity_policy::block check adds NO storage:
        // it only compares the two counters already in the word.
        static_assert(gen_bits >= 5, "the state byte's worth of generation bits must fit next to the two counters");
        static_assert(8 + 2 * N <= W, "state byte + two counters must fit one atomic word");
        static constexpr word_t gen_mask = (word_t{1} << gen_bits) - 1;
        static constexpr word_t F_LOCKED = word_t{1} << gen_bits;
        static constexpr word_t F_INTERRUPTED = word_t{1} << (gen_bits + 1);
        static constexpr word_t F_WAITERS = word_t{1} << (gen_bits + 2);
        static constexpr unsigned serving_shift = gen_bits + 3;
        static constexpr unsigned ticket_shift = serving_shift + N;
        static constexpr word_t entry_mask = (word_t{1} << N) - 1;
        static constexpr word_t serving_mask = entry_mask << serving_shift;
        static constexpr word_t ticket_one = word_t{1} << ticket_shift;
        static constexpr word_t serving_one = word_t{1} << serving_shift;

        static constexpr word_t ticket_of(word_t w) noexcept { return w >> ticket_shift; } // top field: no mask needed
        static constexpr word_t serving_of(word_t w) noexcept { return (w >> serving_shift) & entry_mask; }
        static constexpr word_t gen_of(word_t w) noexcept { return w & gen_mask; }
        static constexpr word_t with_serving(word_t w, word_t s) noexcept { return (w & ~serving_mask) | ((s & entry_mask) << serving_shift); }
        static constexpr word_t with_ticket(word_t w, word_t t) noexcept { return (w & ~(entry_mask << ticket_shift)) | ((t & entry_mask) << ticket_shift); }
        static constexpr word_t next_gen(word_t w) noexcept { return (w & ~gen_mask) | ((w + 1) & gen_mask); }

        static constexpr bool checked = (Policy == capacity_policy::block);

        // Taking one more ticket would make ticket == serving while somebody is queued or inside.
        static constexpr bool no_room(word_t w) noexcept { return ((ticket_of(w) + 1) & entry_mask) == serving_of(w); }

        // block policy: sleep until the word changes (an unlock() frees a number), publishing WAITERS first.
        void wait_for_room(word_t w) noexcept {
            if (!(w & F_WAITERS)) {
                if (!word.compare_exchange_weak(w, w | F_WAITERS, std::memory_order_relaxed, std::memory_order_relaxed)) return; // changed: re-evaluate
                w |= F_WAITERS;
            }
            word.wait(w, std::memory_order_relaxed);
        }

        std::atomic<word_t> word{0};

        enum class outcome { acquired, cancelled };

        // Sleep until it is our turn (then enter), or our generation is cancelled.
        outcome wait_turn(word_t my, word_t gen) noexcept {
            for (;;) {
                word_t w = word.load(std::memory_order_acquire);
                if (gen_of(w) != gen) return outcome::cancelled;
                if (!(w & F_LOCKED) && serving_of(w) == my) {
                    // Our turn. Enter by claiming LOCKED -- one CAS on the whole word, so if interrupt() got in
                    // first the CAS fails and we see the new generation instead: we never enter on a stale turn.
                    if (word.compare_exchange_weak(w, w | F_LOCKED, std::memory_order_acquire, std::memory_order_relaxed)) return outcome::acquired;
                    continue;
                }
                if (!(w & F_WAITERS)) {
                    if (!word.compare_exchange_weak(w, w | F_WAITERS, std::memory_order_relaxed, std::memory_order_relaxed)) continue;
                    w |= F_WAITERS;
                }
                word.wait(w, std::memory_order_relaxed); // returns when the word != w: unlock(), interrupt() or a spurious wake
            }
        }

    public:
        using entry_type = EntrySizeType;
        static constexpr std::size_t information_bytes = 1 + 2 * sizeof(EntrySizeType); // what the design needs
        static constexpr std::size_t generation_bits = gen_bits;                         // what the spare width buys

        // Plain lock(): never fails. If the line is cancelled while we wait, take a new ticket.
        void lock() noexcept {
            word_t w = word.load(std::memory_order_relaxed);
            // uncontended: nobody inside, nobody queued -> take the ticket AND enter in one CAS
            if (!(w & F_LOCKED) && ticket_of(w) == serving_of(w)) {
                if (word.compare_exchange_strong(w, with_ticket(w, ticket_of(w) + 1) | F_LOCKED, std::memory_order_acquire, std::memory_order_relaxed)) return;
            }
            for (;;) {
                word_t before;
                if constexpr (checked) {
                    // take a ticket only if one is free; otherwise wait for an unlock() to free one
                    word_t cur = word.load(std::memory_order_relaxed);
                    for (;;) {
                        if (no_room(cur)) { wait_for_room(cur); cur = word.load(std::memory_order_relaxed); continue; }
                        if (word.compare_exchange_weak(cur, with_ticket(cur, ticket_of(cur) + 1), std::memory_order_relaxed, std::memory_order_relaxed)) break;
                    }
                    before = cur;
                } else {
                    before = word.fetch_add(ticket_one, std::memory_order_relaxed); // atomic: ticket + generation together
                }
                if (wait_turn(ticket_of(before), gen_of(before)) == outcome::acquired) return;
            }
        }

        [[nodiscard]] bool try_lock() noexcept {
            word_t w = word.load(std::memory_order_relaxed);
            while (!(w & F_LOCKED) && ticket_of(w) == serving_of(w)) {
                if (word.compare_exchange_weak(w, with_ticket(w, ticket_of(w) + 1) | F_LOCKED, std::memory_order_acquire, std::memory_order_relaxed)) return true;
            }
            return false;
        }

        // false: interrupted -- before queueing or while queued -- and the mutex is NOT held.
        [[nodiscard]] bool lock_interruptible() noexcept {
            word_t w = word.load(std::memory_order_relaxed);
            for (;;) {
                if (w & F_INTERRUPTED) return false;
                if constexpr (checked) {
                    if (no_room(w)) { wait_for_room(w); w = word.load(std::memory_order_relaxed); continue; } // re-checks INTERRUPTED each time round
                }
                // Take the ticket only if the flag is still clear: a CAS, not a fetch_add, so the check and the
                // ticket are one atomic step (a ticket taken and then "un-taken" would leave a hole in the line).
                const bool free_now = !(w & F_LOCKED) && ticket_of(w) == serving_of(w);
                const word_t desired = with_ticket(w, ticket_of(w) + 1) | (free_now ? F_LOCKED : word_t{0});
                if (word.compare_exchange_weak(w, desired, std::memory_order_acquire, std::memory_order_relaxed)) {
                    if (free_now) return true;
                    return wait_turn(ticket_of(w), gen_of(w)) == outcome::acquired; // w still holds the pre-CAS value
                }
            }
        }

        void unlock() noexcept {
            word_t w = word.load(std::memory_order_relaxed);
            // Leave the section and hand the turn on, clearing WAITERS in the same step (waiters re-assert it).
            while (!word.compare_exchange_weak(w, with_serving(w, serving_of(w) + 1) & ~(F_LOCKED | F_WAITERS), std::memory_order_release, std::memory_order_relaxed)) {}
            if (w & F_WAITERS) word.notify_all();
        }

        // Snapshot ("held or someone queued"): for diagnostics, never for deciding whether to lock.
        [[nodiscard]] bool locking() const noexcept {
            const word_t w = word.load(std::memory_order_acquire);
            return (w & F_LOCKED) || ticket_of(w) != serving_of(w);
        }

        [[nodiscard]] bool is_interrupted() const noexcept {
            return (word.load(std::memory_order_acquire) & F_INTERRUPTED) != 0;
        }

        // Cancels every queued waiter at once. Idempotent while interrupted.
        void interrupt() noexcept {
            word_t w = word.load(std::memory_order_relaxed);
            for (;;) {
                if (w & F_INTERRUPTED) return;
                // someone inside keeps the lock and becomes ticket 0 (so its unlock() lands on serving == ticket);
                // otherwise the line is simply empty
                word_t nw = with_ticket(with_serving(next_gen(w), 0), (w & F_LOCKED) ? 1 : 0);
                nw = (nw | F_INTERRUPTED) & ~F_WAITERS;
                if (word.compare_exchange_weak(w, nw, std::memory_order_acq_rel, std::memory_order_relaxed)) break;
            }
            if (w & F_WAITERS) word.notify_all();
        }

        void reset_interrupt() noexcept { word.fetch_and(~F_INTERRUPTED, std::memory_order_release); }
    };

    using small_ordered_mutex = ordered_mutex<std::uint8_t>;
    // Same width, but safe with any number of threads (see capacity_policy::block).
    using checked_small_ordered_mutex = ordered_mutex<std::uint8_t, capacity_policy::block>;
}
#endif
