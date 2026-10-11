#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <span>
#include <type_traits>
#include <utility>

namespace FCS::Worker::detail {

    // The tasks a runtime shed pulls out of a wedged pool, laid out as a fixed two-dimensional
    // array: ROWS x row_capacity() slots of Task, one row per source.
    //
    //   row 0 .. workers-1    each worker's local deque   (oldest task first)
    //   row workers + 0       fast lane   (priority_mpmc_queue: high before normal)
    //   row workers + 1       slow lane
    //   row workers + 2       the admission gate's refill staging (pending_)
    //   row workers + 3       the admission gate's cushion         (newest tasks)
    //
    // Within a row the order is the order the source would have handed the tasks out in.
    //
    // Bounded and allocated up front (when the pool starts, not when it is already wedged): the
    // slots are raw storage, so pages nobody ever writes are never committed -- and are never
    // copied by the copy-on-write clone either. A task is constructed in its slot only when a
    // row receives it.
    //
    // Not thread-safe: it is only ever touched by the single thread running the shed (the
    // watchdog in the parent, the cloned thread in the child).
    template<typename Task>
    class migration_batch {
    public:
        static constexpr std::size_t shared_rows = 4;

        migration_batch() = default;
        migration_batch(const migration_batch&) = delete;
        migration_batch& operator=(const migration_batch&) = delete;
        ~migration_batch() { if (!abandoned_) clear(); }

        void configure(std::size_t workers, std::size_t row_capacity) {
            clear();
            workers_ = workers;
            row_capacity_ = row_capacity;
            const std::size_t rows = workers + shared_rows;
            counts_ = std::make_unique<std::size_t[]>(rows);   // value-initialised: all rows empty
            slots_.reset(static_cast<Task*>(::operator new(rows * row_capacity * sizeof(Task), std::align_val_t{alignof(Task)})));
        }

        [[nodiscard]] bool configured() const noexcept { return slots_ != nullptr; }
        [[nodiscard]] std::size_t workers() const noexcept { return workers_; }
        [[nodiscard]] std::size_t rows() const noexcept { return slots_ ? workers_ + shared_rows : 0; }
        [[nodiscard]] std::size_t row_capacity() const noexcept { return row_capacity_; }

        [[nodiscard]] std::size_t local_row(std::size_t worker) const noexcept { return worker; }
        [[nodiscard]] std::size_t fast_row() const noexcept { return workers_; }
        [[nodiscard]] std::size_t slow_row() const noexcept { return workers_ + 1; }
        [[nodiscard]] std::size_t pending_row() const noexcept { return workers_ + 2; }
        [[nodiscard]] std::size_t cushion_row() const noexcept { return workers_ + 3; }

        [[nodiscard]] std::size_t size(std::size_t row) const noexcept { return counts_[row]; }
        [[nodiscard]] std::size_t total() const noexcept {
            std::size_t n = 0;
            for (std::size_t r = 0; r < rows(); ++r) n += counts_[r];
            return n;
        }
        [[nodiscard]] bool room(std::size_t row) const noexcept { return counts_[row] < row_capacity_; }

        // Appends to `row`. On a full row returns false and leaves `item` untouched -- the caller
        // still owns it.
        [[nodiscard]] bool put(std::size_t row, Task&& item) noexcept(std::is_nothrow_move_constructible_v<Task>) {
            if (!room(row)) return false;
            ::new (static_cast<void*>(slot(row, counts_[row]))) Task(std::move(item));
            ++counts_[row];
            return true;
        }

        // The tasks currently in `row`, in order. Edit freely: reorder them, run one, reset() one
        // to drop it (an empty task is skipped on injection).
        [[nodiscard]] std::span<Task> row(std::size_t r) noexcept { return {slot(r, 0), counts_[r]}; }
        [[nodiscard]] std::span<const Task> row(std::size_t r) const noexcept { return {slot(r, 0), counts_[r]}; }

        // Destroys what is left in `row` (tasks that were moved out are empty shells by now).
        void clear_row(std::size_t r) noexcept {
            for (std::size_t i = 0; i < counts_[r]; ++i) std::destroy_at(slot(r, i));
            counts_[r] = 0;
        }
        void clear() noexcept {
            for (std::size_t r = 0; r < rows(); ++r) clear_row(r);
        }

        // Forget every task WITHOUT running its destructor, and free only the storage. For the
        // parent of a successful clone: those tasks now live in the child, and a closure's
        // destructor could just as well wait on something a wedged worker still holds.
        void abandon() noexcept {
            for (std::size_t r = 0; r < rows(); ++r) counts_[r] = 0;
            abandoned_ = true;
        }

    private:
        [[nodiscard]] Task* slot(std::size_t row, std::size_t index) const noexcept { return slots_.get() + row * row_capacity_ + index; }

        struct raw_free {
            void operator()(Task* p) const noexcept { ::operator delete(p, std::align_val_t{alignof(Task)}); }
        };
        std::unique_ptr<Task, raw_free> slots_;
        std::unique_ptr<std::size_t[]> counts_;
        std::size_t workers_{0};
        std::size_t row_capacity_{0};
        bool abandoned_{false};
    };

    // What a shed found and did. The first block is filled in by the parent before the clone and
    // reaches the child through the copy; the second is filled in by the child.
    struct migration_report {
        // evacuated (parent side)
        std::size_t evacuated_local{};     // across all workers' deques
        std::size_t evacuated_fast{};
        std::size_t evacuated_slow{};
        std::size_t evacuated_pending{};
        std::size_t evacuated_cushion{};
        bool refill_staging_claimed_elsewhere{false}; // a wedged worker held it: its staged tasks could not be taken
        std::uint64_t residual_gauge{};    // what the depth gauges still counted after the sweep: tasks that could not be taken (advisory)

        // re-injected (child side)
        std::size_t injected_direct{};     // straight into the worker's own deque, before it started
        std::size_t injected_cushion{};    // through the admission cushion
        std::size_t injected_mpmc{};       // cushion refused: straight into the fast/slow lane
        std::size_t dropped_empty{};       // reset() by the hook
        std::size_t refused{};             // nowhere left to put them: still in the batch, destroyed with it

        [[nodiscard]] std::size_t evacuated() const noexcept {
            return evacuated_local + evacuated_fast + evacuated_slow + evacuated_pending + evacuated_cushion;
        }
    };

    // Handed to the user's migration hook, which runs in the CLONE, on its only thread, after
    // the pool's runtime state has been rebuilt and before any worker exists. So it can touch
    // anything without a race -- repair state the wedged workers were holding (unlock the mutex
    // one of them died inside), restart a backend's poller thread, edit the batch. It must not
    // wait for anything another thread of the parent would have had to provide.
    template<typename Task>
    struct migration_context {
        migration_batch<Task>& batch;
        migration_report& report;
    };

}
