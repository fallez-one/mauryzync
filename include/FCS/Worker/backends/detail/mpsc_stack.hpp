#pragma once

#include <atomic>

namespace FCS::Worker::backend::detail {

    // Lock-free multi-producer / single-consumer hand-off: any thread push()es
    // an intrusive node, the one consumer take_all()s the whole batch in FIFO
    // order with a single exchange. It's a Treiber stack that is only ever
    // emptied wholesale, which is exactly why it has no ABA problem: nobody
    // pops an individual node, so a head pointer can't be recycled under a
    // pending CAS.
    //
    // Used by the completion backends so that *only the driver thread* ever
    // touches the kernel submission structure: registration and cancellation
    // from user threads become commands pushed here, and the driver turns them
    // into submissions. That removes the submit mutex entirely (and keeps
    // every submission on one long-lived thread, which io_uring wants -- see
    // detail/io_uring_ring.hpp).
    //
    // Node must expose `Node* next`.
    template<typename Node>
    class mpsc_stack {
    public:
        // Any thread. seq_cst so it pairs with the consumer's seq_cst
        // `sleeping` announcement (see the backends' wake protocol): either the
        // producer sees the consumer is about to sleep and wakes it, or the
        // consumer sees this node before it sleeps.
        void push(Node* node) noexcept {
            Node* head = head_.load(std::memory_order_relaxed);
            do {
                node->next = head;
            } while (!head_.compare_exchange_weak(head, node, std::memory_order_seq_cst, std::memory_order_relaxed));
        }

        // Consumer only. Returns the pending nodes oldest-first (nullptr if none).
        [[nodiscard]] Node* take_all() noexcept {
            Node* node = head_.exchange(nullptr, std::memory_order_acq_rel);
            Node* reversed = nullptr;
            while (node) {
                Node* next = node->next;
                node->next = reversed;
                reversed = node;
                node = next;
            }
            return reversed;
        }

        [[nodiscard]] bool empty() const noexcept { return head_.load(std::memory_order_seq_cst) == nullptr; }

    private:
        std::atomic<Node*> head_{nullptr};
    };

}
