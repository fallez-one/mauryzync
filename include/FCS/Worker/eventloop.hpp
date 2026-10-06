#pragma once

#include "backends/detail/source_types.hpp"
#include "pool_service.hpp"
#include "types.hpp"

#include <cstddef>
#include <utility>

namespace FCS::Worker {

    template<typename Derived>
    class eventloop_crtp {
    public:
        void start() { derived().start_backend(); }
        void stop() noexcept { derived().stop_backend(); }

        template<typename Tag, typename Callback>
        [[nodiscard]] subscription subscribe(Callback&& callback) {
            return derived().service().template subscribe<Tag>(std::forward<Callback>(callback));
        }

        // `Options`: recv_option flags for a readiness-based backend (epoll,
        // eventually kqueue) -- see backends/detail/source_types.hpp. A
        // backend that ignores them (or isn't readiness-based) just never
        // looks at the parameter, so it costs nothing to always accept it.
        template<typename Tag, std::size_t Size, backend::detail::recv_option Options = backend::detail::recv_option::none,
                 typename Reader, typename Source, typename Callback>
        [[nodiscard]] subscription subscribe(Source&& source, Reader&& reader, Callback&& callback) {
            return derived().template register_source<Tag, Size, Options>(
                std::forward<Source>(source), std::forward<Reader>(reader), std::forward<Callback>(callback));
        }

        // Write-side counterpart of subscribe(). For full duplex, pass the
        // same native handle to both; a backend that supports it (see
        // epoll::backend::register_sink()) shares one registration.
        template<typename Tag, std::size_t Size, typename Writer, typename Sink, typename Callback>
        [[nodiscard]] subscription subscribe_write(Sink&& sink, Writer&& writer, Callback&& callback) {
            return derived().template register_sink<Tag, Size>(
                std::forward<Sink>(sink), std::forward<Writer>(writer), std::forward<Callback>(callback));
        }

    private:
        Derived& derived() noexcept { return static_cast<Derived&>(*this); }
    };

    // Backends that have no real multiplexer yet (io_uring, kqueue) inherit register_source
    // as-is: it wires the callback into the Tag channel without touching the native handle.
    // register_sink() is the same story for the write side, and stays this way for any
    // backend -- including epoll and, eventually, io_uring/IOCP -- until it defines its own.
    template<typename Derived, typename Native>
    class generic_eventlooper : public eventloop_crtp<Derived> {
    public:
        explicit generic_eventlooper(pool_service<>& service) noexcept : service_(&service) {}

        [[nodiscard]] pool_service<>& service() noexcept { return *service_; }
        void start_backend() { service_->start(); }
        void stop_backend() noexcept { service_->stop(); }

        template<typename Tag, std::size_t Size, backend::detail::recv_option Options = backend::detail::recv_option::none,
                 typename Reader, typename Source, typename Callback>
        [[nodiscard]] subscription register_source(Source&&, Reader&&, Callback&& callback) {
            return service_->template subscribe<Tag>(std::forward<Callback>(callback));
        }

        template<typename Tag, std::size_t Size, typename Writer, typename Sink, typename Callback>
        [[nodiscard]] subscription register_sink(Sink&&, Writer&&, Callback&& callback) {
            return service_->template subscribe<Tag>(std::forward<Callback>(callback));
        }

        template<typename Tag, typename... Args>
        [[nodiscard]] std::size_t post(Args&&... args) { return service_->template post<Tag>(std::forward<Args>(args)...); }

        template<typename Tag, typename... Args>
        [[nodiscard]] std::size_t deliver(Args&&... args) { return post<Tag>(std::forward<Args>(args)...); }

    private:
        pool_service<>* service_;
    };

}
