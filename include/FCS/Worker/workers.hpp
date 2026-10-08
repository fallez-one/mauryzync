#pragma once

#include "config.hpp"
#include "eventloop.hpp"
#include "pool_service.hpp"

#if FCS_WORKER_BACKEND_EPOLL
#  include "backends/epoll.hpp"
namespace FCS::Worker {
    using eventlooper = backend::epoll::eventlooper;
    // The same backend for a pool_service with non-default traits (task size, semaphore, ...).
    template<typename Pool> using basic_eventlooper = backend::epoll::basic_eventlooper<Pool>;
}
#elif FCS_WORKER_BACKEND_IOCP
#  include "backends/iocp.hpp"
namespace FCS::Worker {
    using eventlooper = backend::iocp::eventlooper;
    // The same backend for a pool_service with non-default traits (task size, semaphore, ...).
    template<typename Pool> using basic_eventlooper = backend::iocp::basic_eventlooper<Pool>;
}
#elif FCS_WORKER_BACKEND_KQUEUE
#  include "backends/kqueue.hpp"
namespace FCS::Worker {
    using eventlooper = backend::kqueue::eventlooper;
    // The same backend for a pool_service with non-default traits (task size, semaphore, ...).
    template<typename Pool> using basic_eventlooper = backend::kqueue::basic_eventlooper<Pool>;
}
#elif FCS_WORKER_BACKEND_IO_URING
#  include "backends/io_uring.hpp"
namespace FCS::Worker {
    using eventlooper = backend::io_uring::eventlooper;
    // The same backend for a pool_service with non-default traits (task size, semaphore, ...).
    template<typename Pool> using basic_eventlooper = backend::io_uring::basic_eventlooper<Pool>;
}
#endif
