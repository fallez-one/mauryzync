#pragma once

#include "config.hpp"
#include "eventloop.hpp"
#include "pool_service.hpp"

#if FCS_WORKER_BACKEND_EPOLL
#  include "backends/epoll.hpp"
namespace FCS::Worker { using eventlooper = backend::epoll::eventlooper; }
#elif FCS_WORKER_BACKEND_IOCP
#  include "backends/iocp.hpp"
namespace FCS::Worker { using eventlooper = backend::iocp::eventlooper; }
#elif FCS_WORKER_BACKEND_KQUEUE
#  include "backends/kqueue.hpp"
namespace FCS::Worker { using eventlooper = backend::kqueue::eventlooper; }
#elif FCS_WORKER_BACKEND_IO_URING
#  include "backends/io_uring.hpp"
namespace FCS::Worker { using eventlooper = backend::io_uring::eventlooper; }
#endif
