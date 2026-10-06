#pragma once
#include "../eventloop.hpp"
namespace FCS::Worker::backend::kqueue {
    class backend final : public generic_eventlooper<backend, int> {
    public:
        using generic_eventlooper<backend, int>::generic_eventlooper;
    };
    using eventlooper = backend;
}
