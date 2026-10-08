#pragma once
#include "../eventloop.hpp"
namespace FCS::Worker::backend::kqueue {
    template<typename Pool = pool_service<>>
    class backend final : public generic_eventlooper<backend<Pool>, int, Pool> {
    public:
        using generic_eventlooper<backend, int>::generic_eventlooper;
    };
    using eventlooper = backend<pool_service<>>;
    template<typename Pool> using basic_eventlooper = backend<Pool>;
}
