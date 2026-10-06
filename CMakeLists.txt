cmake_minimum_required(VERSION 3.20)
project(FCS-Workers VERSION 2.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Defaults ON only when FCS-Workers is the top-level project (not when pulled in
# via add_subdirectory/FetchContent), so a consumer doesn't inherit its strict flags.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(_fcs_top_level_default ON)
else()
    set(_fcs_top_level_default OFF)
endif()
option(FCS_WORKER_IS_TOP_LEVEL "Whether FCS-Workers itself were actively developed" ${_fcs_top_level_default})
option(FCS_APPLY_COMPILE_FLAGS "Whether to enable FCS-Workers preferred compile flags" ${FCS_WORKER_IS_TOP_LEVEL})

if(FCS_WORKER_IS_TOP_LEVEL)
	set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

find_package(Threads REQUIRED)

add_library(FCS-Workers INTERFACE)
target_include_directories(FCS-Workers INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(FCS-Workers INTERFACE Threads::Threads)

if(WIN32)
    target_compile_definitions(FCS-Workers INTERFACE FCS_WORKER_BACKEND_IOCP=1)
    target_link_libraries(FCS-Workers INTERFACE ws2_32)
else()
    option(FCS_USE_IO_URING "Whether to prefer io_uring instead of epoll" OFF)
    option(FCS_IO_URING_PROBE "Probe io_uring at configure time and fall back to epoll if it is unusable" ON)
    set(_fcs_use_io_uring ${FCS_USE_IO_URING})
    if(_fcs_use_io_uring AND FCS_IO_URING_PROBE)
        include(cmake/ProbeIoUring.cmake)
        fcs_probe_io_uring(_fcs_io_uring_ok)
        if(NOT _fcs_io_uring_ok)
            message(WARNING "FCS_USE_IO_URING=ON but the io_uring probe failed here; falling back to the epoll backend "
                            "(pass -DFCS_IO_URING_PROBE=OFF to force io_uring anyway)")
            set(_fcs_use_io_uring OFF)
        endif()
    endif()
    if(_fcs_use_io_uring)
        target_compile_definitions(FCS-Workers INTERFACE FCS_WORKER_BACKEND_IO_URING=1)
    else()
        target_compile_definitions(FCS-Workers INTERFACE FCS_WORKER_BACKEND_EPOLL=1)
    endif()
endif()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/cmake/CompileFlags.cmake")
    include(cmake/CompileFlags.cmake)
endif()

if(FCS_APPLY_COMPILE_FLAGS AND COMMAND SERVER_OPTIMIZE)
    SERVER_OPTIMIZE(FCS-Workers INTERFACE)
endif()

# Standalone io_uring probe (exit codes documented in the source). Also handy on a
# deploy host: `fcs_io_uring_probe human`.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(fcs_io_uring_probe cmake/probes/io_uring_probe.cpp)
    if(FCS_APPLY_COMPILE_FLAGS AND COMMAND SERVER_OPTIMIZE)
        SERVER_OPTIMIZE(fcs_io_uring_probe PRIVATE)
    endif()
endif()

add_executable(fcs_workers_example examples/example.cpp)
target_link_libraries(fcs_workers_example PRIVATE FCS-Workers)

add_executable(fcs_workers_stressor benchmarks/pool_stressor.cpp)
target_link_libraries(fcs_workers_stressor PRIVATE FCS-Workers)
if(WIN32)
    target_link_libraries(fcs_workers_stressor PRIVATE winmm) # timeBeginPeriod
endif()

# Lock-free park/unpark primitives that replaced the pool's mutex + condition_variable.
add_executable(fcs_workers_parking_lot_test tests/parking_lot_test.cpp)
target_link_libraries(fcs_workers_parking_lot_test PRIVATE FCS-Workers)

# Normal-priority admission reserve: a high-priority flood must not lock normal out at the door.
add_executable(fcs_workers_priority_admission_test tests/priority_admission_test.cpp)
target_link_libraries(fcs_workers_priority_admission_test PRIVATE FCS-Workers)

# Cooperative coroutines: watchdog-driven yield, reverse aging, thread-safe external awaits.
add_executable(fcs_workers_coop_yield_test tests/coop_yield_test.cpp)
target_link_libraries(fcs_workers_coop_yield_test PRIVATE FCS-Workers)

# Pool traits: custom semaphore type, unbounded admission gate, timer capacity, and their concepts.
add_executable(fcs_workers_pool_traits_test tests/pool_traits_test.cpp)
target_link_libraries(fcs_workers_pool_traits_test PRIVATE FCS-Workers)

# Bounded O(log N) timer min-heap (replaced an unbounded mutex-guarded one).
add_executable(fcs_workers_timer_heap_test tests/timer_heap_test.cpp)
target_link_libraries(fcs_workers_timer_heap_test PRIVATE FCS-Workers)

# Inline (non-allocating, move-only) callable that replaced std::function.
add_executable(fcs_workers_inline_function_test tests/inline_function_test.cpp)
target_link_libraries(fcs_workers_inline_function_test PRIVATE FCS-Workers)

# Stall watchdog: deterministic registry-level checks + an end-to-end rescue.
add_executable(fcs_workers_stall_rescue tests/stall_rescue.cpp)
target_link_libraries(fcs_workers_stall_rescue PRIVATE FCS-Workers)

# Unit test for the shared steal-estimate cache (header-only, no backend).
add_executable(fcs_workers_idle_estimate_test tests/idle_estimate_test.cpp)
target_link_libraries(fcs_workers_idle_estimate_test PRIVATE FCS-Workers)

# Backend contract test: identical assertions on epoll / io_uring / IOCP (see the file header).
add_executable(fcs_workers_backend_contract tests/backend_contract.cpp)
target_link_libraries(fcs_workers_backend_contract PRIVATE FCS-Workers)

# Raw Winsock/IOCP ceiling (no pool) -- Windows only; see the file header.
if(WIN32)
    add_executable(fcs_workers_iocp_ceiling benchmarks/iocp_ceiling.cpp)
    target_link_libraries(fcs_workers_iocp_ceiling PRIVATE ws2_32 mswsock)
endif()

# Wakeup latency / submit throughput benchmark (A/B across library versions).
add_executable(fcs_workers_wake_bench benchmarks/wake_bench.cpp)
target_link_libraries(fcs_workers_wake_bench PRIVATE FCS-Workers)

# steal() work-redistribution benchmark (idle-estimate cache A/B via the ttl argument).
add_executable(fcs_workers_steal_bench benchmarks/steal_bench.cpp)
target_link_libraries(fcs_workers_steal_bench PRIVATE FCS-Workers)

# Backend A/B drain benchmark (TCP loopback on every OS). Build the same source against each backend.
add_executable(fcs_workers_backend_bench benchmarks/backend_bench.cpp)
target_link_libraries(fcs_workers_backend_bench PRIVATE FCS-Workers)

# Regression test for epoll registration/handler lifetime under concurrent dispatch.
# Deterministic (ASan: heap-use-after-free on the pre-fix backend, 5/5 runs). POSIX-only: uses socketpair().
if(NOT WIN32)
    add_executable(fcs_workers_dispatch_race tests/dispatch_race.cpp)
    target_link_libraries(fcs_workers_dispatch_race PRIVATE FCS-Workers)
endif()
