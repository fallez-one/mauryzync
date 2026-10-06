# Configure-time io_uring availability probe.
#
#   fcs_probe_io_uring(<out_var>)
#
# Compiles and runs cmake/probes/io_uring_probe.cpp on the *build* machine and
# sets <out_var> to ON only if io_uring_setup() actually succeeded. Exit-code
# contract (see the probe): 0 ok, 72 no kernel support, 77 denied by security
# policy, 1 unexpected, 255 not Linux.
#
# Not probed (assumed available) when cross-compiling, since the result of
# running the probe here says nothing about the target. The probe says what the
# *configure* host allows; the deploy host can still differ -- run the
# fcs_io_uring_probe target there (`fcs_io_uring_probe human`) to check.
include_guard(GLOBAL)

function(fcs_probe_io_uring out_var)
    set(${out_var} OFF PARENT_SCOPE)

    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        message(STATUS "io_uring probe: skipped (target is not Linux)")
        return()
    endif()

    if(CMAKE_CROSSCOMPILING)
        message(STATUS "io_uring probe: cross-compiling, cannot run it here -- assuming io_uring is available on the target")
        set(${out_var} ON PARENT_SCOPE)
        return()
    endif()

    # try_run() caches its result variables; drop them so a changed sysctl /
    # seccomp profile / kernel is picked up on the next configure.
    unset(FCS_IO_URING_PROBE_RUN CACHE)
    unset(FCS_IO_URING_PROBE_COMPILE CACHE)

    try_run(FCS_IO_URING_PROBE_RUN FCS_IO_URING_PROBE_COMPILE
        ${CMAKE_BINARY_DIR}/CMakeFiles/fcs_io_uring_probe
        ${CMAKE_CURRENT_FUNCTION_LIST_DIR}/probes/io_uring_probe.cpp
        COMPILE_OUTPUT_VARIABLE _fcs_probe_compile_log
        RUN_OUTPUT_VARIABLE _fcs_probe_run_log
        ARGS human)

    if(NOT FCS_IO_URING_PROBE_COMPILE)
        message(STATUS "io_uring probe: failed to compile -- treating io_uring as unavailable")
        message(VERBOSE "${_fcs_probe_compile_log}")
        return()
    endif()

    string(STRIP "${_fcs_probe_run_log}" _fcs_probe_msg)
    if(FCS_IO_URING_PROBE_RUN STREQUAL "0")
        message(STATUS "io_uring probe: ok")
        set(${out_var} ON PARENT_SCOPE)
    elseif(FCS_IO_URING_PROBE_RUN STREQUAL "72")
        message(STATUS "io_uring probe: unavailable (${_fcs_probe_msg})")
    elseif(FCS_IO_URING_PROBE_RUN STREQUAL "77")
        message(STATUS "io_uring probe: denied (${_fcs_probe_msg})")
    else()
        message(STATUS "io_uring probe: exit ${FCS_IO_URING_PROBE_RUN} (${_fcs_probe_msg})")
    endif()
endfunction()
