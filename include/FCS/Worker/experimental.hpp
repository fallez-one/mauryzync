#pragma once

// FCS_EXPERIMENTAL_ALWAYS_ON -- opt-in gate for the "always on" total-stall resurrection path:
//   pool_service::watchdog_total_stall_poll()  /  watchdog_finalize_wait()  /  watchdog_on_migrate()
//
// OFF (the default): those three calls still exist, so code that configures them compiles
// unchanged, but each is a no-op, and no hot path (worker loop, submit) pays anything for it.
// ON: the watchdog may escalate a total stall to a runtime shed (see pool_service_shed.inl).
//
// Enable with -DFCS_EXPERIMENTAL_ALWAYS_ON=1 (or the CMake option of the same name).
#if !defined(FCS_EXPERIMENTAL_ALWAYS_ON)
#  define FCS_EXPERIMENTAL_ALWAYS_ON 0
#endif
