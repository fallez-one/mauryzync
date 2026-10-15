#pragma once

// One place that knows whether this build has C++ exceptions, so the rest of the library can be
// built with -fno-exceptions (GCC/Clang) or /EHs-c- + _HAS_EXCEPTIONS=0 (MSVC).
//
//   FCS_HAS_EXCEPTIONS   1 when exceptions are available, 0 when not. Detected; or forced from the
//                        command line (-DFCS_HAS_EXCEPTIONS=0 keeps the library on its no-throw
//                        paths even in a build that does have exceptions).
//   FCS_TRY / FCS_CATCH_ALL
//                        `FCS_TRY { a } FCS_CATCH_ALL { b }` is try/catch(...) when exceptions are
//                        available. When they are not it is `{ a }` with `{ b }` compiled but
//                        never reached -- so a boundary that turns a throwing callback into a
//                        value still compiles, and simply has nothing to catch.
//
// The library itself never throws. Where it used to (FCS::async_result::result()), it now returns
// an FCS::Expected whose error carries a function pointer; whether that pointer is ever called --
// and so whether anything is thrown -- is the caller's decision. See coroutine.hpp.

#include <exception>   // defines _HAS_EXCEPTIONS on MSVC's STL

#if !defined(FCS_HAS_EXCEPTIONS)
#  if defined(__cpp_exceptions) || defined(__EXCEPTIONS)
#    define FCS_HAS_EXCEPTIONS 1
#  elif defined(_CPPUNWIND) && !(defined(_HAS_EXCEPTIONS) && _HAS_EXCEPTIONS == 0)
#    define FCS_HAS_EXCEPTIONS 1
#  else
#    define FCS_HAS_EXCEPTIONS 0
#  endif
#endif

#if FCS_HAS_EXCEPTIONS
#  define FCS_TRY try
#  define FCS_CATCH_ALL catch (...)
#else
#  define FCS_TRY if constexpr (true)
#  define FCS_CATCH_ALL else
#endif
