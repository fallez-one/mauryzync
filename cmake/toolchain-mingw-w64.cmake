# Cross-compile for Windows (IOCP backend) from Linux:
#   cmake -S . -B build-win -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/toolchain-mingw-w64.cmake
# Run the result under WINE:  wine build-win/fcs_workers_stressor.exe
set(CMAKE_SYSTEM_NAME Windows)
set(CMAKE_SYSTEM_PROCESSOR x86_64)
set(_fcs_triple x86_64-w64-mingw32)
set(CMAKE_C_COMPILER /usr/bin/${_fcs_triple}-gcc-posix)
set(CMAKE_CXX_COMPILER /usr/bin/${_fcs_triple}-g++-posix)   # posix threading model: std::thread/std::mutex available
set(CMAKE_RC_COMPILER /usr/bin/${_fcs_triple}-windres)
set(CMAKE_FIND_ROOT_PATH /usr/${_fcs_triple})
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
# Link the MinGW runtime statically so the .exe runs under WINE without hunting for DLLs.
set(CMAKE_EXE_LINKER_FLAGS_INIT "-static -static-libgcc -static-libstdc++")
