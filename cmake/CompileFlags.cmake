function(SERVER_OPTIMIZE program_target scope)
    if(MSVC)
        target_compile_options(${program_target} ${scope}
            /permissive-
            # /GR-

            /utf-8
            /W4

            /Zc:preprocessor
            /Zc:__cplusplus
            /MP

            /EHs-c-
            "$<$<CONFIG:Debug>:/fsanitize=address>"
            "$<$<CONFIG:Debug>:/Od>"
            "$<$<CONFIG:Debug>:/sdl>"

            "$<$<CONFIG:Release>:/O2>"
            "$<$<CONFIG:Release>:/Ob3>"
            "$<$<CONFIG:Release>:/Oi>"
            "$<$<CONFIG:Release>:/GL>"
            "$<$<CONFIG:Release>:/Gy>"
            "$<$<CONFIG:Release>:/Gw>"
            "$<$<CONFIG:Release>:/guard:cf>"

            # Choose ONE:
            # "$<$<CONFIG:Release>:/fp:fast>"
            "$<$<CONFIG:Release>:/fp:precise>"
        )
        target_compile_definitions(${program_target} ${scope} _HAS_EXCEPTIONS=0)
        if(CMAKE_GENERATOR MATCHES "Ninja")
            target_compile_options(${program_target} ${scope} "$<$<CONFIG:Debug>:/Z7>")

            # Remove /Zi or /Zi-like flags if they are being added by default
            string(REPLACE "/Zi" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
        else()
            target_compile_options(${program_target} ${scope} "$<$<CONFIG:Debug>:/Zi>")
        endif()

        # Suppress noisy padding alignment specifier warning
        target_compile_options(${program_target} ${scope} "/wd4324")

        target_link_options(${program_target} ${scope}
            /DEBUG
            /INCREMENTAL:NO
            "$<$<CONFIG:Debug>:/INFERASANLIBS>"
            "$<$<CONFIG:Release>:/LTCG>"
            "$<$<CONFIG:Release>:/OPT:REF>"
            "$<$<CONFIG:Release>:/OPT:ICF>"
            "$<$<CONFIG:Release>:/guard:cf>"
        )
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU" AND NOT MSVC)
        option(ENABLE_LTO "Enable Link Time Optimization" ON)

        # 1. COMMON (GLOBAL STRICTNESS)
        target_compile_options(${program_target} ${scope}
            -Wall -Wextra -Wpedantic

            -Wshadow -Wconversion -Wsign-conversion
            -Wold-style-cast -Wnull-dereference
            -Wdouble-promotion -Wformat=2
            -Woverloaded-virtual -Wcast-align -Wunused

            -pedantic

            # -fno-exceptions
            # -fno-rtti
            -fvisibility=hidden
            -fdiagnostics-color=always
        )
        if(NOT WIN32)
            target_compile_definitions(${program_target} ${scope} _REENTRANT)
        endif()

        if(WIN32)
            target_compile_definitions(${program_target} ${scope} $<$<CONFIG:Debug>:_DEBUG>)
        endif()

        # 2. DEBUG
        target_compile_options(${program_target} ${scope} "$<$<CONFIG:Debug>:-Og;-g3;-fno-omit-frame-pointer;-fno-optimize-sibling-calls>")

        target_compile_options(${program_target} ${scope} "$<$<CONFIG:Debug>:-fsanitize=address,undefined>")

        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            target_compile_options(${program_target} ${scope} -fno-limit-debug-info)
        endif()

        # if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        #     if(WIN32 AND NOT MSVC)
        #         target_compile_definitions(${program_target} PRIVATE _GLIBCXX_NO_EXCEPTIONS)
            
        #     elseif(APPLE)
        #         target_compile_definitions(${program_target} PRIVATE _LIBCPP_HAS_NO_EXCEPTIONS)
            
        #     else()
        #         target_compile_definitions(${program_target} PRIVATE _GLIBCXX_NO_EXCEPTIONS _LIBCPP_HAS_NO_EXCEPTIONS)
        #     endif()
        
        # elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU")

        #     target_compile_definitions(${program_target} PRIVATE _GLIBCXX_NO_EXCEPTIONS)
        # endif()

        # 3. RELEASE
        target_compile_options(${program_target} ${scope} "$<$<CONFIG:Release>:-O3;-DNDEBUG;-ffunction-sections;-fdata-sections;-fomit-frame-pointer;-fstack-protector-strong;-fstack-clash-protection;-D_FORTIFY_SOURCE=2>")

        target_link_options(${program_target} ${scope}
            "$<$<CONFIG:Release>:-s>"
            "$<$<CONFIG:Debug>:-fsanitize=address,undefined>"
        )

        # Only evaluate if we are building in Release or MinSizeRel
        set(IS_RELEASE_CONFIG $<OR:$<CONFIG:Release>,$<CONFIG:MinSizeRel>>)

        # Compiler flags (Enable section splitting for GC sections on GCC/Clang/MinGW)
        if(NOT MSVC)
            target_compile_options(${program_target} ${scope} $<$<BOOL:${IS_RELEASE_CONFIG}>:-ffunction-sections -fdata-sections>)
        endif()

        target_link_options(${program_target} ${scope}
            $<$<AND:$<CXX_COMPILER_ID:MSVC>,${IS_RELEASE_CONFIG}>:/OPT:REF>
            $<$<AND:$<OR:$<CXX_COMPILER_ID:GNU>,$<CXX_COMPILER_ID:Clang>>,${IS_RELEASE_CONFIG},$<NOT:$<PLATFORM_ID:Darwin>>>:-Wl,--gc-sections>            
            $<$<AND:$<OR:$<CXX_COMPILER_ID:AppleClang>,$<CXX_COMPILER_ID:Clang>>,${IS_RELEASE_CONFIG},$<PLATFORM_ID:Darwin>>:-Wl,-dead_strip>
        )



        # Floating-point model (default fast)
        # add_compile_options(-ffast-math)

        # 4. LTO (Full default, Thin optional)
        if(ENABLE_LTO)
            include(CheckIPOSupported)
            check_ipo_supported(RESULT IPO_SUPPORTED)

            if(IPO_SUPPORTED)
                set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)

                # if(LTO_MODE STREQUAL "thin")
                #     add_compile_options($<$<CONFIG:Release>:-flto=thin>)
                # else()
                #     add_compile_options($<$<CONFIG:Release>:-flto>)
                # endif()
            endif()
        endif()

    # 5. HARDENING (Linux / ELF only)
        if(UNIX AND NOT APPLE)

            # RELRO + NOW (linker hardening)
            target_link_options(${program_target} ${scope} 
                -Wl,-z,relro
                -Wl,-z,now
            )

            # CET (Control Flow Enforcement Technology)
            include(CheckCXXCompilerFlag)
            check_cxx_compiler_flag("-fcf-protection=full" HAS_CET)

            if(HAS_CET)
                target_compile_options(${program_target} ${scope} -fcf-protection=full)
            endif()

        endif()
        set(CMAKE_POSITION_INDEPENDENT_CODE ON)

    endif()
endfunction()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")

    # Fix ASan vs Debug CRT conflict on Windows (Clang-cl / Clang-win)
    if(WIN32)
        set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreadedDLL")
    endif()
endif()