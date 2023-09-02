set(DYNAMIC_PROJECT False)

cmake_minimum_required(VERSION 3.27)
project(ccms)

if (APPLE)
    if(DYNAMIC_PROJECT)
        set(VCPKG_TARGET_TRIPLET x64-osx-dynamic)
    else()
        set(VCPKG_TARGET_TRIPLET x64-osx)
    endif()
elseif (LINUX)
    if(DYNAMIC_PROJECT)
        set(VCPKG_TARGET_TRIPLET x64-linux-dynamic)
    else()
        set(VCPKG_TARGET_TRIPLET x64-linux)
    endif()
endif()

include($ENV{HOME}/apps/vcpkg/scripts/buildsystems/vcpkg.cmake)

find_package(PostgreSQL REQUIRED)
find_package(Arrow REQUIRED)

include_directories (${PostgreSQL_INCLUDE_DIRS})
link_directories(${PostgreSQL_LIBRARY_DIRS})

# dbg-macro
include(FetchContent)

FetchContent_Declare(dbg_macro GIT_REPOSITORY https://github.com/sharkdp/dbg-macro)
FetchContent_MakeAvailable(dbg_macro)

get_filename_component(ARROW_CONFIG_PATH ${Arrow_CONFIG} DIRECTORY)

set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall -Wextra")
set(CMAKE_CXX_STANDARD "20")
set(CMAKE_EXPORT_COMPILE_COMMANDS "1")

add_executable(ccms
    include/ccms.h ccms.cpp
    include/main.h main.cpp
    include/data_type_conversion.h data_type_conversion.cpp 
    include/arrow_backend.h arrow_backend.cpp
    include/arrow_schema_info.h arrow_schema_info.cpp
    include/postgresql_backend.h postgresql_backend.cpp
    include/runmain_pq.h runmain_pq.cpp
    include/generic_tools.h generic_tools.cpp
    include/schema_info.h schema_info.cpp
    include/timer.h timer.cpp)

#target_link_libraries(ccms PRIVATE Arrow::arrow_shared SOCI::soci_core SOCI::soci_empty SOCI::soci_postgresql  PostgreSQL::PostgreSQL)

if (DYNAMIC_PROJECT)
    target_link_libraries(ccms PRIVATE Arrow::arrow_shared PostgreSQL::PostgreSQL dbg_macro)
else()
    target_link_libraries(ccms PRIVATE Arrow::arrow_static PostgreSQL::PostgreSQL dbg_macro)
endif()