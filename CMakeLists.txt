cmake_minimum_required(VERSION 3.10)

set(SIMPLEMR_VERSION 0.0.1)

project(SimpleMapReduce VERSION ${SIMPLEMR_VERSION} LANGUAGES CXX)
message(STATUS "Build SimpleMapReduce: v${SIMPLEMR_VERSION}")

if(UNIX AND NOT APPLE)
  set(USED_OS "Linux")
elseif(APPLE)
  set(USED_OS "MacOS")
elseif(UNIX)
  set(USED_OS "Unix")
elseif(WIN32)
  set(USED_OS "Win32")
endif()

if(NOT USED_OS STREQUAL "Linux")
  message(SEND_ERROR "Currently only work on Linux")
endif()

set(BUILD_LIBRARY ON)

# library name
set(libname simplemapreduce)

# MPI setup
find_package(MPI REQUIRED)
if(MPI_FOUND)
  include_directories(SYSTEM ${MPI_INCLUDE_PATH})
else (MPI_FOUND)
  message(SEND_ERROR "MPI not found")
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Set build type" FORCE)
endif()

# for OPEN-MPI
# ref: https://github.com/open-mpi/ompi/issues/5157
add_definitions(-DOMPI_SKIP_MPICXX)

# to prevent std::string from converting to std::__cxx11::basic_string
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

if((NOT CMAKE_CXX_STANDARD) OR (CMAKE_CXX_STANDARD LESS 17))
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

set(CMAKE_CXX_EXTENSIONS OFF)

set(MPI_CXX_COMPILER mpic++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# ------------------------------------------------------------
#   Options
# ------------------------------------------------------------
option(SIMPLEMR_BUILD_TEST "Build tests" OFF)
option(SIMPLEMR_BUILD_APP "Build executable (./app)" OFF)

# ------------------------------------------------------------
#   Shared Library
# ------------------------------------------------------------
# grouping main task and package source files
add_library(${libname} SHARED "")
include(${CMAKE_SOURCE_DIR}/cmake/sourcelist.cmake)
target_include_directories(${libname}
  PUBLIC 
    ${CMAKE_SOURCE_DIR}/include
)

# check tbb availability
find_package(TBB QUIET)
if(TBB_FOUND)
  message(STATUS "Build with TBB")
  target_compile_definitions(${libname} PRIVATE HAS_TBB)
  target_link_libraries(${libname} PRIVATE tbb)
endif()

target_compile_options(${libname}
  PUBLIC
    $<$<CONFIG:Release>:-O3>
    $<$<CONFIG:Debug>:-O0 -g -Wall -Wextra>
    $<$<CONFIG:RelWithDebInfo>:-O3 -g>
)
target_compile_definitions(${libname}
  PUBLIC
    $<$<NOT:$<CONFIG:Debug>>:NDEBUG>
)

# ------------------------------------------------------------
#   Testing
# ------------------------------------------------------------
message(STATUS "Build test: ${SIMPLEMR_BUILD_TEST}")
if(SIMPLEMR_BUILD_TEST)
  enable_testing()
  add_subdirectory(tests)
endif()

# ------------------------------------------------------------
#   App Executable
# ------------------------------------------------------------
message(STATUS "Build app: ${SIMPLEMR_BUILD_APP}")
if(SIMPLEMR_BUILD_APP)
  add_subdirectory(app)
endif()