cmake_minimum_required(VERSION 3.10)

set(SIMPLE_MR_VERSION 0.0.1)

project(SimpleMapReduce VERSION ${SIMPLE_MR_VERSION} LANGUAGES CXX)
message(STATUS "Build SimpleMapReduce: v${SIMPLE_MR_VERSION}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Set build type" FORCE)
endif()

# for OPEN-MPI
# ref: https://github.com/open-mpi/ompi/issues/5157
add_definitions(-DOMPI_SKIP_MPICXX)

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

if((NOT CMAKE_CXX_STANDARD) OR (CMAKE_CXX_STANDARD LESS 17))
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_COMPILER mpic++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# grouping main task and package source files
add_library(simplemapreduce SHARED "")
include(${CMAKE_SOURCE_DIR}/cmake/sourcelist.cmake)
target_include_directories(simplemapreduce
  PUBLIC 
    ${CMAKE_SOURCE_DIR}/include
)

target_compile_options(simplemapreduce
  PUBLIC
    $<$<CONFIG:Release>:-O3>
    $<$<CONFIG:Debug>:-O0 -g -Wall -Wextra>
    $<$<CONFIG:RelWithDebInfo>:-O3 -g>
)
target_compile_definitions(simplemapreduce
  PUBLIC
    $<$<NOT:$<CONFIG:Debug>>:NDEBUG>
)