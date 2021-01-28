cmake_minimum_required(VERSION 3.10)

set(SIMPLE_MR_VERSION 0.0.1)

project(SimpleMapReduce VERSION ${SIMPLE_MR_VERSION} LANGUAGES CXX)
message(STATUS "Build SimpleMapReduce: v${SIMPLE_MR_VERSION}")

# for OPEN-MPI
# ref: https://github.com/open-mpi/ompi/issues/5157
add_definitions(-DOMPI_SKIP_MPICXX)

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

if((NOT CMAKE_CXX_STANDARD) OR (CMAKE_CXX_STANDARD LESS 17))
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

set(CMAKE_CXX_COMPILER mpic++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -lmpi_cxx")

include_directories(${SimpleMapReduce_SOURCE_DIR}/include)
include_directories(${SimpleMapReduce_SOURCE_DIR}/include/data)
include_directories(${SimpleMapReduce_SOURCE_DIR}/include/mr)
include_directories(${SimpleMapReduce_SOURCE_DIR}/include/ops)
include_directories(${SimpleMapReduce_SOURCE_DIR}/include/proc)
include_directories(${SimpleMapReduce_SOURCE_DIR}/include/util)

# grouping main task and package source files
add_executable(main "")
include(${SimpleMapReduce_SOURCE_DIR}/cmake/sourcelist.cmake)
include(${SimpleMapReduce_SOURCE_DIR}/example/sourcelist.cmake)

set_property(TARGET main PROPERTY CXX_STANDARD 17)

target_compile_options(main PRIVATE -Wall -Wextra)
