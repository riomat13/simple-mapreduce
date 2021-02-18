cmake_minimum_required(VERSION 3.10)

project(simplemr-task LANGUAGES CXX)

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

set(SIMPLEMR_BUILD_APP_TYPE "wordcount" CACHE INTERNAL "App name to build")

# MPI
find_package(MPI REQUIRED)
if(MPI_FOUND)
  include_directories(SYSTEM ${MPI_INCLUDE_PATH})
else (MPI_FOUND)
  message(SEND_ERROR "MPI not found")
endif()
add_definitions(-DOMPI_SKIP_MPICXX)

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

if(NOT MPI_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_EXTENSIONS OFF)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
  set(MPI_CXX_COMPILER mpic++)
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/..)

# create executable
add_executable(run_task "")

# set selected app as executable
if(SIMPLEMR_BUILD_APP_TYPE STREQUAL "custom")
  include(sourcelist.cmake)
elseif(SIMPLEMR_BUILD_APP_TYPE STREQUAL "wordcount")
  target_sources(run_task PRIVATE ${PROJECT_SOURCE_DIR}/wordcount/main.cc)
elseif(SIMPLEMR_BUILD_APP_TYPE STREQUAL "wordcount-with-combiner")
  target_sources(run_task PRIVATE ${PROJECT_SOURCE_DIR}/wordcount_with_combiner/main.cc)
elseif(SIMPLEMR_BUILD_APP_TYPE STREQUAL "movielens")
  target_sources(run_task PRIVATE ${PROJECT_SOURCE_DIR}/movielens/main.cc)
else()
  message(SEND_ERROR "Invalid app name: ${SIMPLEMR_BUILD_APP_TYPE}")
endif()

message(STATUS "App name: ${SIMPLEMR_BUILD_APP_TYPE}")
unset(SIMPLEMR_BUILD_APP_TYPE)

target_include_directories(run_task PRIVATE ${PROJECT_SOURCE_DIR}/../include)
if(TARGET simplemapreduce)
  target_link_libraries(run_task PRIVATE simplemapreduce)
else()
  target_link_libraries(run_task PRIVATE ${PROJECT_SOURCE_DIR}/../build/libsimplemapreduce.so)
endif()
target_link_libraries(run_task PRIVATE ${MPI_LIBRARIES})
target_compile_options(run_task PRIVATE -Wall -Wextra)