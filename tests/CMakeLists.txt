cmake_minimum_required(VERSION 3.10)
project(SimpleMapReduce-tests LANGUAGES CXX)

option(SIMPLEMR_TEST_VERBOSE "Test verbosity" OFF)

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

set(TEST_NAME simplemr-tests)

# set all test source files
set(TEST_SOURCES
  main.cc
  test_context.cc
  test_queue.cc
  test_sorter.cc
  test_writer.cc
  utils.cc
)

enable_testing()

add_executable(${TEST_NAME} ${TEST_SOURCES})
if(TARGET simplemapreduce)
  target_link_libraries(${TEST_NAME} PRIVATE simplemapreduce)
else()
  target_link_libraries(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/../build/libsimplemapreduce.so)
endif()
target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/../include)
target_link_libraries(${TEST_NAME} PRIVATE ${MPI_LIBRARIES})
if(SIMPLEMR_TEST_VERBOSE)
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME} --success)
else()
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endif()