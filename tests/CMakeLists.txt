cmake_minimum_required(VERSION 3.10)
project(SimpleMapReduce-tests LANGUAGES CXX)

include(ProcessorCount)
include(${PROJECT_SOURCE_DIR}/../cmake/utils.cmake)

option(SIMPLEMR_TEST_VERBOSE "Test verbosity" OFF)
option(SIMPLEMR_TEST_INTEGRATION "Integration testing" ON)

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
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g")
  set(MPI_CXX_COMPILER mpic++)
endif()

# Unittests
set(UTEST_NAME simplemr-utests)

set(LIB_SOURCES
  ${PROJECT_SOURCE_DIR}/../src/bytes.cc
  ${PROJECT_SOURCE_DIR}/../src/fileformat.cc
  ${PROJECT_SOURCE_DIR}/../src/loader.cc
  ${PROJECT_SOURCE_DIR}/../src/log.cc
  ${PROJECT_SOURCE_DIR}/../src/queue.cc
  ${PROJECT_SOURCE_DIR}/../src/sorter.cc
  ${PROJECT_SOURCE_DIR}/../src/writer.cc
)

# set all test source files
set(UTEST_SOURCES
  main.cc
  test_bytes.cc
  test_context.cc
  test_fileformat.cc
  test_func.cc
  test_loader.cc
  test_queue.cc
  test_shuffle.cc
  test_sorter.cc
  test_writer.cc
  utils.cc
)

enable_testing()

add_executable(${UTEST_NAME} ${LIB_SOURCES} ${UTEST_SOURCES})
setup_test(${UTEST_NAME})

# test verbosity
if(SIMPLEMR_TEST_VERBOSE)
  add_test(NAME ${UTEST_NAME} COMMAND ${UTEST_NAME} --success)
else()
  add_test(NAME ${UTEST_NAME} COMMAND ${UTEST_NAME})
endif()

# Integration tests
message(STATUS "Integration testing: ${SIMPLEMR_TEST_INTEGRATION}")
if(SIMPLEMR_TEST_INTEGRATION)
  ProcessorCount(N_PROC)
  if(N_PROC LESS 2)
    # check processor count and fail if not suffice to run integration tests
    message(SEND_ERROR "Failed: Processor Count: ${N_PROC}")
  else()
    message(STATUS "Processor Count: ${N_PROC}")
  endif()

  set(ITEST_NAME simplemr-itests)

  set(ITEST_SOURCES
    main.cc
    test_integration.cc
    utils.cc
  )

  add_executable(${ITEST_NAME} ${LIB_SOURCES} ${ITEST_SOURCES})
  setup_test(${ITEST_NAME})
  add_test(NAME ${ITEST_NAME} COMMAND ${MPIEXEC} -np 2 ./${ITEST_NAME})
endif()