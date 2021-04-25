cmake_minimum_required(VERSION 3.10)
project(SimpleMapReduce-tests LANGUAGES CXX)

include(CTest)
include(ProcessorCount)
include(${PROJECT_SOURCE_DIR}/../cmake/utils.cmake)

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

option(SIMPLEMR_TEST_VERBOSE "Test verbosity" OFF)
set(SIMPLEMR_TESTCASES OFF CACHE INTERNAL "Partial testcases")
if (NOT SIMPLEMR_TESTCASES)
  set(SIMPLEMR_TEST_INTEGRATION ON CACHE INTERNAL "Integration testing")
else()
  # set integration test off when running partial tests
  set(SIMPLEMR_TEST_INTEGRATION OFF CACHE INTERNAL "Integration testing")
endif()

set(SIMPLEMR_TEST_MPI_NUM_NODES 2 CACHE INTERNAL "Number of NPI nodes")
set(SIMPLEMR_TEST_MPI_ARGS "" CACHE INTERNAL "MPI arguments for testing")

# # MPI
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
  ${PROJECT_SOURCE_DIR}/../src/argparse.cc
  ${PROJECT_SOURCE_DIR}/../src/bytes.cc
  ${PROJECT_SOURCE_DIR}/../src/commons.cc
  ${PROJECT_SOURCE_DIR}/../src/loader.cc
  ${PROJECT_SOURCE_DIR}/../src/local_fileformat.cc
  ${PROJECT_SOURCE_DIR}/../src/log.cc
  ${PROJECT_SOURCE_DIR}/../src/parser.cc
  ${PROJECT_SOURCE_DIR}/../src/queue.cc
  ${PROJECT_SOURCE_DIR}/../src/writer.cc
)

enable_testing()

if(SIMPLEMR_TESTCASES STREQUAL "")
  message(STATUS "Skipping unit tests")
else()
  if(NOT SIMPLEMR_TESTCASES)
    message(STATUS "Unit testing: All")
    # set all test source files
    set(UTEST_SOURCES
      main.cc
      test_argparse.cc
      test_bytes.cc
      test_context.cc
      test_func.cc
      test_loader.cc
      test_local_fileformat.cc
      test_log.cc
      test_parser.cc
      test_queue.cc
      test_shuffle.cc
      test_sorter.cc
      test_writer.cc
      utils.cc
      ${LIB_SOURCES}
    )
  else()
    # set partial test cases
    string(REPLACE "," ";" SIMPLEMR_TESTCASES ${SIMPLEMR_TESTCASES})
    message(STATUS "Unit testing:")
    foreach(name ${SIMPLEMR_TESTCASES})
      list(APPEND srcs "${PROJECT_SOURCE_DIR}/test_${name}.cc")
      message(STATUS "  - test_${name}")

      if(${name} STREQUAL "argparse")
        list(APPEND srcs ${PROJECT_SOURCE_DIR}/../src/argparse.cc)
      elseif(${name} STREQUAL "bytes")
        list(APPEND srcs ${PROJECT_SOURCE_DIR}/../src/bytes.cc)
      elseif(${name} STREQUAL "context")
        list(APPEND srcs
            ${PROJECT_SOURCE_DIR}/../src/bytes.cc
            ${PROJECT_SOURCE_DIR}/../src/queue.cc
            ${PROJECT_SOURCE_DIR}/../src/writer.cc
        )
      elseif(${name} STREQUAL "func")
      elseif(${name} STREQUAL "loader")
        list(APPEND srcs
          ${PROJECT_SOURCE_DIR}/../src/bytes.cc
          ${PROJECT_SOURCE_DIR}/../src/loader.cc
          ${PROJECT_SOURCE_DIR}/../src/queue.cc
          ${PROJECT_SOURCE_DIR}/../src/writer.cc
        )
      elseif(${name} STREQUAL "local_fileformat")
        list(APPEND srcs
          ${PROJECT_SOURCE_DIR}/../src/local_fileformat.cc
          ${PROJECT_SOURCE_DIR}/../src/log.cc
        )
      elseif(${name} STREQUAL "log")
        list(APPEND srcs ${PROJECT_SOURCE_DIR}/../src/log.cc)
      elseif(${name} STREQUAL "parser")
        list(APPEND srcs ${PROJECT_SOURCE_DIR}/../src/parser.cc)
      elseif(${name} STREQUAL "queue")
        list(APPEND srcs
          ${PROJECT_SOURCE_DIR}/../src/bytes.cc
          ${PROJECT_SOURCE_DIR}/../src/queue.cc
        )
      elseif(${name} STREQUAL "shuffle")
        list(APPEND srcs
          ${PROJECT_SOURCE_DIR}/../src/bytes.cc
          ${PROJECT_SOURCE_DIR}/../src/queue.cc
          ${PROJECT_SOURCE_DIR}/../src/writer.cc
        )
      elseif(${name} STREQUAL "sorter")
        list(APPEND srcs
          ${PROJECT_SOURCE_DIR}/../src/bytes.cc
          ${PROJECT_SOURCE_DIR}/../src/loader.cc
          ${PROJECT_SOURCE_DIR}/../src/queue.cc
        )
      elseif(${name} STREQUAL "writer")
        list(APPEND srcs
          ${PROJECT_SOURCE_DIR}/../src/bytes.cc
          ${PROJECT_SOURCE_DIR}/../src/queue.cc
          ${PROJECT_SOURCE_DIR}/../src/writer.cc
        )
      endif()
    endforeach()

    list(REMOVE_DUPLICATES srcs)

    set(UTEST_SOURCES
      main.cc
      utils.cc
      ${srcs}
    )
  endif()

  add_executable(${UTEST_NAME} ${UTEST_SOURCES})
  target_compile_definitions(${UTEST_NAME} PRIVATE IS_LINUX)
  setup_test(${UTEST_NAME} OFF)

  # test verbosity
  if(SIMPLEMR_TEST_VERBOSE)
    add_test(NAME ${UTEST_NAME} COMMAND ${UTEST_NAME} --success)
  else()
    add_test(NAME ${UTEST_NAME} COMMAND ${UTEST_NAME})
  endif()
endif()

# Integration tests
message(STATUS "Integration testing: ${SIMPLEMR_TEST_INTEGRATION}")
if(SIMPLEMR_TEST_INTEGRATION)
  # check if integration tests are executable
  ProcessorCount(N_PROC)
  if(N_PROC LESS 2)
    # check processor count and fail if not suffice to run integration tests
    message(SEND_ERROR "Failed: Processor Count: ${N_PROC}")
  else()
    message(STATUS "Processor Count: ${N_PROC}")
  endif()

  foreach(type RANGE 1 7)
    set(ITEST_NAME simplemr-itests${type})

    set(ITEST_SOURCES
      # main source files
      ${PROJECT_SOURCE_DIR}/../src/job.cc
      ${PROJECT_SOURCE_DIR}/../src/job_runner.cc
      ${PROJECT_SOURCE_DIR}/../src/local_manager.cc
      ${PROJECT_SOURCE_DIR}/../src/local_runner.cc
      # test source files
      ${PROJECT_SOURCE_DIR}/main.cc
      ${PROJECT_SOURCE_DIR}/test_integration.cc
      ${PROJECT_SOURCE_DIR}/utils.cc
    )

    add_executable(${ITEST_NAME} ${LIB_SOURCES} ${ITEST_SOURCES})
    target_compile_definitions(${ITEST_NAME} PRIVATE "INTEGRATION${type}")
    setup_test(${ITEST_NAME} ON)
    add_test(NAME ${ITEST_NAME} COMMAND ${MPIEXEC} -np ${SIMPLEMR_TEST_MPI_NUM_NODES} ${SIMPLEMR_TEST_MPI_ARGS} ./${ITEST_NAME})
  endforeach()
endif()