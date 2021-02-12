cmake_minimum_required(VERSION 3.10)
project(SimpleMapReduce-tests LANGUAGES CXX)

include(CTest)
include(ProcessorCount)
include(${PROJECT_SOURCE_DIR}/../cmake/utils.cmake)

option(SIMPLEMR_TEST_VERBOSE "Test verbosity" OFF)
set(SIMPLEMR_TESTCASES OFF CACHE INTERNAL "Partial testcases")
if (NOT SIMPLEMR_TESTCASES)
  set(SIMPLEMR_TEST_INTEGRATION ON CACHE INTERNAL "Integration testing")
else()
  # set integration test off when running partial tests
  set(SIMPLEMR_TEST_INTEGRATION OFF CACHE INTERNAL "Integration testing")
endif()

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
  else()
    # set partial test cases
    string(REPLACE "," ";" SIMPLEMR_TESTCASES ${SIMPLEMR_TESTCASES})
    message(STATUS "Unit testing:")
    foreach(name ${SIMPLEMR_TESTCASES})
      list(APPEND srcs "${PROJECT_SOURCE_DIR}/test_${name}.cc")
      message(STATUS "  - test_${name}")
    endforeach()

    set(UTEST_SOURCES
      main.cc
      utils.cc
      ${srcs}
    )
  endif()

  add_executable(${UTEST_NAME} ${LIB_SOURCES} ${UTEST_SOURCES})
  setup_test(${UTEST_NAME})

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

  foreach(type 1 2 3 4 5 6)
    set(ITEST_NAME simplemr-itests${type})

    set(ITEST_SOURCES
      main.cc
      test_integration.cc
      utils.cc
    )

    add_executable(${ITEST_NAME} ${LIB_SOURCES} ${ITEST_SOURCES})
    target_compile_definitions(${ITEST_NAME} PRIVATE "INTEGRATION${type}")
    setup_test(${ITEST_NAME})
    add_test(NAME ${ITEST_NAME} COMMAND ${MPIEXEC} -np 2 ./${ITEST_NAME})
  endforeach()
endif()