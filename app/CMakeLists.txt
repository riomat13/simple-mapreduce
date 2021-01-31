cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

project(simplemr-task LANGUAGES CXX)

# for OPEN-MPI
# ref: https://github.com/open-mpi/ompi/issues/5157
add_definitions(-DOMPI_SKIP_MPICXX)

# to prevent std::string from converting to std::__cxx11::basic_string
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_COMPILER mpicxx)

set(CMAKE_CXX_COMPILER mpic++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR})

# create executable using simple-mapreduce app
find_file(SOURCE
  NAMES libsimplemapreduce.so
  PATHS ${PROJECT_SOURCE_DIR}/../build
  DOC "Path to source file of mapreduce app"
)

if(NOT SOURCE)
  message(FATAL_ERROR "A mapreduce app source file not found. Build the app first.")
endif()

add_executable(run_task "")
include(sourcelist.cmake)
#target_link_directories(run_task PUBLIC ../build)
target_include_directories(run_task PUBLIC ../include)
target_link_libraries(run_task PUBLIC ${SOURCE})
target_compile_options(run_task PUBLIC -Wall -Wextra)