# set up testing
function(setup_test test_target use_mpi)
  # set include directory
  target_include_directories(${test_target} PRIVATE ${PROJECT_SOURCE_DIR}/../include)

  # set libraries
  if(use_mpi)
    target_link_libraries(${test_target} PRIVATE ${MPI_LIBRARIES})
  endif()
endfunction()