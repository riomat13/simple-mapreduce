# set up testing
function(setup_test test_target)
  # set include directory
  target_include_directories(${test_target} PRIVATE ${PROJECT_SOURCE_DIR}/../include)

  # set MPI
  target_link_libraries(${test_target} PRIVATE ${MPI_LIBRARIES})
endfunction()