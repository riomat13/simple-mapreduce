# set up testing
function(setup_test test_target)
  # link library
  if(TARGET simplemapreduce)
    target_link_libraries(${test_target} PRIVATE simplemapreduce)
  else()
    target_link_libraries(${test_target} PRIVATE ${PROJECT_SOURCE_DIR}/../build/libsimplemapreduce.so)
  endif()

  # set include directory
  target_include_directories(${test_target} PRIVATE ${PROJECT_SOURCE_DIR}/../include)

  # set MPI
  target_link_libraries(${test_target} PRIVATE ${MPI_LIBRARIES})
endfunction()