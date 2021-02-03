function(setup_test test_target)
  # link library
  if(TARGET simplemapreduce)
    target_link_libraries(${test_target} PRIVATE simplemapreduce)
  else()
    target_include_directories(${test_target} PRIVATE ${PROJECT_SOURCE_DIR}/../include)
  endif()

  # set include directory
  target_include_directories(${test_target} PRIVATE ${PROJECT_SOURCE_DIR}/../include)

  # set MPI
  target_link_libraries(${test_target} PRIVATE ${MPI_LIBRARIES})
endfunction()