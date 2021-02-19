target_sources(simplemapreduce PRIVATE
  ${SimpleMapReduce_SOURCE_DIR}/src/argparse.cc
  ${SimpleMapReduce_SOURCE_DIR}/src/bytes.cc
  ${SimpleMapReduce_SOURCE_DIR}/src/fileformat.cc
  ${SimpleMapReduce_SOURCE_DIR}/src/job.cc
  ${SimpleMapReduce_SOURCE_DIR}/src/loader.cc
  ${SimpleMapReduce_SOURCE_DIR}/src/log.cc
  ${SimpleMapReduce_SOURCE_DIR}/src/parser.cc
  ${SimpleMapReduce_SOURCE_DIR}/src/queue.cc
  ${SimpleMapReduce_SOURCE_DIR}/src/writer.cc
)
