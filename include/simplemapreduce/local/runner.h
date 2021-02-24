#ifndef SIMPLEMAPREDUCE_LOCAL_RUNNER_H_
#define SIMPLEMAPREDUCE_LOCAL_RUNNER_H_

#include <filesystem>

#include "simplemapreduce/base/job_runner.h"

namespace mapreduce {
namespace local {

/**
 * Job Runner for shared memory.
 * This is intended to be used on local machine or distributed file system such as NFS.
 */
class LocalJobRunner : public mapreduce::base::JobRunner {
 public:
  /** Run MapReduce Job. */
  void start() override;

 private:
  /**
   * Receive file path to process with Mapper from master node.
   */
  std::string receive_filepath();

  /**
   * Execute map tasks on child nodes
   */
  void run_map_tasks();

  /**
   * Execute shuffle tasks on child nodes
   */
  void run_shuffle_tasks();

  /**
   * Execute reduce tasks on child nodes
   */
  void run_reduce_tasks();

  /// Output file path to write results
  std::filesystem::path output_fpath_;
};

}  // namespace local
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_LOCAL_RUNNER_H_