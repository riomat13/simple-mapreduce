#ifndef SIMPLEMAPREDUCE_OPS_JOB_H_
#define SIMPLEMAPREDUCE_OPS_JOB_H_

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <mpi.h>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/base/job_tasks.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/fileformat.h"

namespace mapreduce {

/**
 * Current task types.
 * Used for tag to send/recv messages.
 */
enum TaskType {
  start,
  ready,
  map_data,
  map_start,
  map_end,
  shuffle_start,
  shuffle_end,
  sort_start,
  sort_end,
  reduce_start,
  reduce_end,
  end,
};

/**
 * Job class to handle and manage all mapreduce process
 * including master and child nodes
 */
class Job {
 public:
  Job();
  Job(int &, char *[]);
  ~Job();

  /// Not allowed to copy nor move
  Job(const Job&) = delete;
  Job &operator=(const Job&) = delete;
  Job(Job&&) = delete;
  Job &operator=(Job&&) = delete;

  /**
   * Add directory path containing input files.
   *
   *  @param path   input directory path
   */
  void add_input_path(const std::string&);

  /**
   * Add directory path to store output files.
   * Note that if already set by command line argument,
   * raise warning and skip this.
   *
   *  @param path   output directory path
   */
  void set_output_path(const std::string&);

  /**
   * Set configurations for Job.
   */
  template <typename T>
  void set_config(const std::string &, T&&);

  /**
   * Setup Mapper.
   */
  template <class> void set_mapper();

  /**
   * Setup Combiner
   */
  template <class> void set_combiner();

  /**
   * Setup Reducer.
   */
  template <class> void set_reducer();

  /**
   * Start MapReduce job.
   *
   *  @return status exit status 0 if no error, otherwise 1
   */
  int run();

 private:
  /**
   * Helper for initialization.
   * This will construct mapper and reducer classes
   * and check the MPI, worker states.
   */
  void start_up();

  /**
   * Clean up all files and directory in a tmp directory.
   * If the tmp directory does not exist, create a new one.
   */
  void setup_tmp_dir();

  /**
   * Clean up all files and directory in an output directory.
   * If the directory does not exist, create a new one.
   */
  void setup_output_dir();

  /**
   * Get file path of the last state
   * based on provided output path.
   */
  std::filesystem::path get_output_file_path();

  /**
   * Receive file path to process with Mapper from master node.
   */
  std::string receive_filepath();

  /**
   * Search available worker except master and return the worker ID as MPI_Rank.
   * If not node is available, return -1
   */
  int find_available_worker();

  /**
   * Run master node to handle map/reduce tasks on child nodes
   */
  void start_master_node();

  /**
   * Send signal to start mapper tasks
   */
  void start_mapper_tasks();

  /**
   * Manage worker node run on master node.
   * Each function takes responsible for one worker.
   */
  void handle_mapper_worker();

  /**
   * Send signal to start reducer tasks
   */
  void start_reducer_tasks();

  /**
   * Execute tasks on child nodes
   */
  void run_child_tasks();

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

  /**
   * Clean up temporary data
   */
  void cleanup_temps();

  /// Worker flag to valid to run.
  /// Set to false if not going to run,
  /// which is when -h/--help option is passed.
  bool is_valid_{true};

  /// Job parameters
  std::shared_ptr<mapreduce::JobConf> conf_ = std::make_shared<mapreduce::JobConf>();

  /// Mapper instance
  std::unique_ptr<mapreduce::base::MapTask> mapper_ = nullptr;

  /// Combiner instance
  std::unique_ptr<mapreduce::base::ReduceTask> combiner_ = nullptr;

  /// Reducer instance
  std::unique_ptr<mapreduce::base::ReduceTask> reducer_ = nullptr;

  /// FileFormat instance
  mapreduce::FileFormat file_fmt_{};

  /// Network parameters and statuses
  std::vector<MPI_Request> mpi_reqs;
  std::vector<MPI_Status> mpi_worker_statuses;
};

}  // namespace mapreduce

#include "simplemapreduce/ops/job-inl.h"

#endif