#ifndef SIMPLEMAPREDUCE_OPS_JOB_H_
#define SIMPLEMAPREDUCE_OPS_JOB_H_

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <mpi.h>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/config.h"
#include "simplemapreduce/base/fileformat.h"
#include "simplemapreduce/base/job_manager.h"
#include "simplemapreduce/base/job_runner.h"
#include "simplemapreduce/base/job_tasks.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/ops/context.h"

namespace mapreduce {

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
  void set_config(Config, T&&);

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
   * Run master node to handle map/reduce tasks on child nodes
   */
  void start_master_node();

  /**
   * Clean up temporary data
   */
  void cleanup_temps();

  /// Worker flag to valid to run.
  /// Set to false if not going to run,
  /// which is when -h/--help option is passed.
  bool is_valid_{true};

  /// Used if the current worker run job manger
  bool is_master_{false};

  /// Whether mapper/reducer are set
  bool has_mapper_{false};
  bool has_reducer_{false};

  /// Job parameters
  std::shared_ptr<mapreduce::JobConf> conf_ = std::make_shared<mapreduce::JobConf>();

  /// Job manager running on master node
  std::unique_ptr<mapreduce::base::JobManager> job_manager_;

  /// Child job runner
  std::unique_ptr<mapreduce::base::JobRunner> job_runner_;

  /// Input file handler
  /// This will be used for Job Manager
  std::unique_ptr<mapreduce::base::FileFormat> file_fmt_;

  /// Network parameters and statuses
  std::vector<MPI_Request> mpi_reqs;
  std::vector<MPI_Status> mpi_worker_statuses;
};

}  // namespace mapreduce

#include "simplemapreduce/ops/job-inl.h"

#endif