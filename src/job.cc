#include "simplemapreduce/ops/job.h"

#include <chrono>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <type_traits>

#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/local/runner.h"
#include "simplemapreduce/proc/writer.h"
#include "simplemapreduce/util/argparse.h"
#include "simplemapreduce/util/log.h"
#include "simplemapreduce/util/parser.h"

namespace fs = std::filesystem;

using namespace mapreduce::base;
using namespace mapreduce::commons;
using namespace mapreduce::data;
using namespace mapreduce::proc;
using namespace mapreduce::util;

namespace mapreduce {

/* --------------------------------------------------
 *   Constructor/Destructor
 * -------------------------------------------------- */
Job::Job() {
  /// Start networking
  MPI_Init(nullptr, nullptr);
  start_up();
}

Job::Job(int &argc, char *argv[]) {
  ArgParser parser{argc, argv};

  /// Not to run if -h/--help option is passed
  if (parser.is_help()) {
    is_valid_ = false;
    return;
  }

  /// Start networking
  MPI_Init(&argc, &argv);
  start_up();

  auto inputs = parse_string(parser.get_option("input"));
  if (!inputs.empty())
    file_fmt_.add_input_paths(inputs);
  
  auto output = parser.get_option("output");
  file_fmt_.set_output_path(std::move(output));
}

void Job::start_up() {
  runner_ = std::make_unique<mapreduce::local::LocalJobRunner>();
  runner_->set_conf(conf_);

  /// Initialize connection and get the status
  MPI_Comm_rank(MPI_COMM_WORLD, &(conf_->mpi_rank));
  MPI_Comm_size(MPI_COMM_WORLD, &(conf_->mpi_size));

  if (conf_->mpi_rank != 0) {
    /// Exclude master node as a count of workers
    /// This will be used for later computation and identifiers
    conf_->worker_rank = conf_->mpi_rank - 1;
  }

  conf_->worker_size = conf_->mpi_size - 1;

  if (conf_->worker_size == 0) {
    logger.error("No worker node is provieded. Aborting...");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
}

Job::~Job() {
  /// Close connection
  if (is_valid_)
    MPI_Finalize();
}

/* --------------------------------------------------
 *   Setup
 * -------------------------------------------------- */
void Job::add_input_path(const std::string& path) {
  file_fmt_.add_input_path(std::move(path));
}

void Job::set_output_path(const std::string& path) {
  file_fmt_.set_output_path(std::move(path));
}

void Job::setup_tmp_dir() {
  /// Clear all directory if exists
  if (fs::exists(conf_->tmpdir)) {
    for (const auto& p: fs::directory_iterator(conf_->tmpdir)) {
      fs::remove_all(p);
    }
  } else {
    /// create a new one if not exist
    fs::create_directory(conf_->tmpdir);
  }
}

void Job::setup_output_dir() {
  /// Clear all directory if exists
  if (fs::exists(file_fmt_.get_output_path())) {
    for (const auto& p: fs::directory_iterator(file_fmt_.get_output_path())) {
      fs::remove_all(p);
    }
  } else {
    /// create a new one if not exist
    fs::create_directory(file_fmt_.get_output_path());
  }
}

/* --------------------------------------------------
 *   Helper functions
 * -------------------------------------------------- */
int Job::find_available_worker() {
  /// If not all workers started
  if (static_cast<int>(mpi_reqs.size()) < conf_->worker_size) {
    mpi_reqs.emplace_back();
    mpi_worker_statuses.emplace_back();

    return mpi_reqs.size() - 1;
  }

  int index;
  MPI_Waitany(mpi_reqs.size(), mpi_reqs.data(), &index, mpi_worker_statuses.data());
  return index;
}

/* --------------------------------------------------
 *   Master Node
 * -------------------------------------------------- */
void Job::start_mapper_tasks() {
  logger.debug("[Master] Starting Map tasks");

  /// Temporary data container to receive data from child node
  char tmp;

  for (std::string path = file_fmt_.get_filepath(); !path.empty(); path = file_fmt_.get_filepath()) {
    /// Find available worker node
    /// This involves blocking until at least one connection is finished
    int worker_id = find_available_worker();

    /// Notify to worker being assigned a task
    MPI_Send("\1", 1, MPI_CHAR, worker_id+1, TaskType::ready, MPI_COMM_WORLD);

    /// Update state as "busy"
    MPI_Recv(&tmp, 1, MPI_CHAR, worker_id+1, TaskType::map_start, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    /// Send a path to the worker
    MPI_Send(path.c_str(), path.size(), MPI_CHAR, worker_id+1, TaskType::map_data, MPI_COMM_WORLD);

    /// Wait for the current processing worker becomes free
    MPI_Irecv(&tmp, 1, MPI_CHAR, worker_id+1, TaskType::map_end, MPI_COMM_WORLD, &mpi_reqs[worker_id]);
  }

  /// Wait for all processes are finished
  MPI_Waitall(mpi_reqs.size(), mpi_reqs.data(), mpi_worker_statuses.data());

  while (static_cast<int>(mpi_reqs.size()) < conf_->worker_size)
    find_available_worker();

  /// Send signals to all nodes to notify the map process ends
  for (int i = 0; i < conf_->worker_size; ++i)
    MPI_Isend("\0", 1, MPI_CHAR, i+1, TaskType::ready, MPI_COMM_WORLD, &mpi_reqs[i]);

  /// Get signals of finished tasks up to shuffle
  for (int i = 0; i < conf_->worker_size; ++i)
    MPI_Irecv(&tmp, 1, MPI_CHAR, i+1, TaskType::shuffle_end, MPI_COMM_WORLD, &mpi_reqs[i]);

  MPI_Waitall(mpi_reqs.size(), mpi_reqs.data(), mpi_worker_statuses.data());

  logger.debug("[Master] All Maps finished");
}

void Job::start_reducer_tasks() {
  logger.debug("[Master] Starting Reduce tasks");

  /// Send signals to all nodes to resume for reduce tasks
  for (int i = 1; i < conf_->mpi_size; ++i)
    MPI_Send("\0", 1, MPI_CHAR, i, TaskType::sort_start, MPI_COMM_WORLD);

  /// Check all process have been finished
  char tmp;
  for (int i = 0; i < conf_->worker_size; ++i)
    MPI_Irecv(&tmp, 1, MPI_CHAR, i+1, TaskType::reduce_end, MPI_COMM_WORLD, &mpi_reqs[i]);

  /// Block until received all signals to finish tasks
  MPI_Waitall(mpi_reqs.size(), mpi_reqs.data(), mpi_worker_statuses.data());

  logger.debug("[Master] All Reduces finished");
}

void Job::start_master_node() {
  /// Following two processes must be synchronized
  /// Map/Shuffle
  start_mapper_tasks();
  /// Sort/Reduce
  start_reducer_tasks();
}

/* --------------------------------------------------
 *   Main
 * -------------------------------------------------- */
int Job::run() {
  /// Not run when -h/--help option is passed
  if (!is_valid_) return 0;

  /// send target file to each node
  if (conf_->mpi_rank == 0) {
    if (!has_mapper_ || !has_reducer_)
      throw std::runtime_error("Mapper and/or Reducer is not set.");

    /// Set up a directory to store intermediate state data
    setup_tmp_dir();
    setup_output_dir();

    start_master_node(); 

    fs::remove_all(conf_->tmpdir);
    logger.info("[Master] Finished task");
  } else {
    conf_->output_dirpath = file_fmt_.get_output_path();
    runner_->start();
    logger.debug("[Worker] Finished all tasks on worker ", conf_->worker_rank);
  }

  return 0;
}
  
}  // namespace mapreduce