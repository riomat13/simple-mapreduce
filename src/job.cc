#include "simplemapreduce/ops/job.h"

#include <chrono>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <type_traits>

#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/proc/writer.h"
#include "simplemapreduce/util/log.h"

namespace fs = std::filesystem;

using namespace mapreduce::commons;
using namespace mapreduce::data;
using namespace mapreduce::proc;
using namespace mapreduce::util;

namespace mapreduce {

/* --------------------------------------------------
 *   Constructor/Destructor
 * -------------------------------------------------- */
Job::Job()
{
  /// Start networking
  MPI_Init(nullptr, nullptr);
  start_up();
}

Job::Job(int &argc, char *argv[])
{
  /// Start networking
  MPI_Init(&argc, &argv);
  start_up();
}

void Job::start_up()
{
  /// Initialize connection and get the status
  MPI_Comm_rank(MPI_COMM_WORLD, &(conf_->mpi_rank));
  MPI_Comm_size(MPI_COMM_WORLD, &(conf_->mpi_size));

  if (conf_->mpi_rank != 0)
  {
    /// Exclude master node as a count of workers
    /// This will be used for later computation and identifiers
    conf_->worker_rank = conf_->mpi_rank - 1;
  }

  conf_->worker_size = conf_->mpi_size - 1;

  if (conf_->worker_size == 0)
  {
    logger.error("No worker node is provieded. Aborting...");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
}

Job::~Job()
{
  /// Close connection
  MPI_Finalize();
}

/* --------------------------------------------------
 *   Setup
 * -------------------------------------------------- */
void Job::setup_tmp_dir()
{
  /// Clear all directory if exists
  if (fs::exists(conf_->tmpdir))
  {
    for (const auto& p: fs::directory_iterator(conf_->tmpdir))
      fs::remove_all(p);
  } else {
    /// create a new one if not exist
    fs::create_directory(conf_->tmpdir);
  }
}

void Job::setup_output_dir()
{
  /// Clear all directory if exists
  if (fs::exists(file_fmt_.get_output_path()))
  {
    for (const auto& p: fs::directory_iterator(file_fmt_.get_output_path()))
      fs::remove_all(p);
  } else {
    /// create a new one if not exist
    fs::create_directory(file_fmt_.get_output_path());
  }
}

fs::path Job::get_output_file_path()
{
  /// Set file path named by current worker rank and join with the output directory
  fs::path outdir = file_fmt_.get_output_path();
  std::ostringstream oss;
  oss << std::setw(5) << std::setfill('0') << conf_->worker_rank;
  fs::path fname = oss.str();
  
  return outdir / fname;
}

/* --------------------------------------------------
 *   Helper functions
 * -------------------------------------------------- */
int Job::find_available_worker()
{
  /// If not all workers started
  if (static_cast<int>(mpi_reqs.size()) < conf_->worker_size)
  {
    mpi_reqs.emplace_back();
    mpi_worker_statuses.emplace_back();

    return mpi_reqs.size() - 1;
  }

  int index;
  MPI_Waitany(mpi_reqs.size(), mpi_reqs.data(), &index, mpi_worker_statuses.data());
  return index;
}

std::string Job::receive_filepath()
{
    MPI_Status status;
    MPI_Probe(0, TaskType::map_data, MPI_COMM_WORLD, &status);

    /// Get data size to reveive for receiving file path.
    /// Lengths of file path are vary so that need to check data size first.
    int data_size;
    MPI_Get_count(&status, MPI_CHAR, &data_size);

    /// Receive target file path to apply map task
    char path_data[data_size];
    MPI_Recv(path_data, data_size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    return std::string(path_data, data_size);
}

/* --------------------------------------------------
 *   Master Node
 * -------------------------------------------------- */
void Job::start_mapper_tasks()
{
  logger.debug("[Master] Starting Map tasks");

  /// Extract all files in input directories
  std::vector<std::string> paths = file_fmt_.get_input_file_paths();

  /// Temporary data container to receive data from child node
  char tmp;

  for (auto& path: paths)
  {
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
  {
    MPI_Isend("\0", 1, MPI_CHAR, i+1, TaskType::ready, MPI_COMM_WORLD, &mpi_reqs[i]);
  }

  /// Get signals of finished tasks up to shuffle
  for (int i = 0; i < conf_->worker_size; ++i)
    MPI_Irecv(&tmp, 1, MPI_CHAR, i+1, TaskType::shuffle_end, MPI_COMM_WORLD, &mpi_reqs[i]);

  MPI_Waitall(mpi_reqs.size(), mpi_reqs.data(), mpi_worker_statuses.data());

  logger.debug("[Master] All Maps finished");
}

void Job::start_reducer_tasks()
{
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

void Job::start_master_node()
{
  /// Following two processes must be synchronized
  /// Map/Shuffle
  start_mapper_tasks();
  /// Sort/Reduce
  start_reducer_tasks();
}

/* --------------------------------------------------
 *   Child Node
 * -------------------------------------------------- */
void Job::run_child_tasks()
{
  conf_->output_fpath = get_output_file_path();

  /// Run mapreduce processes
  run_map_tasks();
  run_shuffle_tasks();

  /// Send signal to notify finished tasks up to shuffle
  MPI_Send("\0", 1, MPI_CHAR, 0, TaskType::shuffle_end, MPI_COMM_WORLD);

  /// Receive signal to resume tasks
  char tmp;
  MPI_Recv(&tmp, 1, MPI_CHAR, 0, TaskType::sort_start, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  /// Resume after all processes up to shuffle in all nodes has been finished
  run_reduce_tasks();

  /// Send signal to notify finished all tasks
  MPI_Send("\0", 1, MPI_CHAR, 0, TaskType::reduce_end, MPI_COMM_WORLD);

  logger.debug("[Worker] Finished all tasks on worker ", conf_->worker_rank);
}

void Job::run_map_tasks()
{
  /// Send signal to notify enqueue step is finished
  auto mq = mapper_->get_mq();

  std::future<void> combiner_ftr;

  if (combiner_ != nullptr)
  {
    /// Start Combiner
    combiner_->set_mq(mq);
    combiner_ftr = std::async(std::launch::async, [&]{ combiner_->run(); });
  }

  while (true)
  {
    char req;
    MPI_Recv(&req, 1, MPI_CHAR, 0, TaskType::ready, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    /// Once received end of process signal, break the loop
    if (req == '\0') 
      break;

    /// Check the future status if already run any task
    /// Send to notify this worker is available
    MPI_Send("\1", 1, MPI_CHAR, 0, TaskType::map_start, MPI_COMM_WORLD);

    /// Once received empty string, stop receiving data
    std::string target_path = receive_filepath();

    logger.debug("[Worker] Assigned a file: \"", target_path, "\" to worker ", conf_->worker_rank);

    /// Read data from text file
    {
      std::ifstream ifs(target_path);
      std::ostringstream oss;
      oss << ifs.rdbuf();
      ifs.close();

      /// Read data from text file
      ByteData key{oss.str()}, value{1l};
      oss.clear();

      /// Start map task
      mapper_->run(key, value);
    }

    /// Notify the map task is finished
    MPI_Send("\1", 1, MPI_CHAR, 0, TaskType::map_end, MPI_COMM_WORLD);
  }

  /// Send signal to the end of Map
  mq->end();

  if(combiner_ != nullptr)
  {
    logger.debug("[Worker] Running Combiner on worker ", conf_->worker_rank);
    /// Wait until Combiner end and send signal to notify the end of the process
    combiner_ftr.get();
    mq->end();
  }

  logger.debug("[Worker] Finished Map on worker ", conf_->worker_rank);
}

void Job::run_shuffle_tasks()
{
  auto shuffler = mapper_->get_shuffle();
  shuffler->run();
}

void Job::run_reduce_tasks() {
  reducer_->run();
}

int Job::run()
{
  /// send target file to each node
  if (conf_->mpi_rank == 0)
  {
    if (mapper_ == nullptr || reducer_ == nullptr)
      throw std::runtime_error("Mapper and/or Reducer is not set.");

    /// Set up a directory to store intermediate state data
    setup_tmp_dir();
    setup_output_dir();

    start_master_node(); 

    fs::remove_all(conf_->tmpdir);
    logger.info("[Master] Finished task");
  } else {
    run_child_tasks();
  }

  return 0;
}
  
}  // namespace mapreduce