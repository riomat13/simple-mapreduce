#include "simplemapreduce/local/runner.h"

#include <future>
#include <string>

#include <mpi.h>

#include "simplemapreduce/base/job_tasks.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/util/log.h"

namespace fs = std::filesystem;

using namespace mapreduce::base;
using namespace mapreduce::data;
using namespace mapreduce::util;

namespace mapreduce {
namespace local {

std::string LocalJobRunner::receive_filepath() {
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

void LocalJobRunner::start() {
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
}

void LocalJobRunner::run_map_tasks() {
  /// Send signal to notify enqueue step is finished
  auto mq = mapper_->get_mq();

  std::future<void> combiner_ftr;

  if (combiner_ != nullptr) {
    /// Start Combiner
    combiner_->set_mq(mq);
    combiner_ftr = std::async(std::launch::async, [&]{ combiner_->run(); });
  }

  while (true) {
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
    ByteData key, value{1l};
    key.read_file(target_path);

    /// Start map task
    mapper_->run(key, value);

    /// Notify the map task is finished
    MPI_Send("\1", 1, MPI_CHAR, 0, TaskType::map_end, MPI_COMM_WORLD);
  }

  /// Send signal to the end of Map
  mq->end();

  if(combiner_ != nullptr) {
    logger.debug("[Worker] Running Combiner on worker ", conf_->worker_rank);
    /// Wait until Combiner end and send signal to notify the end of the process
    combiner_ftr.get();
    mq->end();
  }

  logger.debug("[Worker] Finished Map on worker ", conf_->worker_rank);
}

void LocalJobRunner::run_shuffle_tasks() {
  auto shuffler = mapper_->get_shuffle();
  shuffler->run();
}

void LocalJobRunner::run_reduce_tasks() {
  reducer_->run();
}

}  // namespace local
}  // namespace mapreduce