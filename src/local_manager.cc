#include "simplemapreduce/local/manager.h"

#include <mpi.h>

#include "simplemapreduce/base/job_tasks.h"
#include "simplemapreduce/util/log.h"

using namespace mapreduce::base;
using namespace mapreduce::util;

namespace mapreduce {
namespace local {

void LocalJobManager::start() {
  /// Following two processes must be synchronized
  /// Map/Shuffle
  start_mapper_tasks();
  /// Sort/Reduce
  start_reducer_tasks();
}

int LocalJobManager::find_available_worker() {
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

void LocalJobManager::start_mapper_tasks() {
  logger.debug("[Master] Starting Map tasks");

  /// Temporary data container to receive data from child node
  char tmp;

  for (std::string path = file_fmt_->get_filepath(); !path.empty(); path = file_fmt_->get_filepath()) {
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

void LocalJobManager::start_reducer_tasks() {
  logger.debug("[Master] Starting Reduce tasks");

  /// Send signals to all nodes to resume for reduce tasks
  for (int i = 1; i <= conf_->worker_size; ++i)
    MPI_Send("\0", 1, MPI_CHAR, i, TaskType::sort_start, MPI_COMM_WORLD);

  /// Check all process have been finished
  char tmp;
  for (int i = 0; i < conf_->worker_size; ++i)
    MPI_Irecv(&tmp, 1, MPI_CHAR, i+1, TaskType::reduce_end, MPI_COMM_WORLD, &mpi_reqs[i]);

  /// Block until received all signals to finish tasks
  MPI_Waitall(mpi_reqs.size(), mpi_reqs.data(), mpi_worker_statuses.data());

  logger.debug("[Master] All Reduces finished");
}

}  // namespace local
}  // namespace mapreduce