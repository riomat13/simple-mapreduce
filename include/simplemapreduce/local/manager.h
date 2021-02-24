#ifndef SIMPLEMAPREDUCE_LOCAL_MANAGER_H_
#define SIMPLEMAPREDUCE_LOCAL_MANAGER_H_

#include <vector>

#include <mpi.h>

#include "simplemapreduce/base/job_manager.h"

namespace mapreduce {
namespace local {

class LocalJobManager : public mapreduce::base::JobManager {
 public:
  void start() override;

 private:
  /**
   * Send signal to start mapper tasks
   */
  void start_mapper_tasks();

  /**
   * Send signal to start reducer tasks
   */
  void start_reducer_tasks();

  /**
   * Search available worker except master and return the worker ID as MPI_Rank.
   * If not node is available, return -1
   */
  int find_available_worker();

  /// Network parameters and statuses
  std::vector<MPI_Request> mpi_reqs{};
  std::vector<MPI_Status> mpi_worker_statuses{};
};

}  // namespace local
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_LOCAL_MANAGER_H_