#ifndef SIMPLEMAPREDUCE_OPS_CONF_H_
#define SIMPLEMAPREDUCE_OPS_CONF_H_

#include <filesystem>

namespace fs = std::filesystem;

namespace mapreduce {

  /**
   * Store basic configurations and parameters for mapreduce tasks
   */
  struct JobConf {
    /* # of groups to split tasks */ int n_groups{10};
    /* Temporary directory path */   fs::path tmpdir{"./_tmp"};
    /* # of nodes */                 int mpi_size;
    /* Current rank in mpi */        int mpi_rank;
    /* # of worker to run tasks */   int worker_size;
    /* Current worker rank */        int worker_rank{-1};
    /* Logger level */               int log_level{0};  // not used
  };

} // namespace mapreduce

#endif