#ifndef SIMPLEMAPREDUCE_OPS_CONF_H_
#define SIMPLEMAPREDUCE_OPS_CONF_H_

#include <filesystem>

namespace mapreduce {

  /**
   * Store basic configurations and parameters for mapreduce tasks
   */
  struct JobConf {
    /* # of groups to split tasks */ int n_groups{10};
    /* Temporary directory path */   std::filesystem::path tmpdir{"./_tmp"};
    /* Output file path */           std::filesystem::path output_dirpath;
    /* # of worker to run tasks */   int worker_size{0};
    /* Current worker rank */        int worker_rank{0};
  };

}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_OPS_CONF_H_