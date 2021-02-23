#ifndef SIMPLEMAPREDUCE_BASE_JOB_MANAGER_H_
#define SIMPLEMAPREDUCE_BASE_JOB_MANAGER_H_

#include <memory>

#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/ops/fileformat.h"

namespace mapreduce {
namespace base {

class JobManager {
 public:
  virtual void start() = 0;

  void set_conf(std::shared_ptr<mapreduce::JobConf> conf) { conf_ = conf; }

  void set_file_format(std::unique_ptr<mapreduce::FileFormat> file_fmt) { file_fmt_ = std::move(file_fmt); };

 protected:
  std::shared_ptr<mapreduce::JobConf> conf_ = nullptr;

  std::unique_ptr<mapreduce::FileFormat> file_fmt_ = nullptr;
};

}  // namespace base
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_BASE_JOB_MANAGER_H_