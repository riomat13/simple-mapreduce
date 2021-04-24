#ifndef SIMPLEMAPREDUCE_BASE_JOB_RUNNER_H_
#define SIMPLEMAPREDUCE_BASE_JOB_RUNNER_H_

#include <filesystem>
#include <memory>

#include "simplemapreduce/base/job_tasks.h"
#include "simplemapreduce/ops/conf.h"

namespace mapreduce {
namespace base {

class JobRunner {
 public:
  virtual ~JobRunner() = default;

  virtual void start() = 0;

  void set_mapper(std::unique_ptr<mapreduce::base::MapTask>);
  void set_combiner(std::unique_ptr<mapreduce::base::ReduceTask>);
  void set_reducer(std::unique_ptr<mapreduce::base::ReduceTask>);

  void set_conf(std::shared_ptr<mapreduce::JobConf> conf) { conf_ = conf; }

 protected:
  std::shared_ptr<mapreduce::JobConf> conf_ = nullptr;

  std::unique_ptr<mapreduce::base::MapTask> mapper_ = nullptr;
  std::unique_ptr<mapreduce::base::ReduceTask> combiner_ = nullptr;
  std::unique_ptr<mapreduce::base::ReduceTask> reducer_ = nullptr;
};

}  // namespace base
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_BASE_JOB_RUNNER_H_