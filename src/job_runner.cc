#include "simplemapreduce/base/job_runner.h"

namespace mapreduce {
namespace base {

void JobRunner::set_mapper(std::unique_ptr<mapreduce::base::MapTask> mapper) {
  mapper_ = std::move(mapper);
  mapper_->set_conf(conf_);
};

void JobRunner::set_combiner(std::unique_ptr<mapreduce::base::ReduceTask> combiner) {
  combiner_ = std::move(combiner);
  combiner_->set_conf(conf_);
  combiner_->as_combiner();
};

void JobRunner::set_reducer(std::unique_ptr<mapreduce::base::ReduceTask> reducer) {
  reducer_ = std::move(reducer);
  reducer_->set_conf(conf_);
};

}  // namespace base
}  // namespace mapreduce