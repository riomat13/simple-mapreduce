#include "simplemapreduce/commons.h"

namespace mapreduce {

template <class Mapper>
void Job::set_mapper() {
  static_assert(std::is_base_of<mapreduce::base::MapTask, Mapper>::value,
                "Invalid Mapper Class");

  if (is_master_)
    has_mapper_ = true;
  else
    job_runner_->set_mapper(std::make_unique<Mapper>());
}

template <class Combiner>
void Job::set_combiner() {
  static_assert(std::is_base_of<mapreduce::base::ReduceTask, Combiner>::value,
                "Invalid Combiner Class");

  if (!is_master_)
    job_runner_->set_combiner(std::make_unique<Combiner>());
}

template <class Reducer>
void Job::set_reducer() {
  static_assert(std::is_base_of<mapreduce::base::ReduceTask, Reducer>::value,
                "Invalid Reducer Class");

  if (is_master_)
    has_reducer_ = true;
  else
    job_runner_->set_reducer(std::make_unique<Reducer>());
}

template <int N>
void Job::set_config(mapreduce::Config key, char value[N]) {
  set_config(key, std::string(value));
}

}  // namespace mapreduce