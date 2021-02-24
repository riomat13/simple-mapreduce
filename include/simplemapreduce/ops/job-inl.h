#include <type_traits>

#include "simplemapreduce/util/log.h"

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

template <typename T>
void Job::set_config(const std::string &key, T&& value) {
  if (key == "n_groups") {
    if (value > conf_->worker_size || value < 1) {
      /// Group size can be at most the number of workers
      conf_->n_groups = conf_->worker_size;
      if (is_master_) {
        if (value > 0) {
          mapreduce::util::logger.warning("Group size exceeds the number of worker nodes. Use the number of worker nodes instead.");
        } else if (value == 0) {
          mapreduce::util::logger.warning("Group size must be non-zero. Use the number of worker nodes instead.");
        }
        mapreduce::util::logger.info("[Master] Config: ", key, "=", conf_->worker_size);
      }
      return;
    } else {
      conf_->n_groups = value;
    }
  }
  else if (key == "log_level") {
    mapreduce::util::logger.set_log_level(mapreduce::util::LogLevel(value));
  } else {
    if (is_master_)
      mapreduce::util::logger.warning("Invalid parameter key: ", key);
    return;
  }
  
  /// Only show the change from master node to avoid duplicates
  if (is_master_)
    mapreduce::util::logger.info("[Master] Config: ", key, "=", value);
}
  
}  // namespace mapreduce