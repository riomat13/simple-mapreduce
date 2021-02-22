#include <type_traits>

#include "simplemapreduce/util/log.h"

namespace mapreduce {

template <class Mapper>
void Job::set_mapper() {
  static_assert(std::is_base_of<mapreduce::base::MapTask, Mapper>::value,
                "Invalid Mapper Class");

  runner_->set_mapper(std::make_unique<Mapper>());
  has_mapper_ = true;
}

template <class Combiner>
void Job::set_combiner() {
  static_assert(std::is_base_of<mapreduce::base::ReduceTask, Combiner>::value,
                "Invalid Combiner Class");

  runner_->set_combiner(std::make_unique<Combiner>());
}

template <class Reducer>
void Job::set_reducer() {
  static_assert(std::is_base_of<mapreduce::base::ReduceTask, Reducer>::value,
                "Invalid Reducer Class");

  runner_->set_reducer(std::make_unique<Reducer>());
  has_reducer_ = true;
}

template <typename T>
void Job::set_config(const std::string &key, T&& value) {
  if (key == "n_groups") {
    if (value > conf_->worker_size || value < 0) {
      /// Group size can be at most the number of workers
      conf_->n_groups = conf_->worker_size;
      if (conf_->worker_rank == 0) {
        if (value > 0) {
          mapreduce::util::logger.warning("Group size exceeds the number of worker nodes. Use the number of worker nodes instead.");
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
    if (conf_->worker_rank == 0)
      mapreduce::util::logger.warning("Invalid parameter key: ", key);
    return;
  }
  
  /// Only show the change from master node to avoid duplicates
  if (conf_->worker_rank == 0)
    mapreduce::util::logger.info("[Master] Config: ", key, "=", value);
}
  
}  // namespace mapreduce