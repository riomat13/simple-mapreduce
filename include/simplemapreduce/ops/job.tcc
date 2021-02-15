#include <stdexcept>
#include <type_traits>

#include "simplemapreduce/util/log.h"

namespace mapreduce {

template <class Mapper>
void Job::set_mapper()
{
  if (!std::is_base_of<mapreduce::base::MapTask, Mapper>::value)
    throw std::runtime_error("Invalid Mapper Class");

  mapper_ = std::make_unique<Mapper>();
  mapper_->set_conf(conf_);
}

template <class Combiner>
void Job::set_combiner()
{
  if (!std::is_base_of<mapreduce::base::ReduceTask, Combiner>::value)
    throw std::runtime_error("Invalid Combiner Class");

  combiner_ = std::make_unique<Combiner>();
  combiner_->set_conf(conf_);
}

template <class Reducer>
void Job::set_reducer()
{
  if (!std::is_base_of<mapreduce::base::ReduceTask, Reducer>::value)
    throw std::runtime_error("Invalid Reducer Class");

  reducer_ = std::make_unique<Reducer>();
  reducer_->set_conf(conf_);
}

template <typename T>
void Job::set_config(const std::string &key, T&& value)
{
  if (key == "n_groups")
  {
    if (value > conf_->worker_size || value < 0)
    {
      /// Group size can be at most the number of workers
      conf_->n_groups = conf_->worker_size;
      if (conf_->worker_rank == 0)
      {
        if (value > 0)
          mapreduce::util::logger.warning("Group size exceeds the number of worker nodes. Use the number of worker nodes instead.");
        mapreduce::util::logger.info("[Master] Config: ", key, "=", conf_->worker_size);
      }
      return;
    } else {
      conf_->n_groups = value;
    }
  }
  else if (key == "log_level")
    mapreduce::util::logger.set_log_level(mapreduce::util::LogLevel(value));
  else {
    if (conf_->worker_rank == 0)
      mapreduce::util::logger.warning("Invalid parameter key: ", key);
    return;
  }
  
  /// Only show the change from master node to avoid duplicates
  if (conf_->worker_rank == 0)
    mapreduce::util::logger.info("[Master] Config: ", key, "=", value);
}
  
}  // namespace mapreduce