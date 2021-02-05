#ifndef SIMPLEMAPREDUCE_OPS_JOB_TASKS_H_
#define SIMPLEMAPREDUCE_OPS_JOB_TASKS_H_

#include <memory>

#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"
#include "simplemapreduce/proc/shuffle.h"
#include "simplemapreduce/proc/sorter.h"
#include "simplemapreduce/proc/writer.h"

using namespace mapreduce::proc;

namespace mapreduce {

template <typename K, typename V>
class MapperJob
{
 public:
  typedef MessageQueue<K, V> MQ;

  MapperJob()
  {
    mq_ = std::make_shared<MQ>(MQ());
  }

  /**
   * Set configuration to create Shuffle and Sorter.
   */
  void set_conf(const JobConf &conf)
  {
    conf_ = conf;
  }

  /**
   * Get const Context data writer.
   */
  std::unique_ptr<Context> get_context()
  {
    std::unique_ptr<MQWriter<K, V>> writer = std::make_unique<MQWriter<K, V>>(mq_);
    return std::make_unique<Context>(std::move(writer));
  }

  /**
   * Get const Shuffle instance.
   */
  std::unique_ptr<Shuffle<K, V>> get_shuffle()
  {
    return std::make_unique<Shuffle<K, V>>(mq_, conf_);
  };

  /**
   * Get const Sorter instance.
   */
  std::unique_ptr<Sorter<K, V>> get_sorter()
  { 
    return std::make_unique<Sorter<K, V>>(conf_);
  }
  
 private:
  /// Used to create tasks with Mapper state
  template <class M, class R> friend class Job;

  JobConf conf_;

  /**
   * Create a MessageQueue object for mapper
   */
  std::shared_ptr<MQ> get_mq() { return mq_; };

  /// MessageQueue to store data processed by mapper
  std::shared_ptr<MQ> mq_ = nullptr;

  std::unique_ptr<Shuffle<K, V>> shuffle_ = nullptr;
  std::unique_ptr<Sorter<K, V>> sorter_ = nullptr;
};

}  // namespace mapreduce

#endif