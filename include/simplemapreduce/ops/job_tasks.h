#ifndef SIMPLEMAPREDUCE_OPS_JOB_TASKS_H_
#define SIMPLEMAPREDUCE_OPS_JOB_TASKS_H_

#include <memory>

#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"
#include "simplemapreduce/proc/loader.h"
#include "simplemapreduce/proc/shuffle.h"
#include "simplemapreduce/proc/sorter.h"
#include "simplemapreduce/proc/writer.h"

using namespace mapreduce::proc;

namespace mapreduce {

class JobTask
{
 public:
  /**
   * Set configuration to create Shuffle and Sorter.
   */
  void set_conf(const JobConf &conf)
  {
    conf_ = conf;
  }

 protected:
  JobConf conf_;
};

template <typename K, typename V>
class MapperJob : public JobTask
{
 public:
  typedef MessageQueue<K, V> MQ;

  MapperJob()
  {
    mq_ = std::make_shared<MQ>(MQ());
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

 private:
  /// Used to create tasks with Mapper state
  template <class M, class R> friend class Job;

  /**
   * Create a MessageQueue object for mapper
   */
  std::shared_ptr<MQ> get_mq() { return mq_; };

  /// MessageQueue to store data processed by mapper
  std::shared_ptr<MQ> mq_ = nullptr;

  std::unique_ptr<Shuffle<K, V>> shuffle_ = nullptr;
};

template <typename K, typename V>
class ReduceJob : public JobTask
{
 public:

  /**
   * Create output data writer.
   *
   *  @param path& output file path
   */
  std::unique_ptr<Context> get_context(const std::string &path)
  {
    std::unique_ptr<OutputWriter> writer = std::make_unique<OutputWriter>(path);
    return std::make_unique<Context>(std::move(writer));
  }

  /**
   * Get const Sorter instance.
   */
  std::unique_ptr<Sorter<K, V>> get_sorter()
  {
    std::unique_ptr<DataLoader<K, V>> loader = std::make_unique<BinaryFileDataLoader<K, V>>(conf_);
    return std::make_unique<Sorter<K, V>>(std::move(loader));
  }

 private:
  /// Used to create tasks with Mapper state
  template <class M, class R> friend class Job;

  std::unique_ptr<Sorter<K, V>> sorter_ = nullptr;
};

}  // namespace mapreduce

#endif