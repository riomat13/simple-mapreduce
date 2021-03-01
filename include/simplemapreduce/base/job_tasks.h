#ifndef SIMPLEMAPREDUCE_BASE_JOB_TASKS_H_
#define SIMPLEMAPREDUCE_BASE_JOB_TASKS_H_

#include <memory>

#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/proc/loader.h"
#include "simplemapreduce/proc/shuffle.h"
#include "simplemapreduce/proc/sorter.h"
#include "simplemapreduce/proc/writer.h"

namespace mapreduce {
namespace base {

/**
 * Current task types.
 * Used for tag to send/recv messages.
 */
enum TaskType {
  start,
  ready,
  map_data,
  map_start,
  map_end,
  shuffle_start,
  shuffle_end,
  sort_start,
  sort_end,
  reduce_start,
  reduce_end,
  end,
};

class JobTask {
 public:
  /**
   * Set MapReduce job configuration.
   */
  void set_conf(std::shared_ptr<mapreduce::JobConf> conf) { conf_ = conf; }

 protected:
  std::shared_ptr<mapreduce::JobConf> conf_;

  virtual void run() {};
};

class MapTask : public JobTask {
 public:

  MapTask() {
    mq_ = std::make_shared<mapreduce::data::MessageQueue>(mapreduce::data::MessageQueue());
  }

  virtual std::unique_ptr<mapreduce::proc::ShuffleTask> get_shuffle() = 0;

  /**
   * Run Map process.
   *
   *  @param key    Mapper input key data
   *  @param value  Mapper input value data
   */
  virtual void run(mapreduce::data::ByteData&, mapreduce::data::ByteData&) = 0;

  /**
   * Create a MessageQueue object for mapper
   */
  std::shared_ptr<mapreduce::data::MessageQueue> get_mq() { return mq_; };

 private:
  /// MessageQueue to store data processed by mapper
  std::shared_ptr<mapreduce::data::MessageQueue> mq_ = nullptr;
};

class ReduceTask : public JobTask {
 public:
  /**
   * Run Reduce process.
   */
  virtual void run() = 0;

  /**
   * Set a ShuffleTask object for reduce.
   * This should have initial data in container which is not shuffled
   * because it would be processed by the same worker and directly stored to the container.
   */
  virtual void set_shuffle(std::unique_ptr<mapreduce::proc::ShuffleTask>) = 0;

  /**
   * Set a MessageQueue object for combiner.
   * If this is set, the Reducer will be seen as Combiner.
   */
  virtual void set_mq(std::shared_ptr<mapreduce::data::MessageQueue>) = 0;

  /** If called, this object will be used as Combiner. */
  virtual void as_combiner() = 0;
};

}  // namespace base
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_OPS_JOB_TASKS_H_