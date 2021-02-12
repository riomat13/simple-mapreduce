#ifndef SIMPLEMAPREDUCE_OPS_JOB_TASKS_H_
#define SIMPLEMAPREDUCE_OPS_JOB_TASKS_H_

#include <memory>

#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"
#include "simplemapreduce/proc/loader.h"
#include "simplemapreduce/proc/shuffle.h"
#include "simplemapreduce/proc/sorter.h"
#include "simplemapreduce/proc/writer.h"

using namespace mapreduce::data;
using namespace mapreduce::proc;

namespace mapreduce {

class JobTask
{
 public:
  /**
   * Set MapReduce job configuration.
   */
  void set_conf(std::shared_ptr<JobConf> conf) { conf_ = conf; }

 protected:
  std::shared_ptr<JobConf> conf_;

  virtual void run() {};
};

class MapperJob : public JobTask
{
 public:

  MapperJob()
  {
    mq_ = std::make_shared<MessageQueue>(MessageQueue());
  }

  virtual std::unique_ptr<ShuffleJob> get_shuffle() = 0;

  /**
   * Run Map process.
   *
   *  @param key    Mapper input key data
   *  @param value  Mapper input value data
   */
  virtual void run(ByteData&, ByteData&) = 0;

 protected:
  /**
   * Create a MessageQueue object for mapper
   */
  std::shared_ptr<MessageQueue> get_mq() { return mq_; };

  /// MessageQueue to store data processed by mapper
  std::shared_ptr<MessageQueue> mq_ = nullptr;

 private:
  /// Used to create tasks with Mapper state
  friend class Job;
};

class ReduceJob : public JobTask
{
 public:
  /**
   * Run Reduce process.
   */
  virtual void run() = 0;

 protected:
  /**
   * Set a MessageQueue object for combiner.
   * If this is set, the Reducer will be seen as Combiner.
   */
  virtual void set_mq(std::shared_ptr<MessageQueue>) = 0;

 private:
  /// Used to create tasks with Mapper state
  friend class Job;
};

}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_OPS_JOB_TASKS_H_