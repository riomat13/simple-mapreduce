#ifndef SIMPLEMAPREDUCE_REDUCER_H_
#define SIMPLEMAPREDUCE_REDUCER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"
#include "simplemapreduce/ops/job_tasks.h"

using namespace mapreduce::data;

namespace mapreduce {

template <typename /* Input key datatype    */ IKeyType,
          typename /* Input value datatype  */ IValueType,
          typename /* Output key datatype   */ OKeyType,
          typename /* Output value datatype */ OValueType>
class Reducer : public ReduceJob
{
 public:

  /**
   * Reducer function
   *
   *  @param key      Input mapped key
   *  @param value[]  Input mapped value
   *  @param context& Context used for sending data
   */
  virtual void reduce(const IKeyType&, const std::vector<IValueType>&, const Context<OKeyType, OValueType>&) = 0;

  /**
   * Run before executing reduce.
   * Override this if need to configure.
   */
  void setup(Context<OKeyType, OValueType>&) {};

 private:
  /// Used to create tasks with Mapper state
  friend class Job;

  /** Run main reduce task. */
  void run() override;

  /**
   * Run reduce task.
   *
   *  @param outpath&   Output directory path
   */
  inline void run_(const fs::path&);

  /**
   * Run reduce task for combiner.
   *
   *  @param mq     MessageQueue to store output data
   */
  inline void run_(std::shared_ptr<MessageQueue>);

  /**
   * Create output data writer.
   *
   *  @param path   output file path
   */
  std::unique_ptr<Context<IKeyType, IValueType>> get_context(const std::string& path)
  {
    std::unique_ptr<OutputWriter<IKeyType, IValueType>> writer = std::make_unique<OutputWriter<IKeyType, IValueType>>(path);
    return std::make_unique<Context<IKeyType, IValueType>>(std::move(writer));
  }

  /**
   * Create output data writer for combiner.
   *
   *  @param mq     MessageQueue to store output data
   */
  std::unique_ptr<Context<IKeyType, IValueType>> get_context(std::shared_ptr<MessageQueue> mq)
  {
    std::unique_ptr<MQWriter> writer = std::make_unique<MQWriter>(mq);
    return std::make_unique<Context<IKeyType, IValueType>>(std::move(writer));
  }

  /** Get const Sorter instance. */
  std::unique_ptr<Sorter<IKeyType, IValueType>> get_sorter()
  {
    std::unique_ptr<DataLoader> loader = std::make_unique<BinaryFileDataLoader<IKeyType, IValueType>>(this->conf_);
    return std::make_unique<Sorter<IKeyType, IValueType>>(std::move(loader));
  }

  /**
   * Get const Sorter instance for combiner.
   *
   *  @param mq     MessageQueue created at Mapper task or the previous Combiner/Reducer task
   */
  std::unique_ptr<Sorter<IKeyType, IValueType>> get_sorter(std::shared_ptr<MessageQueue> mq)
  {
    std::unique_ptr<DataLoader> loader = std::make_unique<MQDataLoader>(mq);
    return std::make_unique<Sorter<IKeyType, IValueType>>(std::move(loader));
  }

  /// MessageQueue to store data
  std::shared_ptr<MessageQueue> mq_ = nullptr;
};

} // namespace mapreduce

#include "simplemapreduce/reducer.tcc"

#endif  // SIMPLEMAPREDUCE_REDUCER_H_