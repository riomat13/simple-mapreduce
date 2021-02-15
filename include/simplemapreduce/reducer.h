#ifndef SIMPLEMAPREDUCE_REDUCER_H_
#define SIMPLEMAPREDUCE_REDUCER_H_

#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/base/job_tasks.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"

namespace mapreduce {

template <typename /* Input key datatype    */ IKeyType,
          typename /* Input value datatype  */ IValueType,
          typename /* Output key datatype   */ OKeyType,
          typename /* Output value datatype */ OValueType>
class Reducer : public mapreduce::base::ReduceTask
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
  void setup(mapreduce::Context<OKeyType, OValueType>&) {};

 private:
  /// Used to create tasks with Mapper state
  friend class mapreduce::Job;

  /** Run main reduce task. */
  void run() override;

  /**
   * Run reduce task.
   *
   *  @param outpath&   Output directory path
   */
  inline void run_(const std::filesystem::path&);

  /**
   * Run reduce task for combiner.
   *
   *  @param mq     MessageQueue to store output data
   */
  inline void run_(std::shared_ptr<mapreduce::data::MessageQueue>);

  /**
   * Set a MessageQueue object for combiner.
   * If this is set, the Reducer will be seen as Combiner.
   */
  void set_mq(std::shared_ptr<mapreduce::data::MessageQueue> mq) override { mq_ = mq; };

  /**
   * Create output data writer.
   *
   *  @param path   output file path
   */
  std::unique_ptr<mapreduce::Context<IKeyType, IValueType>> get_context(const std::string& path)
  {
    std::unique_ptr<mapreduce::proc::OutputWriter<IKeyType, IValueType>> writer = std::make_unique<mapreduce::proc::OutputWriter<IKeyType, IValueType>>(path);
    return std::make_unique<mapreduce::Context<IKeyType, IValueType>>(std::move(writer));
  }

  /**
   * Create output data writer for combiner.
   *
   *  @param mq     MessageQueue to store output data
   */
  std::unique_ptr<mapreduce::Context<IKeyType, IValueType>> get_context(std::shared_ptr<mapreduce::data::MessageQueue> mq)
  {
    std::unique_ptr<mapreduce::proc::MQWriter> writer = std::make_unique<mapreduce::proc::MQWriter>(mq);
    return std::make_unique<mapreduce::Context<IKeyType, IValueType>>(std::move(writer));
  }

  /** Get const Sorter instance. */
  std::unique_ptr<mapreduce::proc::Sorter<IKeyType, IValueType>> get_sorter()
  {
    std::unique_ptr<mapreduce::proc::DataLoader> loader = std::make_unique<mapreduce::proc::BinaryFileDataLoader<IKeyType, IValueType>>(this->conf_);
    return std::make_unique<mapreduce::proc::Sorter<IKeyType, IValueType>>(std::move(loader));
  }

  /**
   * Get const Sorter instance for combiner.
   *
   *  @param mq     MessageQueue created at Mapper task or the previous Combiner/Reducer task
   */
  std::unique_ptr<mapreduce::proc::Sorter<IKeyType, IValueType>> get_sorter(std::shared_ptr<mapreduce::data::MessageQueue> mq)
  {
    std::unique_ptr<mapreduce::proc::DataLoader> loader = std::make_unique<mapreduce::proc::MQDataLoader>(mq);
    return std::make_unique<mapreduce::proc::Sorter<IKeyType, IValueType>>(std::move(loader));
  }

  /// MessageQueue to store data
  std::shared_ptr<mapreduce::data::MessageQueue> mq_ = nullptr;
};

} // namespace mapreduce

#include "simplemapreduce/reducer.tcc"

#endif  // SIMPLEMAPREDUCE_REDUCER_H_