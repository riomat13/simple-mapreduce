#ifndef SIMPLEMAPREDUCE_REDUCER_H_
#define SIMPLEMAPREDUCE_REDUCER_H_

#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/base/job_tasks.h"
#include "simplemapreduce/base/job_runner.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"

namespace mapreduce {

template <typename /* Input key datatype    */ IKeyType,
          typename /* Input value datatype  */ IValueType,
          typename /* Output key datatype   */ OKeyType,
          typename /* Output value datatype */ OValueType>
class Reducer : public mapreduce::base::ReduceTask {
 public:
  /**
   * Reducer function
   *
   *  @param key      Input mapped key
   *  @param value[]  Input mapped value
   *  @param context& Context used for sending data
   */
  virtual void reduce(const IKeyType&, const std::vector<IValueType>&, const Context<OKeyType, OValueType>&) = 0;

 private:
  /**
   * Set file path to write data and return it.
   * The file name will be defined based on worker rank.
   */
  std::filesystem::path get_output_filepath();

  /** Run main reduce task. */
  void run() override;

  /**
   * Run reduce task.
   *
   *  @param outpath&   Output directory path
   */
  inline void run_(const std::filesystem::path&);

  /** Run reduce task as combiner. */
  inline void run_();

  /**
   * Set a MessageQueue object for combiner.
   * If this is set, the Reducer will be seen as Combiner.
   */
  void set_mq(std::shared_ptr<mapreduce::data::MessageQueue> mq) override { mq_ = mq; };

  void as_combiner() override { is_combiner_ = true; }

  /**
   * Create output data writer.
   *
   *  @param path   output file path
   */
  std::unique_ptr<mapreduce::Context<OKeyType, OValueType>> get_context(const std::string&);

  /**
   * Create output data writer for combiner.
   *
   *  @param mq     MessageQueue to store output data
   */
  std::unique_ptr<mapreduce::Context<OKeyType, OValueType>> get_context(std::shared_ptr<mapreduce::data::MessageQueue>);

  /** Get const Sorter instance. */
  std::unique_ptr<mapreduce::proc::Sorter<IKeyType, IValueType>> get_sorter();

  /**
   * Get const Sorter instance for combiner.
   *
   *  @param mq     MessageQueue created at Mapper task or the previous Combiner/Reducer task
   */
  std::unique_ptr<mapreduce::proc::Sorter<IKeyType, IValueType>> get_sorter(std::shared_ptr<mapreduce::data::MessageQueue>);

  /// Container to store shuffled key/value data
  std::unique_ptr<std::map<IKeyType, std::vector<IValueType>>> container_ = nullptr;

  /// MessageQueue to store data
  std::shared_ptr<mapreduce::data::MessageQueue> mq_ = nullptr;

  bool is_combiner_{false};
};

} // namespace mapreduce

#include "simplemapreduce/reducer-inl.h"

#endif  // SIMPLEMAPREDUCE_REDUCER_H_