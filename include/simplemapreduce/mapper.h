#ifndef SIMPLEMAPREDUCE_MAPPER_H_
#define SIMPLEMAPREDUCE_MAPPER_H_

#include <memory>
#include <string>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/base/job_tasks.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"

namespace mapreduce {

template <typename /* Input key data type */    IKeyType,
          typename /* input value data type */  IValueType,
          typename /* Output key data type */   OKeyType,
          typename /* Output value data type */ OValueType>
class Mapper : public mapreduce::base::MapTask
{
 public:
  /**
   * Main Map function.
   *
   *  @param key      Key data. If this is the first mapper, the input is file content
   *  @param value    Input value data. If this is the first mapper, the value is 1.
   *  @param context  Output data writer
   */
  virtual void map(const IKeyType&, const IValueType&, const Context<OKeyType, OValueType>&) = 0;

  /**
   * Run before executing mapper.
   * Override this if need to configure.
   */
  void setup(mapreduce::Context<OKeyType, OValueType>&) {};

 private:
  /// Used to create tasks with Mapper state
  friend class Job;

  /**
   * Get const Context data writer.
   */
  std::unique_ptr<mapreduce::Context<OKeyType, OValueType>> get_context()
  {
    std::unique_ptr<mapreduce::proc::MQWriter> writer = std::make_unique<mapreduce::proc::MQWriter>(get_mq());
    return std::make_unique<mapreduce::Context<OKeyType, OValueType>>(std::move(writer));
  }

  /**
   * Get const Shuffle instance.
   */
  std::unique_ptr<mapreduce::proc::ShuffleJob> get_shuffle() override
  {
    std::unique_ptr<mapreduce::proc::ShuffleJob> shuffle = std::make_unique<mapreduce::proc::Shuffle<OKeyType, OValueType>>(get_mq(), conf_);
    return shuffle;
  };

  /**
   * Run map task
   *
   *  @param key    Mapper input key data
   *  @param value  Mapper input value data
   */
  void run(mapreduce::data::ByteData&, mapreduce::data::ByteData&);
};

}  // namespace mapreduce

#include "simplemapreduce/mapper.tcc"

#endif  // SIMPLEMAPREDUCE_MAPPER_H_