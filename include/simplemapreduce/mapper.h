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
class Mapper : public mapreduce::base::MapTask {
 public:
  /**
   * Main Map function.
   *
   *  @param key      Key data. If this is the first mapper, the input is file content
   *  @param value    Input value data. If this is the first mapper, the value is 1.
   *  @param context  Output data writer
   */
  virtual void map(const IKeyType&, const IValueType&, const Context<OKeyType, OValueType>&) = 0;

 private:
  /**
   * Get const Context data writer.
   */
  std::unique_ptr<mapreduce::Context<OKeyType, OValueType>> get_context();

  /**
   * Get const Shuffle instance.
   */
  std::unique_ptr<mapreduce::proc::ShuffleTask> get_shuffle() override;

  /**
   * Run map task
   *
   *  @param key    Mapper input key data
   *  @param value  Mapper input value data
   */
  void run(mapreduce::data::ByteData&, mapreduce::data::ByteData&);
};

}  // namespace mapreduce

#include "simplemapreduce/mapper-inl.h"

#endif  // SIMPLEMAPREDUCE_MAPPER_H_