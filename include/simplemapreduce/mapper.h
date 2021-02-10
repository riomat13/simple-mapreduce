#ifndef SIMPLEMAPREDUCE_MAPPER_H_
#define SIMPLEMAPREDUCE_MAPPER_H_

#include <filesystem>
#include <memory>
#include <string>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"
#include "simplemapreduce/ops/job_tasks.h"

namespace fs = std::filesystem;

using namespace mapreduce::data;
using namespace mapreduce::proc;

namespace mapreduce {

template <typename /* Input key data type */    IKeyType,
          typename /* input value data type */  IValueType,
          typename /* Output key data type */   OKeyType,
          typename /* Output value data type */ OValueType>
class Mapper : private MapperJob<OKeyType, OValueType>
{
 public:
  /**
   * Mapper function
   *
   *  @param key&      Key data. If this is the first mapper, the input is file content
   *  @param value&    Input value data. If this is the first mapper, the value is 1.
   *  @param context&  Output data writer
   */
  virtual void map(const IKeyType&, const IValueType&, const Context<OKeyType, OValueType>&) = 0;

  /**
   * Run before executing mapper.
   * Override this if need to configure.
   */
  void setup(Context<OKeyType, OValueType>&context) {};

 private:
  /// Used to create tasks with Mapper state
  template <class M, class R> friend class Job;

  /**
   * Run map task
   *
   *  @param key&    Mapper input key data
   *  @param value&  Mapper input value data
   */
  void run(ByteData &key, ByteData &value);
};

} // namespace mapreduce

#include "simplemapreduce/mapper.tcc"

#endif