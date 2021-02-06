#ifndef SIMPLEMAPREDUCE_REDUCER_H_
#define SIMPLEMAPREDUCE_REDUCER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"
#include "simplemapreduce/ops/job_tasks.h"

namespace mapreduce {

  // no longer uses OKey and OValue. Remove?
  template <typename /* Input key datatype    */ IKeyType,
            typename /* Input value datatype  */ IValueType,
            typename /* Output key datatype   */ OKeyType,
            typename /* Output value datatype */ OValueType>
  class Reducer : private ReduceJob<IKeyType, IValueType>
  {
   public:

    /**
     * Reducer function
     * 
     *  @param key      Input mapped key
     *  @param value[]  Input mapped value
     *  @param context& Context used for sending data
     */
    virtual void reduce(const IKeyType&, const std::vector<IValueType>&, const Context&) = 0;

    /**
     * Run before executing reduce
     * Override this if need to configure. 
     */
    void setup(Context &context) {};

   private:
    /// Used to create tasks with Mapper state
    template <class M, class R> friend class Job;
  };

} // namespace mapreduce

#endif