#ifndef MR_REDUCER_H_
#define MR_REDUCER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "commons.h"
#include "ops/context.h"
#include "ops/job.h"

namespace mapreduce {

  // no longer uses OKey and OValue. Remove?
  template <typename /* Input key datatype    */ IKey,
            typename /* Input value datatype  */ IValue,
            typename /* Output key datatype   */ OKey,
            typename /* Output value datatype */ OValue>
  class Reducer
  {
   public:

    /**
     * Reducer function
     * 
     *  @param key      Input mapped key
     *  @param value[]  Input mapped value
     *  @param context& Context used for sending data
     */
    virtual void reduce(const IKey&, const std::vector<IValue>&, const Context&) = 0;

    /**
     * Run before executing reduce
     * Override this if need to configure. 
     */
    void setup(Context &context) {};

   private:

    /// Used to create tasks with Mapper state
    template <class M, class R> friend class Job;

    /**
     * Create a Context object for output.
     * 
     *  @param path& Output file path
     */
    std::unique_ptr<Context> create_context(const std::string &);
  };

} // namespace mapreduce

#include "reducer.tcc"

#endif