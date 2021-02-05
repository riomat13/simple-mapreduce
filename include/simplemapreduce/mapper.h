#ifndef SIMPLEMAPREDUCE_MAPPER_H_
#define SIMPLEMAPREDUCE_MAPPER_H_

#include <filesystem>
#include <memory>
#include <string>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"
#include "simplemapreduce/proc/shuffle.h"
#include "simplemapreduce/proc/sorter.h"

namespace fs = std::filesystem;

using namespace mapreduce::data;
using namespace mapreduce::proc;

namespace mapreduce {

  template <typename /* Input key data type */    IKeyType,
            typename /* input value data type */  IValueType,
            typename /* Output key data type */   OKeyType,
            typename /* Output value data type */ OValueType>
  class Mapper
  {
   public:

    typedef MessageQueue<OKeyType, OValueType> MQ;

    Mapper() { mq_ = std::make_shared<MQ>(MQ()); }
    ~Mapper() {}

    /**
     * Mapper function
     * 
     *  @param key&      Key data. If this is the first mapper, the input is file content
     *  @param value&    Input value data. If this is the first mapper, the value is 1.
     *  @param context&  Output data writer
     */
    virtual void map(const IKeyType&, const IValueType&, const Context &) = 0;

    /**
     * Run before executing mapper.
     * Override this if need to configure. 
     */
    void setup(Context &context) {};

   private:

    /// Used to create tasks with Mapper state
    template <class M, class R> friend class Job;

    /**
     * Create a MessageQueue object for mapper
     */
    std::shared_ptr<MQ> get_mq() { return mq_; };

    /**
     * Create a Context object for mapper
     */
    std::unique_ptr<Context> create_context();

    /**
     * Create a Shuffle object for mapper
     * 
     * Provide directory path to store intermediate data files
     * by either following two formats.
     * 
     *  @param fs::path&
     *  @param std::string&
     */
    std::unique_ptr<Shuffle<OKeyType, OValueType>> create_shuffler(fs::path&, JobConf&);
    std::unique_ptr<Shuffle<OKeyType, OValueType>> create_shuffler(std::string&, JobConf&);

    /**
     * Create a Sorter object for mapper
     * This will sort shuffled data and pass them to reducer.
     * 
     * Provide directory path which stores intermediate data files
     * by either following two formats.
     * 
     *  @param fs::path&
     *  @param std::string&
     */
    std::unique_ptr<Sorter<OKeyType, OValueType>> create_sorter(fs::path&, JobConf&);
    std::unique_ptr<Sorter<OKeyType, OValueType>> create_sorter(std::string&, JobConf&);

    /// MessageQueue to store data processed by mapper
    std::shared_ptr<MQ> mq_ = nullptr;

  };

} // namespace mapreduce

#include "simplemapreduce/mapper.tcc"

#endif