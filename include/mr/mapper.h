#ifndef MR_MAPPER_H_
#define MR_MAPPER_H_

#include <filesystem>
#include <memory>
#include <string>

#include "commons.h"
#include "data/queue.h"
#include "ops/conf.h"
#include "ops/context.h"
#include "ops/job.h"
#include "proc/shuffle.h"
#include "proc/sorter.h"

namespace fs = std::filesystem;

using namespace mapreduce::data;
using namespace mapreduce::proc;

namespace mapreduce {

  template <typename /* Output key data type */   K,
            typename /* Output value data type */ V>
  class Mapper
  {
   public:

    typedef MessageQueue<K, V> MQ;

    Mapper() { mq_ = std::make_shared<MQ>(MQ()); }
    ~Mapper() {}

    /**
     * Mapper function
     * 
     *  @param input
     */
    virtual void map(const std::string &input, const Context &context) = 0;

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
    std::unique_ptr<Shuffle<K, V>> create_shuffler(fs::path&, JobConf&);
    std::unique_ptr<Shuffle<K, V>> create_shuffler(std::string&, JobConf&);

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
    std::unique_ptr<Sorter<K, V>> create_sorter(fs::path&, JobConf&);
    std::unique_ptr<Sorter<K, V>> create_sorter(std::string&, JobConf&);

    /// MessageQueue to store data processed by mapper
    std::shared_ptr<MQ> mq_ = nullptr;

  };

} // namespace mapreduce

#include "mr/mapper.tcc"

#endif