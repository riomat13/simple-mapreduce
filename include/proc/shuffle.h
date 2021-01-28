#ifndef PROC_SHUFFLE_H_
#define PROC_SHUFFLE_H_

#include <memory>
#include <string>
#include <vector>

#include "commons.h"
#include "data/queue.h"
#include "ops/conf.h"
#include "ops/context.h"
#include "ops/job.h"
#include "proc/writer.h"

using namespace mapreduce::data;

namespace mapreduce {
namespace proc {

  /**
   * Shuffle process handler object.
   */
  template <typename K, typename V>
  class Shuffle
  {
   public:
    /**
     * Shuffle constructor
     * 
     *  @param MessageQueue<K, V>* Shared data cotainer to process
     *  @param const string&       Directory path to store intermediate state files
     *  @param const JobConf&      Configuration set in Job class
     */
    Shuffle(std::shared_ptr<MessageQueue<K, V>>, const std::string&, const JobConf&);
    ~Shuffle() {};

    /// Not use for copy/move and to avoid accidentaly pass objects
    Shuffle(const Shuffle &rhs) = delete;
    Shuffle &operator=(const Shuffle &rhs) = delete;
    Shuffle(Shuffle &&rhs) = delete;
    Shuffle &operator=(Shuffle &&rhs) = delete;

    /**
     * Run the shuffle process.
     * The data is taken from MessageQueue,
     * then shuffle and write to intermediate file.
     */
    void run();

   private:
    /// Job configuration
    JobConf conf_;

    /// Hash function to group the intermediate states
    int hash(const std::string&);

    /// Message Queue to get data to process
    std::shared_ptr<MessageQueue<K, V>> mq_ = nullptr;

    /// BinaryFileWriter for each grouping after shuffled
    std::vector<std::unique_ptr<BinaryFileWriter>> fouts_;
  };

} // namespace proc
} // namespace mapreduce

#include "proc/shuffle.tcc"

#endif