#ifndef SIMPLEMAPREDUCE_PROC_SHUFFLE_H_
#define SIMPLEMAPREDUCE_PROC_SHUFFLE_H_

#include <memory>
#include <string>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/job.h"
#include "simplemapreduce/proc/writer.h"

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
     *  @param const JobConf&      Configuration set in Job class
     */
    Shuffle(std::shared_ptr<MessageQueue<K, V>>, const JobConf&);
    ~Shuffle() {};

    /// Not use for copy/move and to avoid accidentaly pass objects
    Shuffle &operator=(const Shuffle &) = delete;
    Shuffle(Shuffle &&) = delete;
    Shuffle &operator=(Shuffle &&) = delete;

    /**
     * Run the shuffle process.
     * The data is taken from MessageQueue,
     * then shuffle and write to intermediate file.
     */
    void run();

   private:
    /// Job configuration
    const JobConf &conf_;

    /// Hash function to group the intermediate states
    int hash(const std::string&);

    /// Message Queue to get data to process
    std::shared_ptr<MessageQueue<K, V>> mq_ = nullptr;

    /// BinaryFileWriter for each grouping after shuffled
    std::vector<std::unique_ptr<BinaryFileWriter<K, V>>> fouts_;
  };

} // namespace proc
} // namespace mapreduce

#include "simplemapreduce/proc/shuffle.tcc"

#endif