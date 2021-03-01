#ifndef SIMPLEMAPREDUCE_PROC_SHUFFLE_H_
#define SIMPLEMAPREDUCE_PROC_SHUFFLE_H_

#include <map>
#include <memory>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/proc/writer.h"

namespace mapreduce {
namespace proc {

class ShuffleTask {
 public:
  virtual ~ShuffleTask() {}

  /**
   * Run shuffle process.
   */
  virtual void run() = 0;
};

/**
 * Shuffle process handler object.
 */
template <typename K, typename V>
class Shuffle : public ShuffleTask {
 public:
  /**
   * Shuffle constructor
   * 
   *  @param mq     data cotainer to process
   *  @param conf   Configuration set in Job class
   */
  Shuffle(std::shared_ptr<mapreduce::data::MessageQueue>, std::shared_ptr<mapreduce::JobConf>);
  ~Shuffle() {};

  /// Not use for copy/move and to avoid accidentaly pass objects
  Shuffle &operator=(const Shuffle&) = delete;
  Shuffle(Shuffle&&) = delete;
  Shuffle &operator=(Shuffle&&) = delete;

  /**
   * Run the shuffle process.
   * The data is taken from MessageQueue,
   * then shuffle and write to intermediate file.
   */
  void run();

 private:
  /// Job configuration
  std::shared_ptr<mapreduce::JobConf> conf_;

  /// Hash function to group the intermediate states
  int hash(const K&);

  /// Message Queue to get data to process
  std::shared_ptr<mapreduce::data::MessageQueue> mq_ = nullptr;

  /// BinaryFileWriter for each grouping after shuffled
  std::vector<std::unique_ptr<mapreduce::proc::BinaryFileWriter<K, V>>> fouts_;
};

}  // namespace proc
}  // namespace mapreduce

#include "simplemapreduce/proc/shuffle-inl.h"

#endif  // SIMPLEMAPREDUCE_PROC_SHUFFLE_H_