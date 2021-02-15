#ifndef SIMPLEMAPREDUCE_DATA_QUEUE_H_
#define SIMPLEMAPREDUCE_DATA_QUEUE_H_

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "simplemapreduce/data/bytes.h"

namespace mapreduce {
namespace data {

/**
 * FIFO queue to store key/value byte data.
 */
class MessageQueue
{
 public:
  MessageQueue() {};
  MessageQueue(const MessageQueue&);

  mapreduce::data::BytePair receive();

  /**
   * Send data to storage.
   * The format can be either
   *  - (ByteData, ByteData)
   *  - std::pair<ByteData, ByteData>
   */
  void send(mapreduce::data::ByteData&&, mapreduce::data::ByteData&&);
  void send(mapreduce::data::BytePair&&);

  /**
   * Signal as end of pushing new data.
   * This is simply pushing empty data pair to the end of the queue.
   */
  void end();

 private:
  std::deque<mapreduce::data::BytePair> queue_;
  std::mutex mq_mutex_;
  std::condition_variable cond_;
};

}  // namespace data
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_DATA_QUEUE_H_