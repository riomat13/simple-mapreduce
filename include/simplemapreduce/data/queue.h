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
 * FIFO queue to store shared text data
 */
class MessageQueue
{
 public:
  MessageQueue() {};
  MessageQueue(const MessageQueue &);

  BytePair receive();
  void send(BytePair&&);
  void end();

 private:
  std::deque<BytePair> queue_;
  std::mutex mq_mutex_;
  std::condition_variable cond_;
};

} // namespace data
} // namespace mapreduce

#endif