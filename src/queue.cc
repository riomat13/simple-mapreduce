#include "simplemapreduce/data/queue.h"

#include <string>

namespace mapreduce {
namespace data {

MessageQueue::MessageQueue(const MessageQueue &rhs)
{
  this->queue_ = std::move(rhs.queue_);
}

BytePair MessageQueue::receive()
{
  std::unique_lock<std::mutex> lock{mq_mutex_};
  cond_.wait(lock, [this] { return !queue_.empty(); });

  BytePair data = std::move(queue_.front());
  queue_.pop_front();

  return data;
}

void MessageQueue::send(BytePair &&data)
{
  std::lock_guard<std::mutex> lock{mq_mutex_};

  /// Data is stored with format encoded by context
  queue_.push_back(std::move(data));
  cond_.notify_one();
}

void MessageQueue::end()
{
  std::lock_guard<std::mutex> lock{mq_mutex_};

  /// Send empty data to tell the end of the process
  /// by sending default values
  queue_.emplace_back(ByteData(), ByteData());
  cond_.notify_one();
}

} // namespace data
} // namespace mapreduce