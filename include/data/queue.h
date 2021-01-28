#ifndef DATA_QUEUE_H_
#define DATA_QUEUE_H_

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace mapreduce {
namespace data {

  /**
   * FIFO queue to store shared text data
   */
  template <typename K, typename V>
  class MessageQueue
  {
   public:
    MessageQueue() {};
    MessageQueue(const MessageQueue<K, V> &);

    std::pair<K, V> receive();
    void send(std::pair<K, V>&&);
    void end();

   private:
    std::deque<std::pair<K, V>> queue_;
    std::mutex mq_mutex_;
    std::condition_variable cond_;
    unsigned int n_threads = std::thread::hardware_concurrency();

  };

} // namespace data
} // namespace mapreduce

#include "queue.tcc"

#endif