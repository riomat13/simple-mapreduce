#include "simplemapreduce/data/queue.h"

#include <string>

namespace mapreduce {
namespace data {

  template <>
  void MessageQueue<std::string, int>::end()
  {
    std::lock_guard<std::mutex> lock{mq_mutex_};

    /// Send empty data to tell the end of the process
    /// by sending default values
    queue_.emplace_back("", 0);
    cond_.notify_one();
  }

  template <>
  void MessageQueue<std::string, long>::end()
  {
    std::lock_guard<std::mutex> lock{mq_mutex_};

    /// Send empty data to tell the end of the process
    /// by sending default values
    queue_.emplace_back("", 0);
    cond_.notify_one();
  }

  template <>
  void MessageQueue<std::string, float>::end()
  {
    std::lock_guard<std::mutex> lock{mq_mutex_};

    /// Send empty data to tell the end of the process
    /// by sending default values
    queue_.emplace_back("", 0);
    cond_.notify_one();
  }

  template <>
  void MessageQueue<std::string, double>::end()
  {
    std::lock_guard<std::mutex> lock{mq_mutex_};

    /// Send empty data to tell the end of the process
    /// by sending default values
    queue_.emplace_back("", 0);
    cond_.notify_one();
  }

} // namespace data
} // namespace mapreduce