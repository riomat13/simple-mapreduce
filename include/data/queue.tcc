namespace mapreduce {
namespace data {

  template <typename K, typename V>
  MessageQueue<K, V>::MessageQueue(const MessageQueue<K, V> &rhs)
  {
    this->queue_ = std::move(rhs.queue_);
  }

  template <typename K, typename V>
  std::pair<K, V> MessageQueue<K, V>::receive()
  {
    std::unique_lock<std::mutex> lock{mq_mutex_};
    cond_.wait(lock, [this] { return !queue_.empty(); });

    std::pair<K, V> data = std::move(queue_.front());
    queue_.pop_front();

    return data;
  }

  template <typename K, typename V>
  void MessageQueue<K, V>::send(std::pair<K, V> &&data)
  {
    std::lock_guard<std::mutex> lock{mq_mutex_};

    /// Data is stored with format encoded by context
    queue_.push_back(std::move(data));
    cond_.notify_one();
  }

} // namespace data
} // namespace mapreduce