#include <string>
#include <utility>

namespace mapreduce {
namespace proc {

  template <typename K, typename V>
  void MQWriter<K, V>::write(std::string &key, int &value)
  {
    mq_->send(std::make_pair<std::string, int>(std::move(key), std::move(value)));
  }

  template <typename K, typename V>
  void MQWriter<K, V>::write(std::string &key, long &value)
  {
    mq_->send(std::make_pair<std::string, long>(std::move(key), std::move(value)));
  }

  template <typename K, typename V>
  void MQWriter<K, V>::write(std::string &key, float &value)
  {
    mq_->send(std::make_pair<std::string, long>(std::move(key), std::move(value)));
  }

  template <typename K, typename V>
  void MQWriter<K, V>::write(std::string &key, double &value)
  {
    mq_->send(std::make_pair<std::string, long>(std::move(key), std::move(value)));
  }

} // namespace proc
} // namespace mapreduce