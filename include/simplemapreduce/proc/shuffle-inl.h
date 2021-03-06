#include <filesystem>
#include <functional>
#include <iomanip>
#include <sstream>

#include "simplemapreduce/util/log.h"
using namespace mapreduce::util;

namespace mapreduce {
namespace proc {

template <typename K, typename V>
Shuffle<K, V>::Shuffle(std::shared_ptr<mapreduce::data::MessageQueue> mq, std::shared_ptr<mapreduce::JobConf> conf)
    : conf_(conf), mq_(std::move(mq)) {
  std::ostringstream oss_rank;
  oss_rank << std::setw(4) << std::setfill('0') << conf_->worker_rank;

  for (int i = 0; i < conf_->n_groups; ++i) {
    /// Create each file path to store intermediate states
    std::ostringstream oss_id;
    oss_id << std::setw(5) << std::setfill('0') << i;
    std::filesystem::path filename = oss_rank.str() + "-" + oss_id.str();

    /// Set writer with the file defined above
    fouts_.push_back(std::make_unique<mapreduce::proc::BinaryFileWriter<K, V>>((conf_->tmpdir / filename).string()));
  }
}

template <typename K, typename V>
int Shuffle<K, V>::hash(const mapreduce::type::String& data) {
  return std::hash<std::string>{}(data) % conf_->n_groups;
}

template <typename K, typename V>
void Shuffle<K, V>::run() {
  /// Get initial item and define the data type
  auto data = mq_->receive();

  /// Run until all processed and receive empty when finished the process
  while (!data.first.empty()) {
    int id = hash(data.first.get_key());
    if (id == conf_->worker_rank) {
      /// data processed on the same worker node at reduce will be stored back to MessageQueue
      /// and retrieve it later
      mq_->send(std::move(data));
    } else {
      fouts_[id]->write(std::move(data.first), std::move(data.second));
    }

    /// Get new one after processed
    data = mq_->receive();
  }

  mq_->end();
}

}  // namespace proc
}  // namespace mapreduce