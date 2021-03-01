#include "simplemapreduce/proc/sorter.h"

namespace mapreduce {
namespace proc {

template <typename K, typename V>
std::unique_ptr<std::map<K, std::vector<V>>> Sorter<K, V>::run() {
  if (container_ == nullptr) {
    container_ = std::make_unique<std::map<K, std::vector<V>>>();
  }

  auto data(loader_->get_item());

  /// store values to vector of the associated key in map
  while (!data.first.empty()) {
    (*container_)[data.first.get_data<K>()].push_back(data.second.get_data<V>());
    data = std::move(loader_->get_item());
  }

  return std::move(container_);
}

}  // namespace proc
}  // namespace mapreduce