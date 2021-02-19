#include "simplemapreduce/proc/sorter.h"

namespace mapreduce {
namespace proc {

template <typename K, typename V>
std::map<K, std::vector<V>> Sorter<K, V>::run() {
  std::map<K, std::vector<V>> container;
  auto data(loader_->get_item());

  /// store values to vector of the associated key in map
  while (!data.first.empty()) {
    container[data.first.get_data<K>()].push_back(data.second.get_data<V>());
    data = std::move(loader_->get_item());
  }

  return container;
}

}  // namespace proc
}  // namespace mapreduce