#include "simplemapreduce/util/validator.h"

using namespace mapreduce::util;

namespace mapreduce {
namespace proc {

  template <typename K, typename V>
  std::map<K, std::vector<V>> Sorter<K, V>::run()
  {
    KVMap container;
    auto data = loader_->get_item();

    /// store values to vector of the associated key in map
    while (is_valid_data<K>(data.first))
    {
      container[data.first].push_back(data.second);
      data = loader_->get_item();
    }

    return container;
  }

} // namespace proc
} // namespace mapreduce