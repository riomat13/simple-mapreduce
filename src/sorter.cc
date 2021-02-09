#include "simplemapreduce/proc/sorter.h"

namespace mapreduce {
namespace proc {

std::map<ByteData, std::vector<ByteData>> Sorter::run()
{
  KVMap container;
  auto data = loader_->get_item();

  /// store values to vector of the associated key in map
  while (!data.first.empty())
  {
    container[data.first].push_back(data.second);
    data = loader_->get_item();
  }

  return container;
}

} // namespace proc
} // namespace mapreduce