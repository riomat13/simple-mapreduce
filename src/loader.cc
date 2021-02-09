#include "simplemapreduce/proc/loader.h"

namespace mapreduce {
namespace proc {

BytePair MQDataLoader::get_item()
{
  /// Fetch data from MessageQueue
  /// A key of last element will be empty.
  return mq_->receive();
}

} // namespace proc
} // namespace mapreduce