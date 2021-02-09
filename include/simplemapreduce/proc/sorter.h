#ifndef SIMPLEMAPREDUCE_PROC_SORTER_H_
#define SIMPLEMAPREDUCE_PROC_SORTER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/proc/loader.h"
#include "simplemapreduce/ops/conf.h"

using namespace mapreduce::data;
namespace mapreduce {
namespace proc {

/**
 * Sort operating class
 */
class Sorter
{
 public:
  typedef std::map<ByteData, std::vector<ByteData>> KVMap;

  /**
   * Constructor of Sorter class.
   * This is grouping items by keywords and pass to reducer.
   * 
   *  @param loader DataLoader unique pointer
   */
  Sorter(std::unique_ptr<DataLoader> loader) : loader_(std::move(loader)) {}

  /**
   * Execute sorting.
   * Each execution handles each file groped ID.
   * This will create a new map with values grouped by the given key.
   * 
   *  @return map<K, std::vector<V>>&  map of vectors grouped by sorting process
   */
  KVMap run();

 private:

  /// File data loader
  std::unique_ptr<DataLoader> loader_;
};

} // namespace proc
} // namespace mapreduce

#endif