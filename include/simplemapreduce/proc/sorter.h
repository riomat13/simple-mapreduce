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
namespace mapreduce {
namespace proc {

/**
 * Sort operating class
 */
template <typename K, typename V>
class Sorter {
 public:
  /**
   * Constructor of Sorter class.
   * This is grouping items by keywords and pass to reducer.
   * 
   *  @param loader DataLoader unique pointer
   */
  Sorter(std::unique_ptr<mapreduce::proc::DataLoader> loader) : loader_(std::move(loader)) {}

  /**
   * Execute sorting.
   * Each execution handles each file groped ID.
   * This will create a new map with values grouped by the given key.
   * 
   *  @return   map of vectors grouped by sorting process
   */
  std::map<K, std::vector<V>> run();

 private:

  /// File data loader
  std::unique_ptr<mapreduce::proc::DataLoader> loader_;
};

}  // namespace proc
}  // namespace mapreduce

#include "simplemapreduce/proc/sorter-inl.h"

#endif  // SIMPLEMAPREDUCE_PROC_SORTER_H_