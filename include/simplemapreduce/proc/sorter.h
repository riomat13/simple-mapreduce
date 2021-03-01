#ifndef SIMPLEMAPREDUCE_PROC_SORTER_H_
#define SIMPLEMAPREDUCE_PROC_SORTER_H_

#include <map>
#include <memory>
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
  std::unique_ptr<std::map<K, std::vector<V>>> run();

  /**
   * Set initial container to be used for sorting task.
   * If this is not run, construct new map in run().
   */
  void set_container(std::unique_ptr<std::map<K, std::vector<V>>> container) { container_ = std::move(container); }

 private:
  /// Sorted item container
  std::unique_ptr<std::map<K, std::vector<V>>> container_ = nullptr;

  /// File data loader
  std::unique_ptr<mapreduce::proc::DataLoader> loader_;
};

}  // namespace proc
}  // namespace mapreduce

#include "simplemapreduce/proc/sorter-inl.h"

#endif  // SIMPLEMAPREDUCE_PROC_SORTER_H_