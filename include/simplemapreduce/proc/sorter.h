#ifndef SIMPLEMAPREDUCE_PROC_SORTER_H_
#define SIMPLEMAPREDUCE_PROC_SORTER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/proc/loader.h"
#include "simplemapreduce/ops/conf.h"

namespace mapreduce {
namespace proc {

  /**
   * Sort operating class
   */
  template <typename K, typename V>
  class Sorter
  {
   public:
    typedef std::map<K, std::vector<V>> KVMap;

    /**
     * Constructor of Sorter class.
     * This is grouping items by keywords and pass to reducer.
     * 
     *  @param loader DataLoader unique pointer
     */
    Sorter(std::unique_ptr<DataLoader<K, V>> loader) : loader_(std::move(loader)) {}

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
    std::unique_ptr<DataLoader<K, V>> loader_;
  };

} // namespace proc
} // namespace mapreduce

#include "simplemapreduce/proc/sorter.tcc"

#endif