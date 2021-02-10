#ifndef TESTS_UTILS_H_
#define TESTS_UTILS_H_

#include <algorithm>
#include <filesystem>
#include <map>
#include <vector>

#include "catch.hpp"

#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/proc/loader.h"

namespace fs = std::filesystem;

using namespace mapreduce::data;
using namespace mapreduce::proc;

/// Directory to store all temporary files for testing
extern fs::path tmpdir;

/**
 * Clear all content in a file with given path.
 *
 *  @param path&  target file path to clear data
 */
void clear_file(const fs::path &path);

/**
 * Extract files in the given directory.
 * The files will be pushed back to the given vector.
 * This will return files which are true by `std::filesystem::is_regular_file`
 *
 *  @param path&        directory path to parse
 *  @param container&   vector to store extracted file paths
 */
void extract_files(const fs::path&, std::vector<fs::path> &);

/**
 * Compare map with key and values.
 * Assumed all values in each containes are the same.
 * Example: {"key1": {1, 2, 3}, "key2": {1, 2, 3}, ...}
 *
 *  @param input&   target map to evaluate
 *  @param keys&    vectors storing keys
 *  @param values&  vector of vectors storing values associated with keys
 *  @param bool     return true if all items are matched without considering order
 */
template <class K, class V>
bool check_map_items(std::map<K, std::vector<V>> &input,
                     std::vector<K> &keys,
                     std::vector<std::vector<V>> &values)
{
  /// Check map holds same number of the keywords
  if(input.size() != keys.size())
    return false;

  /// Compare each values in a keyword with mapped values
  for (size_t i = 0; i < keys.size(); ++i)
  {
    auto item = input.find(keys[i]);

    /// Check if two vectors containes the same items
    if (item->second.size() != values[i].size()
        || !std::is_permutation(item->second.begin(), item->second.end(), values[i].begin()))
      return false;
  }
  return true;
}

class TestDataLoader : public DataLoader
{
 public:
  TestDataLoader(std::vector<BytePair> &input) : kv_items_(std::move(input))
  {
  };

  BytePair get_item()
  {
    /// Return empty once consumed all elements
    if (kv_items_.empty())
      return std::make_pair(ByteData(), ByteData());

    /// Take item one by one
    BytePair item = std::move(kv_items_.back());
    kv_items_.pop_back();
    return item;
  }

 private:
  std::vector<BytePair> kv_items_;
};

#endif