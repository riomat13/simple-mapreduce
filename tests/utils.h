#ifndef TESTS_UTILS_H_
#define TESTS_UTILS_H_

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <type_traits>
#include <vector>

#include "catch.hpp"

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/type.h"
#include "simplemapreduce/proc/loader.h"

/// This is test use only so add aliases
using namespace mapreduce::data;
using namespace mapreduce::type;

/// Directory to store all temporary files for testing
extern std::filesystem::path tmpdir;

/**
 * Clear all content in a file with given path.
 *
 *  @param path  target file path to clear data
 */
void clear_file(const std::filesystem::path& path);

/**
 * Extract files in the given directory.
 * The files will be pushed back to the given vector.
 * This will return files which are true by `std::filesystem::is_regular_file`
 *
 *  @param path        directory path to parse
 *  @param container   vector to store extracted file paths
 */
void extract_files(const std::filesystem::path&, std::vector<std::filesystem::path>&);

/**
 * Compare map with key and values.
 * Assumed all values in each containes are the same.
 * Example: {"key1": {1, 2, 3}, "key2": {1, 2, 3}, ...}
 *
 *  @param input   target map to evaluate
 *  @param keys    vectors storing keys
 *  @param values  vector of vectors storing values associated with keys
 *  @param bool     return true if all items are matched without considering order
 */
template <class K, class V>
bool check_map_items(std::map<K, std::vector<V>>& input,
                     std::vector<K>& keys,
                     std::vector<std::vector<V>>& values) {
  /// Check map holds same number of the keywords
  if(input.size() != keys.size())
    return false;

  /// Compare each values in a keyword with mapped values
  for (size_t i = 0; i < keys.size(); ++i) {
    auto item = input.find(keys[i]);

    /// Check if two vectors containes the same items
    if (item->second.size() != values[i].size()
        || !std::is_permutation(item->second.begin(), item->second.end(), values[i].begin())) {
      return false;
    }
  }
  return true;
}

/**
 * Read binary data and return as actual data type.
 *
 *  @param ifs  input file stream to read data
 */
template <typename T>
ByteData read_binary(std::ifstream& ifs) {
  /// Read numeric data
  ByteData bdata;
  if (std::is_arithmetic_v<T>) {
    char buffer[sizeof(T)];
    ifs.read(buffer, sizeof(T));

    /// Break if reachs EOF
    if (ifs.eof())
      return bdata;

    bdata.set_bytes<T>(buffer, sizeof(T));
  } else {
    /// Read String or CompositeKey
    Size_t data_size;
    ifs.read(reinterpret_cast<char*>(&data_size), sizeof(Size_t));

    /// Break if reachs EOF
    if (ifs.eof())
      return bdata;

    char buffer[data_size];
    ifs.read(buffer, data_size);
    bdata.set_bytes<T>(buffer, data_size);
  }
  return bdata;
}

class TestDataLoader : public mapreduce::proc::DataLoader {
 public:
  TestDataLoader(std::vector<mapreduce::data::BytePair>& input) : kv_items_(std::move(input)) {};

  mapreduce::data::BytePair get_item() {
    /// Return empty once consumed all elements
    if (kv_items_.empty())
      return std::make_pair(mapreduce::data::ByteData(), mapreduce::data::ByteData());

    /// Take item one by one
    mapreduce::data::BytePair item = std::move(kv_items_.back());
    kv_items_.pop_back();
    return item;
  }

 private:
  std::vector<mapreduce::data::BytePair> kv_items_;
};

#endif