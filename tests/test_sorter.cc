#include "simplemapreduce/proc/sorter.h"

#include <algorithm>
#include <cassert>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <utility>

#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/data/type.h"
#include "simplemapreduce/proc/loader.h"

using namespace mapreduce::data;
using namespace mapreduce::proc;
using namespace mapreduce::type;

template <typename K, typename V>
void test_sorter(std::vector<K>& keys, std::vector<std::vector<V>>& values) {
  /// Create input value key-value pairs
  std::vector<BytePair> inputs;
  for (unsigned int i = 0; i < keys.size(); ++i) {
    for (auto& val: values[i])
      inputs.emplace_back(ByteData{K{keys[i]}}, ByteData{V{val}});
  }

  std::unique_ptr<DataLoader> loader =
      std::make_unique<TestDataLoader>(inputs);

  /// Run sort task and group by the keys
  Sorter<K, V> sorter(std::move(loader));
  auto out = sorter.run();

  std::map<K, std::vector<V>> res;
  for (auto it = out->begin(); it != out->end(); ++it)
    res.insert(std::pair<K, std::vector<V>>(it->first, it->second));

  REQUIRE(check_map_items(res, keys, values));
}

TEST_CASE("Sorter", "[sorter]") {

  SECTION("String/Int") {
    std::vector<String> keys{"test", "example", "sort"};
    std::vector<std::vector<Int>> values{
      {1, -1, 5, 123, -6092},
      {4500, -53, 22},
      {-9, -444, -10207}
    };

    test_sorter<String, Int>(keys, values);
  }

  SECTION("Int/Long") {
    std::vector<Int> keys{1, 10, 100};
    std::vector<std::vector<Long>> values{
      {1234567890, -987654321, 543209},
      {301948990, -10240, 2},
      {-1000, 403982117, -5}
    };

    test_sorter<Int, Long>(keys, values);
  }

  SECTION("String/Double") {
    std::vector<String> keys{"test", "example", "sort"};
    std::vector<std::vector<Double>> values{
      {-1.23456789012345, 94504.3300856, 217.8123001115},
      {-40876.401839942, 3901.3044313, -5.091, 625504.126981},
      {4222.9801726, 555.6600123985}
    };

    test_sorter<String, Double>(keys, values);
  }
}

template <typename K, typename V>
void test_sorter_with_initial_data(std::vector<K>& keys,
                                   std::vector<std::vector<V>>& values,
                                   std::map<K, std::vector<V>>& init_data) {
  /// Create input value key-value pairs
  std::vector<BytePair> inputs;
  for (unsigned int i = 0; i < keys.size(); ++i) {
    for (auto& val: values[i])
      inputs.emplace_back(ByteData{K{keys[i]}}, ByteData{V{val}});
  }

  /// Pass initial map data to target results for comparison
  for (const auto& [key, vals]: init_data) {
    auto it = std::find(keys.begin(), keys.end(), key);
    if (it == keys.end()) {
      /// if key is completely new, add the key and values
      keys.push_back(key);
      values.push_back(vals);
    } else {
      /// if key is in vector, extend the associated value list
      auto idx = it - keys.begin();
      for (auto& val: vals) {
        values[idx].push_back(val);
      }
    }
  }

  std::unique_ptr<DataLoader> loader =
      std::make_unique<TestDataLoader>(inputs);

  /// Run sort task and group by the keys
  Sorter<K, V> sorter(std::move(loader));
  sorter.set_container(std::make_unique<std::map<K, std::vector<V>>>(std::move(init_data)));
  auto out = sorter.run();

  std::map<K, std::vector<V>> res;
  for (auto it = out->begin(); it != out->end(); ++it)
    res.insert(std::pair<K, std::vector<V>>(it->first, it->second));

  REQUIRE(check_map_items(res, keys, values));
}

TEST_CASE("Sorter with initial data", "[sorter]") {

  SECTION("String/Int") {
    std::vector<String> keys{"test", "example", "sort"};
    std::vector<std::vector<Int>> values{
      {1, -1, 5, 123, -6092},
      {4500, -53, 22},
      {-9, -444, -10207}
    };

    std::map<String, std::vector<Int>> init_data{
      {"sort", {1, 2, 10, 20}},
      {"mapreduce", {1, 2, 10, 20}},
      {"test", {1, 2, 10, 20}},
    };

    test_sorter_with_initial_data<String, Int>(keys, values, init_data);
  }

  SECTION("Int/Long") {
    std::vector<Int> keys{1, 10, 100};
    std::vector<std::vector<Long>> values{
      {1234567890, -987654321, 543209},
      {301948990, -10240, 2},
      {-1000, 403982117, -5}
    };

    std::map<Int, std::vector<Long>> init_data{
      {1, {1000, 2000, 3000}},
      {5, {1000, 2000, 3000}},
      {50, {1000, 2000, 3000}},
    };

    test_sorter_with_initial_data<Int, Long>(keys, values, init_data);
  }

  SECTION("Long/Float") {
    std::vector<Long> keys{10234, 224567, 956600};
    std::vector<std::vector<Float>> values{
      {-1.567, 850.33, 217.215},
      {-46.839, 3901.043, -5.091, 604.126},
      {2.96, 55.66}
    };

    std::map<Long, std::vector<Float>> init_data{
      {12345, {10.255, 3.410}}
    };

    test_sorter_with_initial_data<Long, Float>(keys, values, init_data);
  }
}

template <typename K, typename V>
void test_with_mqdataloader(std::vector<K>& keys, std::vector<std::vector<V>>& values) {
  assert(keys.size() == values.size());

  /// Set up initial dataset
  std::shared_ptr<MessageQueue> mq = std::make_unique<MessageQueue>();
  for (unsigned int i = 0; i < keys.size(); ++i) {
    for (auto& val: values[i])
      mq->send(ByteData{K{keys[i]}}, ByteData{V{val}});
  }
  mq->end();

  /// Typical target usage is passing data loader and run the sorter
  std::unique_ptr<DataLoader> loader(new MQDataLoader(mq));
  Sorter<K, V> sorter(std::move(loader));
  auto res = sorter.run();

  REQUIRE(check_map_items(*res, keys, values));
}

TEST_CASE("Sorter with MQDataLoader", "[sorter][mq]") {

  SECTION("load String/Long data") {
    std::vector<String> keys{"test", "example"};
    std::vector<std::vector<Long>> values{{10, 20}, {100, 200, 300}};

    test_with_mqdataloader<String, Long>(keys, values);
  }

  SECTION("load Int/Float data") {
    std::vector<Int> keys{100, 101};
    std::vector<std::vector<Float>> values{{1.23, -20}, {-5.0, -4.18, 437.55}};

    test_with_mqdataloader<Int, Float>(keys, values);
  }
}