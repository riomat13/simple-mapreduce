#include "simplemapreduce/proc/sorter.h"

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
#include "simplemapreduce/proc/loader.h"

using namespace mapreduce::data;
using namespace mapreduce::proc;

template <typename K, typename V>
void test_grouping_items(std::vector<K>& keys, std::vector<std::vector<V>>& values)
{
  /// Create input value key-value pairs
  std::vector<BytePair> inputs;
  for (unsigned int i = 0; i < keys.size(); ++i)
    for (auto& val: values[i])
      inputs.emplace_back(ByteData(K(keys[i])), ByteData(V(val)));

  std::unique_ptr<DataLoader> loader =
      std::make_unique<TestDataLoader>(inputs);

  /// Run sort task and group by the keys
  Sorter<K, V> sorter(std::move(loader));
  auto out = sorter.run();

  std::map<K, std::vector<V>> res;
  for (auto it = out.begin(); it != out.end(); ++it)
  {
    res.insert(std::pair<K, std::vector<V>>(it->first, it->second));
  }

  REQUIRE(check_map_items<K, V>(res, keys, values));
}

TEST_CASE("Base Sorter", "[sorter]")
{
  SECTION("string/int")
  {
    std::vector<std::string> keys{"test", "example", "sort"};
    std::vector<std::vector<int>> values{
      {1, -1, 5, 123, -6092},
      {4500, -53, 22},
      {-9, -444, -10207}
    };

    test_grouping_items<std::string, int>(keys, values);
  }

  SECTION("int/long")
  {
    std::vector<int> keys{1, 10, 100};
    std::vector<std::vector<long>> values{
      {1234567890, -987654321, 543209},
      {301948990, -10240, 2},
      {-1000, 403982117, -5}
    };

    test_grouping_items<int, long>(keys, values);
  }

  SECTION("string/double")
  {
    std::vector<std::string> keys{"test", "example", "sort"};
    std::vector<std::vector<double>> values{
      {-1.23456789012345, 94504.3300856, 217.8123001115},
      {-40876.401839942, 3901.3044313, -5.091, 625504.126981},
      {4222.9801726, 555.6600123985}
    };

    test_grouping_items<std::string, double>(keys, values);
  }
}

template <typename K, typename V>
void test_with_mqdataloader(std::vector<K>& keys, std::vector<std::vector<V>>& values)
{
  assert(keys.size() == values.size());

  /// Set up initial dataset
  std::shared_ptr<MessageQueue> mq = std::make_unique<MessageQueue>();
  for (unsigned int i = 0; i < keys.size(); ++i)
  {
    for (auto& val: values[i])
      mq->send(ByteData(K(keys[i])), ByteData(V(val)));
  }
  mq->end();

  /// Typical target usage is passing data loader and run the sorter
  std::unique_ptr<DataLoader> loader(new MQDataLoader(mq));
  Sorter<K, V> sorter(std::move(loader));
  auto res = sorter.run();

  REQUIRE(check_map_items(res, keys, values));
}

TEST_CASE("Sorter with MQDataLoader", "[sorter][mq]")
{
  SECTION("load string/long data")
  {
    std::vector<std::string> keys{"test", "example"};
    std::vector<std::vector<long>> values{{10, 20}, {100, 200, 300}};

    test_with_mqdataloader(keys, values);
  }

  SECTION("load int/float data")
  {
    std::vector<int> keys{100, 101};
    std::vector<std::vector<float>> values{{1.23, -20}, {-5.0, -4.18, 437.55}};

    test_with_mqdataloader(keys, values);
  }
}