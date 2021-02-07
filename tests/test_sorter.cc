#include "simplemapreduce/proc/sorter.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/proc/loader.h"

using namespace mapreduce;
using namespace mapreduce::proc;

TEST_CASE("test_grouping_items", "[sorter]")
{
  std::vector<std::string> keys{"test", "example", "sort"};

  SECTION("int_values")
  {
    std::vector<std::vector<int>> values{
      {1, -1, 5, 123, -6092},
      {4500, -53, 22},
      {-9, -444, -10207}
    };

    /// Create input value key-value pairs
    std::vector<std::pair<std::string, int>> inputs;
    for (unsigned int i = 0; i < keys.size(); ++i)
      for (auto &val: values[i])
        inputs.emplace_back(keys[i], val);

    std::unique_ptr<DataLoader<std::string, int>> loader =
        std::make_unique<TestDataLoader<std::string, int>>(inputs);

    /// Run sort task and group by the keys
    Sorter<std::string, int> sorter(std::move(loader));
    auto res = sorter.run();

    REQUIRE(check_map_items(res, keys, values));
  }

  SECTION("long_values")
  {
    std::vector<std::vector<long>> values{
      {1234567890, -987654321, 543209},
      {301948990, -10240, 2},
      {-1000, 403982117, -5}
    };

    /// Create input value key-value pairs
    std::vector<std::pair<std::string, long>> inputs;
    for (unsigned int i = 0; i < keys.size(); ++i)
      for (auto &val: values[i])
        inputs.emplace_back(keys[i], val);

    std::unique_ptr<DataLoader<std::string, long>> loader =
        std::make_unique<TestDataLoader<std::string, long>>(inputs);

    /// Run sort task and group by the keys
    Sorter<std::string, long> sorter(std::move(loader));
    auto res = sorter.run();

    REQUIRE(check_map_items(res, keys, values));
  }

  SECTION("float_values")
  {
    std::vector<std::vector<float>> values{
      {-1.23, 4.56, 7.8901},
      {-1.23, 4.56, 7.8901},
      {-1.23, 4.56, 7.8901}
    };

    /// Create input value key-value pairs
    std::vector<std::pair<std::string, float>> inputs;
    for (unsigned int i = 0; i < keys.size(); ++i)
      for (auto &val: values[i])
        inputs.emplace_back(keys[i], val);

    std::unique_ptr<DataLoader<std::string, float>> loader =
        std::make_unique<TestDataLoader<std::string, float>>(inputs);

    /// Run sort task and group by the keys
    Sorter<std::string, float> sorter(std::move(loader));
    auto res = sorter.run();

    REQUIRE(check_map_items(res, keys, values));
  }

  SECTION("double_values")
  {
    std::vector<std::vector<double>> values{
      {-1.23456789012345, 94504.3300856, 217.8123001115},
      {-40876.401839942, 3901.3044313, -5.091, 625504.126981},
      {4222.9801726, 555.6600123985}
    };

    /// Create input value key-value pairs
    std::vector<std::pair<std::string, double>> inputs;
    for (unsigned int i = 0; i < keys.size(); ++i)
      for (auto &val: values[i])
        inputs.emplace_back(keys[i], val);

    std::unique_ptr<DataLoader<std::string, double>> loader =
        std::make_unique<TestDataLoader<std::string, double>>(inputs);

    /// Run sort task and group by the keys
    Sorter<std::string, double> sorter(std::move(loader));
    auto res = sorter.run();

    REQUIRE(check_map_items(res, keys, values));
  }
}