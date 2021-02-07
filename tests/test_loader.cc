#include "simplemapreduce/proc/loader.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/proc/writer.h"

using namespace mapreduce::proc;

TEST_CASE("DataLoader", "[data_loader]")
{
  typedef std::pair<std::string, int> KV;

  /// Target key-value pairs
  std::vector<KV> targets{{"test", 1}, {"test", 2}, {"sample", -5}};

  std::vector<KV> items = targets;
  std::unique_ptr<DataLoader<std::string, int>> loader =
      std::make_unique<TestDataLoader<std::string, int>>(items);

  std::vector<KV> res;
  KV data;
  while (!(data = loader->get_item()).first.empty())
    res.push_back(std::move(data));

  REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(targets));
}

TEST_CASE("FileDataLoader", "[data_loader][binary]")
{
  JobConf conf;
  conf.n_groups = 1;
  conf.worker_rank = 0;
  conf.worker_size = 1;
  conf.tmpdir = testdir / "test_sorter";

  fs::create_directories(conf.tmpdir);
  fs::path fname{"0000-00000"};

  std::vector<std::string> keys{"test", "example", "sort"};

  SECTION("int_values")
  {
    clear_file(conf.tmpdir / fname);

    /// Used for value check
    std::vector<std::vector<int>> values;

    /// Write binary data to a target file
    {
      BinaryFileWriter writer(conf.tmpdir / fname);

      for (size_t i = 0; i < keys.size(); ++i)
      {
        int val1 = i + 1;
        int val2 = i + 4321;
        int val3 = i - 1234;
        writer.write(keys[i], val1);
        writer.write(keys[i], val2);
        writer.write(keys[i], val3);

        std::vector<int> vals{val1, val2, val3};
        values.push_back(std::move(vals));
      }
    }

    /// Run sort task and group by the keys
    std::unique_ptr<DataLoader<std::string, int>> loader =
        std::make_unique<BinaryFileDataLoader<std::string, int>>(conf);

    std::map<std::string, std::vector<int>> res;

    std::pair<std::string, int> data;

    while (!(data = loader->get_item()).first.empty())
      res[data.first].push_back(data.second);

    REQUIRE(check_map_items(res, keys, values));
  }

  SECTION("long_values")
  {
    clear_file(conf.tmpdir / fname);

    /// Used for value check
    std::vector<std::vector<long>> values;

    /// Write binary data to a target file
    {
      BinaryFileWriter writer(conf.tmpdir / fname);

      for (size_t i = 0; i < keys.size(); ++i)
      {
        long val1 = i + 1;
        long val2 = i + 1234567890;
        long val3 = i - 1234567890;
        writer.write(keys[i], val1);
        writer.write(keys[i], val2);
        writer.write(keys[i], val3);

        std::vector<long> vals{val1, val2, val3};
        values.push_back(std::move(vals));
      }
    }

    std::unique_ptr<DataLoader<std::string, long>> loader =
        std::make_unique<BinaryFileDataLoader<std::string, long>>(conf);

    std::map<std::string, std::vector<long>> res;

    std::pair<std::string, long> data;

    while (!(data = loader->get_item()).first.empty())
      res[data.first].push_back(data.second);

    REQUIRE(check_map_items(res, keys, values));
  }

  SECTION("float_values")
  {
    clear_file(conf.tmpdir / fname);

    /// Used for value check
    std::vector<std::vector<float>> values;

    /// Write binary data to a target file
    {
      BinaryFileWriter writer(conf.tmpdir / fname);

      for (size_t i = 0; i < keys.size(); ++i)
      {
        float val1 = i + 0.1;
        float val2 = i + 10.987;
        float val3 = i - 0.1234;
        writer.write(keys[i], val1);
        writer.write(keys[i], val2);
        writer.write(keys[i], val3);

        std::vector<float> vals{val1, val2, val3};
        values.push_back(std::move(vals));
      }
    }

    std::unique_ptr<DataLoader<std::string, float>> loader =
        std::make_unique<BinaryFileDataLoader<std::string, float>>(conf);

    std::map<std::string, std::vector<float>> res;

    std::pair<std::string, float> data;

    while (!(data = loader->get_item()).first.empty())
      res[data.first].push_back(data.second);

    REQUIRE(check_map_items(res, keys, values));
  }

  SECTION("double_values")
  {
    clear_file(conf.tmpdir / fname);

    /// Used for value check
    std::vector<std::vector<double>> values;

    /// Write binary data to a target file
    {
      BinaryFileWriter writer(conf.tmpdir / fname);

      for (size_t i = 0; i < keys.size(); ++i)
      {
        double val1 = i + 0.5;
        double val2 = i + 1.23456789;
        double val3 = i - 9.87654321;
        writer.write(keys[i], val1);
        writer.write(keys[i], val2);
        writer.write(keys[i], val3);

        std::vector<double> vals{val1, val2, val3};
        values.push_back(std::move(vals));
      }
    }

    std::unique_ptr<DataLoader<std::string, double>> loader =
        std::make_unique<BinaryFileDataLoader<std::string, double>>(conf);

    std::map<std::string, std::vector<double>> res;

    std::pair<std::string, double> data;

    while (!(data = loader->get_item()).first.empty())
      res[data.first].push_back(data.second);

    REQUIRE(check_map_items(res, keys, values));
  }

  fs::remove_all(testdir);
}