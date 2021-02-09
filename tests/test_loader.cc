#include "simplemapreduce/proc/loader.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/proc/writer.h"

using namespace mapreduce::data;
using namespace mapreduce::proc;

typedef MessageQueue MQ;

TEST_CASE("DataLoader", "[data_loader]")
{
  /// Target key-value pairs
  std::vector<BytePair> targets{
    {ByteData("test"), ByteData(1)},
    {ByteData("test"), ByteData(2)},
    {ByteData("sample"), ByteData(-5)}
  };

  std::vector<BytePair> items = targets;
  std::unique_ptr<DataLoader> loader =
      std::make_unique<TestDataLoader>(items);

  std::vector<BytePair> res;
  BytePair data;
  while (!(data = loader->get_item()).first.empty())
    res.push_back(std::move(data));

  REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(targets));
}

TEST_CASE("BinaryFileDataLoader", "[data_loader][binary]")
{
  JobConf conf;
  conf.n_groups = 1;
  conf.worker_rank = 0;
  conf.worker_size = 1;
  conf.tmpdir = tmpdir / "test_loader";

  fs::create_directories(conf.tmpdir);
  fs::path fname{"0000-00000"};

  clear_file(conf.tmpdir / fname);

  std::vector<std::string> keys{"test", "example", "sort"};

  SECTION("int_values")
  {
    /// Used for value check
    std::vector<ByteData> target_keys;
    std::vector<std::vector<ByteData>> target_values;

    /// Write binary data to a target file
    {
      BinaryFileWriter<std::string, int> writer(conf.tmpdir / fname);

      for (auto &key: keys)
      {
        int val1 = 1;
        int val2 = 4321;
        int val3 = 1234;
        writer.write(ByteData(std::string(key)), ByteData(val1));
        writer.write(ByteData(std::string(key)), ByteData(val2));
        writer.write(ByteData(std::string(key)), ByteData(val3));

        std::vector<ByteData> vals{ByteData(val1), ByteData(val2), ByteData(val3)};
        target_values.push_back(std::move(vals));
      }
    }

    std::unique_ptr<DataLoader> loader =
        std::make_unique<BinaryFileDataLoader<std::string, int>>(conf);

    std::map<ByteData, std::vector<ByteData>> res;
    BytePair data;

    while (!(data = loader->get_item()).first.empty())
      res[data.first].push_back(data.second);

    for (auto &key: keys)
      target_keys.emplace_back(key);

    REQUIRE(check_map_items<ByteData, ByteData>(res, target_keys, target_values));
  }

  SECTION("long_values")
  {
    /// Used for value check
    std::vector<ByteData> target_keys;
    std::vector<std::vector<ByteData>> target_values;

    /// Write binary data to a target file
    {
      BinaryFileWriter<std::string, long> writer(conf.tmpdir / fname);

      for (auto &key: keys)
      {
        long val1 = 1;
        long val2 = 123456789l;
        long val3 = 123456789l;
        writer.write(ByteData(std::string(key)), ByteData(val1));
        writer.write(ByteData(std::string(key)), ByteData(val2));
        writer.write(ByteData(std::string(key)), ByteData(val3));

        std::vector<ByteData> vals{ByteData(val1), ByteData(val2), ByteData(val3)};
        target_values.push_back(std::move(vals));
      }
    }

    std::unique_ptr<DataLoader> loader =
        std::make_unique<BinaryFileDataLoader<std::string, long>>(conf);

    std::map<ByteData, std::vector<ByteData>> res;
    BytePair data;

    while (!(data = loader->get_item()).first.empty())
      res[data.first].push_back(data.second);

    for (auto &key: keys)
      target_keys.emplace_back(key);
    REQUIRE(check_map_items<ByteData, ByteData>(res, target_keys, target_values));
  }

  SECTION("float_values")
  {
    /// Used for value check
    std::vector<ByteData> target_keys;
    std::vector<std::vector<ByteData>> target_values;

    /// Write binary data to a target file
    {
      BinaryFileWriter<std::string, float> writer(conf.tmpdir / fname);

      for (auto &key: keys)
      {
        float val1 = 0.1;
        float val2 = 10.987;
        float val3 = 0.1234;
        writer.write(ByteData(std::string(key)), ByteData(val1));
        writer.write(ByteData(std::string(key)), ByteData(val2));
        writer.write(ByteData(std::string(key)), ByteData(val3));

        std::vector<ByteData> vals{ByteData(val1), ByteData(val2), ByteData(val3)};
        target_values.push_back(std::move(vals));
      }
    }

    std::unique_ptr<DataLoader> loader =
        std::make_unique<BinaryFileDataLoader<std::string, float>>(conf);

    std::map<ByteData, std::vector<ByteData>> res;
    BytePair data;

    while (!(data = loader->get_item()).first.empty())
      res[data.first].push_back(data.second);

    for (auto &key: keys)
      target_keys.emplace_back(key);

    REQUIRE(check_map_items<ByteData, ByteData>(res, target_keys, target_values));
  }

  SECTION("double_values")
  {
    /// Used for value check
    std::vector<ByteData> target_keys;
    std::vector<std::vector<ByteData>> target_values;

    /// Write binary data to a target file
    {
      BinaryFileWriter<std::string, double> writer(conf.tmpdir / fname);

      for (auto &key: keys)
      {
        double val1 = 0.5;
        double val2 = 1.23456789;
        double val3 = 9.87654321;
        writer.write(ByteData(std::string(key)), ByteData(val1));
        writer.write(ByteData(std::string(key)), ByteData(val2));
        writer.write(ByteData(std::string(key)), ByteData(val3));

        std::vector<ByteData> vals{ByteData(val1), ByteData(val2), ByteData(val3)};
        target_values.push_back(std::move(vals));
      }
    }

    std::unique_ptr<DataLoader> loader =
        std::make_unique<BinaryFileDataLoader<std::string, double>>(conf);

    std::map<ByteData, std::vector<ByteData>> res;
    BytePair data;

    while (!(data = loader->get_item()).first.empty())
      res[data.first].push_back(data.second);

    for (auto &key: keys)
      target_keys.emplace_back(key);

    REQUIRE(check_map_items<ByteData, ByteData>(res, target_keys, target_values));
  }

  fs::remove_all(tmpdir);
}

TEST_CASE("MQDataLoader", "[data_loader][mq]")
{
  std::shared_ptr<MQ> mq = std::make_shared<MQ>();
  std::unique_ptr<DataLoader> loader =
      std::make_unique<MQDataLoader>(mq);

  SECTION("string/int")
  {
    std::vector<BytePair> targets{
      {ByteData("test"), ByteData(10)},
      {ByteData("example"), ByteData(-5)},
      {ByteData("test"), ByteData(20)}
    };

    for (auto &kv: targets)
    {
      ByteData key(kv.first), value(kv.second);
      mq->send(std::make_pair(key, value));
    }
    mq->end();

    /// Get all data from loader
    std::vector<BytePair> res;
    BytePair data = loader->get_item();
    while (!(data.first.empty()))
    {
      res.push_back(std::move(data));
      data = loader->get_item();
    }

    /// Check if original data and data got from loader are the same
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(targets));
  }

  SECTION("string/long")
  {
    std::vector<BytePair> targets{
      {ByteData("test"), ByteData(1357902468l)},
      {ByteData("example"), ByteData(-54019283l)},
    };

    for (auto &kv: targets)
      mq->send(std::pair(kv));
    mq->end();

    /// Get all data from loader
    std::vector<BytePair> res;
    BytePair data;
    while (!(data = loader->get_item()).first.empty())
      res.push_back(std::move(data));

    /// Check if original data and data got from loader are the same
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(targets));
  }
}