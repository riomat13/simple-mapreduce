#include "simplemapreduce/proc/loader.h"

#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/data/type.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/proc/writer.h"

namespace fs = std::filesystem;

using namespace mapreduce;
using namespace mapreduce::data;
using namespace mapreduce::proc;
using namespace mapreduce::type;

typedef MessageQueue MQ;

TEST_CASE("load_data", "[binary][read][file]") {
  fs::path fpath{tmpdir / "test_loader" / "tmp_bin"};
  fs::create_directories(fpath.parent_path());

  SECTION("Int16") {
    std::vector<Int16> targets{2, 7, -4, 5};
    std::vector<Int16> res;

    /// Write bytes data to a file
    {
      std::ofstream ofs(fpath, std::ios::binary);
      for (auto val: targets)
        write_binary(ofs, ByteData{val});
      ofs.close();
    }

    /// Read from the file
    {
      std::ifstream ifs(fpath, std::ios::binary);
      ByteData data;

      while (!(data = load_byte_data<Int16>(ifs)).empty())
        res.push_back(data.get_data<Int16>());

      ifs.close();
    }

    REQUIRE_THAT(res, Catch::Matchers::Equals(targets));
  }

  SECTION("Int") {
    std::vector<Int> targets{1, 3, 7};
    std::vector<Int> res;

    /// Write bytes data to a file
    {
      std::ofstream ofs(fpath, std::ios::binary);
      for (auto val: targets)
        write_binary(ofs, ByteData{val});
      ofs.close();
    }

    /// Read from the file
    {
      std::ifstream ifs(fpath, std::ios::binary);
      ByteData data;

      while (!(data = load_byte_data<Int>(ifs)).empty())
        res.push_back(data.get_data<Int>());

      ifs.close();
    }

    REQUIRE_THAT(res, Catch::Matchers::Equals(targets));
  }

  SECTION("Long") {
    std::vector<Long> targets{987654321, 33201958, -7};
    std::vector<Long> res;

    /// Write bytes data to a file
    {
      std::ofstream ofs(fpath, std::ios::binary);
      for (auto val: targets)
        write_binary(ofs, ByteData{val});
      ofs.close();
    }

    /// Read from the file
    {
      std::ifstream ifs(fpath, std::ios::binary);
      ByteData data;

      while (!(data = load_byte_data<Long>(ifs)).empty())
        res.push_back(data.get_data<Long>());

      ifs.close();
    }

    REQUIRE_THAT(res, Catch::Matchers::Equals(targets));
  }

  SECTION("Float") {
    std::vector<Float> targets{987.4321, 33.0195, -1.237};
    std::vector<Float> res;

    /// Write bytes data to a file
    {
      std::ofstream ofs(fpath, std::ios::binary);
      for (auto val: targets)
        write_binary(ofs, ByteData{val});
      ofs.close();
    }

    /// Read from the file
    {
      std::ifstream ifs(fpath, std::ios::binary);
      ByteData data;

      while (!(data = load_byte_data<Float>(ifs)).empty())
        res.push_back(data.get_data<Float>());

      ifs.close();
    }

    REQUIRE_THAT(res, Catch::Matchers::Equals(targets));
  }

  SECTION("Double") {
    std::vector<Double> targets{987.432123456, -3553.220195, -3410.237077};
    std::vector<Double> res;

    /// Write bytes data to a file
    {
      std::ofstream ofs(fpath, std::ios::binary);
      for (auto val: targets)
        write_binary(ofs, ByteData{val});
      ofs.close();
    }

    /// Read from the file
    {
      std::ifstream ifs(fpath, std::ios::binary);
      ByteData data;

      while (!(data = load_byte_data<Double>(ifs)).empty())
        res.push_back(data.get_data<Double>());

      ifs.close();
    }

    REQUIRE_THAT(res, Catch::Matchers::Equals(targets));
  }

  fs::remove_all(tmpdir);
}

TEST_CASE("DataLoader", "[data_loader]") {
  /// Target key-value pairs
  std::vector<BytePair> targets{
    {ByteData("test"), ByteData(Int{1})},
    {ByteData("test"), ByteData(Int{2})},
    {ByteData("sample"), ByteData(Int{-5})}
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

/** Test BinaryFileDataLoader. */
template <typename K, typename V>
void test_binary_file_data_loader(std::vector<K>& keys, std::vector<V>& values) {
  std::shared_ptr<JobConf> conf = std::make_shared<JobConf>();
  conf->n_groups = 1;
  conf->worker_rank = 0;
  conf->worker_size = 1;
  conf->tmpdir = tmpdir / "test_loader";

  fs::create_directories(conf->tmpdir);
  fs::path fname{"0000-00000"};

  clear_file(conf->tmpdir / fname);

  /// Used for value check
  std::vector<ByteData> target_keys;
  std::vector<std::vector<ByteData>> target_values;

  /// Write binary data to a target file
  {
    BinaryFileWriter<String, Int> writer(conf->tmpdir / fname);

    for (auto& key: keys) {
      std::vector<ByteData> vals;
      for (auto& val: values) {
        writer.write(ByteData(K(key)), ByteData(V(val)));
        vals.push_back(ByteData(V(val)));
      }

      target_values.push_back(std::move(vals));
    }
  }

  std::unique_ptr<DataLoader> loader =
      std::make_unique<BinaryFileDataLoader<K, V>>(conf);

  std::map<ByteData, std::vector<ByteData>> res;
  BytePair data;

  while (!(data = loader->get_item()).first.empty())
    res[data.first].push_back(data.second);

  for (auto& key: keys)
    target_keys.emplace_back(std::move(key));

  REQUIRE(check_map_items<ByteData, ByteData>(res, target_keys, target_values));
}

TEST_CASE("BinaryFileDataLoader", "[data_loader][binary]") {

  SECTION("String/Int") {
    std::vector<String> keys{"test", "example", "sort"};
    std::vector<Int> values{1, 4321, -1234};

    test_binary_file_data_loader<String, Int>(keys, values);
  }

  SECTION("Int/Long") {
    std::vector<Int> keys{123, 234, 0, 345, -401};
    std::vector<Long> values{1, 123456789l, 123456789l};

    test_binary_file_data_loader<Int, Long>(keys, values);
  }

  SECTION("Int32/Int16") {
    std::vector<Int32> keys{123, 234, 0, 345, -401};
    std::vector<Int16> values{123, -30, 555};

    test_binary_file_data_loader<Int, Int16>(keys, values);
  }

  SECTION("String/Float") {
    std::vector<String> keys{"test", "example", "sort"};
    std::vector<Float> values{0.1, -74.904, 10.987, 0.1234};

    test_binary_file_data_loader<String, Float>(keys, values);
  }

  SECTION("Long/Double") {
    std::vector<Long> keys{123, 234, 345};
    std::vector<Double> values{0.5, 1.23456789, 9.87654321, -502.012345657};

    test_binary_file_data_loader<Long, Double>(keys, values);
  }

  fs::remove_all(tmpdir);
}

TEST_CASE("MQDataLoader", "[data_loader][mq]") {
  std::shared_ptr<MQ> mq = std::make_shared<MQ>();
  std::unique_ptr<DataLoader> loader =
      std::make_unique<MQDataLoader>(mq);

  SECTION("String/Int") {
    std::vector<BytePair> targets{
      {ByteData(String{"test"}), ByteData(Int{10})},
      {ByteData(String{"example"}), ByteData(Int{-5})},
      {ByteData(String{"test"}), ByteData(Int{20})}
    };

    for (auto& kv: targets) {
      ByteData key(kv.first), value(kv.second);
      mq->send(std::make_pair(key, value));
    }
    mq->end();

    /// Get all data from loader
    std::vector<BytePair> res;
    BytePair data = loader->get_item();
    while (!(data.first.empty())) {
      res.push_back(std::move(data));
      data = loader->get_item();
    }

    /// Check if original data and data got from loader are the same
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(targets));
  }

  SECTION("String/Long") {
    std::vector<BytePair> targets{
      {ByteData(String{"test"}), ByteData(Long{1357902468})},
      {ByteData(String{"example"}), ByteData(Long{-54019283})},
    };

    for (auto& kv: targets)
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

  SECTION("Int/Double") {
    std::vector<BytePair> targets{
      {ByteData(Int{102}), ByteData(Double{13579.02468})},
      {ByteData(Int{-233}), ByteData(Double{-540.19283})},
    };

    for (auto& kv: targets)
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

  SECTION("CompositeKey<String, Long>/Double") {
    std::vector<BytePair> targets{
      {ByteData(CompositeKey<String, Long>(String{"test"}, Long{101})), ByteData(Double{13579.02468})},
      {ByteData(CompositeKey<String, Long>(String{"test"}, Long{102})), ByteData(Double{-540.19283})},
    };

    for (auto& kv: targets)
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