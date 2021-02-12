#include "simplemapreduce/proc/shuffle.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <utility>

#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/conf.h"

namespace fs = std::filesystem;

using namespace mapreduce;
using namespace mapreduce::data;
using namespace mapreduce::proc;

/**
 * Read binary file.
 * The key type is string and value type is any non-array datatype.
 */
template <typename T>
class BinFileReader
{
 public:
  BinFileReader(const fs::path& path)
  {
    ifs.open(path, std::ios::binary | std::ios::in);
  }
  ~BinFileReader() { ifs.close(); }

  void read_binary(std::vector<BytePair>& items)
  {
    while (true)
    {
      size_t keysize;
      ifs.read(reinterpret_cast<char*>(&keysize), sizeof(size_t));
      if(ifs.eof())
        break;

      /// Read key data
      char keydata[keysize];
      ifs.read(keydata, sizeof(char) * keysize);
      std::string key(keydata, keysize);

      /// Read value data
      T value;
      ifs.read(reinterpret_cast<char*>(&value), sizeof(T));

      items.emplace_back(ByteData(key), ByteData(value));
    }
  }

 private:
  std::ifstream ifs;
};

/**
 * Read data from given binary files.
 *
 *  @param files       target files to read
 *  @param container   vector to store key-value pair read from files
 */
template <typename K, typename V>
void read_all_data(std::vector<fs::path>& files, std::vector<BytePair>& container)
{
  for (auto& file: files)
  {
    BinFileReader<V> reader(file);
    reader.read_binary(container);
  }
}

TEST_CASE("Shuffle", "[shuffle]")
{
  typedef MessageQueue MQ;

  std::shared_ptr<JobConf> conf = std::make_shared<JobConf>();
  conf->tmpdir = tmpdir / "test_shuffle";
  fs::remove_all(conf->tmpdir);
  fs::create_directories(conf->tmpdir);

  SECTION("string/int")
  {
    conf->worker_rank = 0;
    conf->worker_size = 2;
    conf->n_groups = 5;  // this will be equal to a number of output files

    /// Target key-value dataset
    std::vector<BytePair> dataset{
      {ByteData("test"), ByteData(10)},
      {ByteData("test"), ByteData(-1234)},
      {ByteData("example"), ByteData(5000)},
      {ByteData("example"), ByteData(-76543)}
    };

    {
      std::shared_ptr<MQ> mq = std::make_shared<MQ>();
      Shuffle<std::string, int> shuffle(mq, conf);

      /// Store all data to MessageQueue
      for (auto& data: dataset)
        mq->send(std::pair(data));
      mq->end();

      shuffle.run();
    }

    /// Total file counts after processed by shuffler
    std::vector<fs::path> bin_files;
    extract_files(conf->tmpdir, bin_files);
    REQUIRE(bin_files.size() == conf->n_groups);

    std::vector<BytePair> kv_items;
    read_all_data<std::string, int>(bin_files, kv_items);

    /// Check original data and stored data are the same
    REQUIRE_THAT(kv_items, Catch::Matchers::UnorderedEquals(dataset));
  }

  SECTION("string/long")
  {
    conf->worker_rank = 1;
    conf->worker_size = 2;
    conf->n_groups = 4;  // this will be equal to a number of output files

    /// Target key-value dataset
    std::vector<BytePair> dataset{
      {ByteData("test"), ByteData(198765432l)},
      {ByteData("test"), ByteData(-123456789l)},
      {ByteData("example"), ByteData(50005000l)},
      {ByteData("example"), ByteData(-76543l)}
    };

    {
      std::shared_ptr<MQ> mq = std::make_shared<MQ>();
      Shuffle<std::string, long> shuffle(mq, conf);

      /// Store all data to MessageQueue
      for (auto& data: dataset)
        mq->send(std::pair(data));
      mq->end();

      shuffle.run();
    }

    /// Total file counts after processed by shuffler
    std::vector<fs::path> bin_files;
    extract_files(conf->tmpdir, bin_files);
    REQUIRE(bin_files.size() == conf->n_groups);

    std::vector<BytePair> kv_items;
    read_all_data<std::string, long>(bin_files, kv_items);

    /// Check original data and stored data are the same
    REQUIRE_THAT(kv_items, Catch::Matchers::UnorderedEquals(dataset));
  }

  SECTION("string/float")
  {

    conf->worker_rank = 3;
    conf->worker_size = 5;
    conf->n_groups = 3;  // this will be equal to a number of output files

    /// Target key-value dataset
    std::vector<BytePair> dataset{
      {ByteData("test"), ByteData(1.2345f)},
      {ByteData("test"), ByteData(-5.4321f)},
      {ByteData("example"), ByteData(102.5987f)},
      {ByteData("example"), ByteData(-980.7628f)}
    };

    {
      std::shared_ptr<MQ> mq = std::make_shared<MQ>();
      Shuffle<std::string, float> shuffle(mq, conf);

      /// Store all data to MessageQueue
      for (auto& data: dataset)
        mq->send(std::pair(data));
      mq->end();

      shuffle.run();
    }

    /// Total file counts after processed by shuffler
    std::vector<fs::path> bin_files;
    extract_files(conf->tmpdir, bin_files);
    REQUIRE(bin_files.size() == conf->n_groups);

    std::vector<BytePair> kv_items;
    read_all_data<std::string, float>(bin_files, kv_items);

    /// Check original data and stored data are the same
    REQUIRE_THAT(kv_items, Catch::Matchers::UnorderedEquals(dataset));
  }

  SECTION("string/double")
  {
    conf->worker_rank = 0;
    conf->worker_size = 3;
    conf->n_groups = 2;  // this will be equal to a number of output files

    /// Target key-value dataset
    std::vector<BytePair> dataset{
      {ByteData("test"), ByteData(1.23456789012345)},
      {ByteData("test"), ByteData(-5.43210987654321)},
      {ByteData("example"), ByteData(50987.5987)},
      {ByteData("example"), ByteData(-22902.1023072)}
    };

    {
      std::shared_ptr<MQ> mq = std::make_shared<MQ>();
      Shuffle<std::string, double> shuffle(mq, conf);

      /// Store all data to MessageQueue
      for (auto& data: dataset)
        mq->send(std::pair(data));
      mq->end();

      shuffle.run();
    }

    /// Total file counts after processed by shuffler
    std::vector<fs::path> bin_files;
    extract_files(conf->tmpdir, bin_files);
    REQUIRE(bin_files.size() == conf->n_groups);

    std::vector<BytePair> kv_items;
    read_all_data<std::string, double>(bin_files, kv_items);

    /// Check original data and stored data are the same
    REQUIRE_THAT(kv_items, Catch::Matchers::UnorderedEquals(dataset));
  }

  fs::remove_all(tmpdir);
}