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
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/conf.h"

namespace fs = std::filesystem;

using namespace mapreduce;
using namespace mapreduce::proc;

/**
 * Read binary file.
 * The key type is string and value type is any non-array datatype.
 */
template <typename T>
class BinFileReader
{
 public:
  BinFileReader(const fs::path &path)
  {
    ifs.open(path, std::ios::binary | std::ios::in);
  }
  ~BinFileReader() { ifs.close(); }

  void read_binary(std::vector<std::pair<std::string, T>> &items)
  {
    while (true)
    {
      size_t keysize;
      ifs.read(reinterpret_cast<char *>(&keysize), sizeof(size_t));
      if(ifs.eof())
        break;

      /// Read key data
      char keydata[keysize];
      ifs.read(keydata, sizeof(char) * keysize);
      std::string key(keydata, keysize);

      /// Read value data
      T value;
      ifs.read(reinterpret_cast<char *>(&value), sizeof(T));

      items.emplace_back(std::move(key), std::move(value));
    }
  }

 private:
  std::ifstream ifs;
};

/**
 * Read data from given binary files.
 * 
 *  @param files&       target files to read
 *  @param container&   vector to store key/value pair read from files
 */
template <typename T>
void read_all_data(std::vector<fs::path> &files, std::vector<std::pair<std::string, T>> &container)
{
  for (auto &file: files)
  {
    BinFileReader<T> reader(file);
    reader.read_binary(container);
  }
}

TEST_CASE("Shuffle", "[shuffle]")
{
  fs::path outdir(testdir / "test_shuffle");
  fs::create_directories(outdir);

  SECTION("string_int")
  {
    typedef MessageQueue<std::string, int> MQ;
    typedef std::pair<std::string, int> KV;

    JobConf conf;
    conf.worker_rank = 0;
    conf.worker_size = 2;
    conf.n_groups = 5;  // this will be equivalent to a number of output files

    /// Target key/value dataset
    std::vector<KV> dataset{{"test", 10}, {"test", -1234}, {"example", 5000}, {"example", -76543}};

    {
      std::shared_ptr<MQ> mq = std::make_shared<MQ>();
      Shuffle<std::string, int> shuffle(mq, outdir.string(), conf);

      /// Store all data to MessageQueue
      for (auto &data: dataset)
        mq->send(std::pair(data));
      mq->end();

      shuffle.run();
    }

    /// Total file counts after processed by shuffler
    std::vector<fs::path> bin_files;
    extract_files(outdir, bin_files);

    REQUIRE(bin_files.size() == conf.n_groups);

    std::vector<KV> kv_items;
    read_all_data(bin_files, kv_items);

    /// Check original data and stored data are the same
    compare_vector(kv_items, dataset);
  }

  SECTION("string_long")
  {
    typedef MessageQueue<std::string, long> MQ;
    typedef std::pair<std::string, long> KV;

    JobConf conf;
    conf.worker_rank = 1;
    conf.worker_size = 2;
    conf.n_groups = 4;  // this will be equivalent to a number of output files

    /// Target key/value dataset
    std::vector<KV> dataset{{"test", 1987654321}, {"test", -1234567890}, {"example", 50005000}, {"example", -76543}};

    {
      std::shared_ptr<MQ> mq = std::make_shared<MQ>();
      Shuffle<std::string, long> shuffle(mq, outdir.string(), conf);

      /// Store all data to MessageQueue
      for (auto &data: dataset)
        mq->send(std::pair(data));
      mq->end();

      shuffle.run();
    }

    /// Total file counts after processed by shuffler
    std::vector<fs::path> bin_files;
    extract_files(outdir, bin_files);

    REQUIRE(bin_files.size() == conf.n_groups);

    std::vector<KV> kv_items;
    read_all_data(bin_files, kv_items);

    /// Check original data and stored data are the same
    compare_vector(kv_items, dataset);
  }

  SECTION("string_float")
  {
    typedef MessageQueue<std::string, float> MQ;
    typedef std::pair<std::string, float> KV;

    JobConf conf;
    conf.worker_rank = 3;
    conf.worker_size = 5;
    conf.n_groups = 3;  // this will be equivalent to a number of output files

    /// Target key/value dataset
    std::vector<KV> dataset{{"test", 1.2345}, {"test", -5.4321}, {"example", 102.5987}, {"example", -980.7628}};

    {
      std::shared_ptr<MQ> mq = std::make_shared<MQ>();
      Shuffle<std::string, float> shuffle(mq, outdir.string(), conf);

      /// Store all data to MessageQueue
      for (auto &data: dataset)
        mq->send(std::pair(data));
      mq->end();

      shuffle.run();
    }

    /// Total file counts after processed by shuffler
    std::vector<fs::path> bin_files;
    extract_files(outdir, bin_files);

    REQUIRE(bin_files.size() == conf.n_groups);

    std::vector<KV> kv_items;
    read_all_data(bin_files, kv_items);

    /// Check original data and stored data are the same
    compare_vector(kv_items, dataset);
  }

  SECTION("string_double")
  {
    typedef MessageQueue<std::string, float> MQ;
    typedef std::pair<std::string, float> KV;

    JobConf conf;
    conf.worker_rank = 0;
    conf.worker_size = 3;
    conf.n_groups = 2;  // this will be equivalent to a number of output files

    /// Target key/value dataset
    std::vector<KV> dataset{{"test", 1.23456789012345}, {"test", -5.43210987654321}, {"example", 50987.5987}, {"example", -22902.1023072}};

    {
      std::shared_ptr<MQ> mq = std::make_shared<MQ>();
      Shuffle<std::string, float> shuffle(mq, outdir.string(), conf);

      /// Store all data to MessageQueue
      for (auto &data: dataset)
        mq->send(std::pair(data));
      mq->end();

      shuffle.run();
    }

    /// Total file counts after processed by shuffler
    std::vector<fs::path> bin_files;
    extract_files(outdir, bin_files);

    REQUIRE(bin_files.size() == conf.n_groups);

    std::vector<KV> kv_items;
    read_all_data(bin_files, kv_items);

    /// Check original data and stored data are the same
    compare_vector(kv_items, dataset);
  }

  fs::remove_all(testdir);
}