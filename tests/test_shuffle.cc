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
#include "simplemapreduce/data/type.h"
#include "simplemapreduce/ops/conf.h"

namespace fs = std::filesystem;

using namespace mapreduce;
using namespace mapreduce::data;
using namespace mapreduce::proc;
using namespace mapreduce::type;

/**
 * Read binary file.
 * The key type is string and value type is any non-array datatype.
 */
template <typename T>
class BinFileReader {
 public:
  BinFileReader(const fs::path& path) {
    ifs.open(path, std::ios::binary | std::ios::in);
  }
  ~BinFileReader() { ifs.close(); }

  void read_binary(std::vector<BytePair>& items) {
    while (true) {
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

      items.emplace_back(ByteData(std::move(key)), ByteData(std::move(value)));
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
void read_all_data(std::vector<fs::path>& files, std::vector<BytePair>& container) {
  for (auto& file: files) {
    BinFileReader<V> reader(file);
    reader.read_binary(container);
  }
}

template <typename K, typename V>
void test_shuffle(std::vector<BytePair>& dataset) {
  typedef MessageQueue MQ;

  std::shared_ptr<JobConf> conf = std::make_shared<JobConf>();
  conf->tmpdir = tmpdir / "test_shuffle";
  fs::remove_all(conf->tmpdir);
  fs::create_directories(conf->tmpdir);

  conf->worker_rank = 0;
  conf->worker_size = 2;
  conf->n_groups = 5;  // this will be equal to a number of output files

  /// Store all shuffled results
  std::vector<BytePair> kv_items;

  {
    std::shared_ptr<MQ> mq = std::make_shared<MQ>();
    Shuffle<K, V> shuffle(mq, conf);

    /// Store all data to MessageQueue
    for (auto& data: dataset)
      mq->send(std::pair(data));
    mq->end();

    shuffle.run();

    /// Take all data stored for the same worker node,
    auto data = mq->receive();
    while (!data.first.empty()) {
      kv_items.emplace_back(std::move(data.first), std::move(data.second));
    }
  }

  /// Total file counts after processed by shuffler
  std::vector<fs::path> bin_files;
  extract_files(conf->tmpdir, bin_files);
  REQUIRE(bin_files.size() == conf->n_groups);

  read_all_data<K, V>(bin_files, kv_items);

  /// Check original data and stored data are the same
  REQUIRE_THAT(kv_items, Catch::Matchers::UnorderedEquals(dataset));
}

TEST_CASE("Shuffle", "[shuffle]") {

  SECTION("String/Int") {

    /// Target key-value dataset
    std::vector<BytePair> dataset{
      {ByteData{String{"test"}}, ByteData{Int{10}}},
      {ByteData{String{"test"}}, ByteData{Int{-1234}}},
      {ByteData{String{"example"}}, ByteData{Int{5000}}},
      {ByteData{String{"example"}}, ByteData{Int{-76543}}}
    };

    test_shuffle<String, Int>(dataset);
  }

  SECTION("String/Long") {
    /// Target key-value dataset
    std::vector<BytePair> dataset{
      {ByteData{String{"test"}}, ByteData{Long{198765432l}}},
      {ByteData{String{"test"}}, ByteData{Long{-123456789l}}},
      {ByteData{String{"example"}}, ByteData{Long{50005000l}}},
      {ByteData{String{"example"}}, ByteData{Long{-76543l}}}
    };

    test_shuffle<String, Long>(dataset);
  }

  SECTION("String/Float") {
    /// Target key-value dataset
    std::vector<BytePair> dataset{
      {ByteData{String{"test"}}, ByteData{Float{1.2345f}}},
      {ByteData{String{"test"}}, ByteData{Float{-5.4321f}}},
      {ByteData{String{"example"}}, ByteData{Float{102.5987f}}},
      {ByteData{String{"example"}}, ByteData{Float{-980.7628f}}}
    };

    test_shuffle<String, Float>(dataset);
  }

  SECTION("String/Double") {
    /// Target key-value dataset
    std::vector<BytePair> dataset{
      {ByteData{String{"test"}}, ByteData{Double{1.23456789012345}}},
      {ByteData{String{"test"}}, ByteData{Double{-5.43210987654321}}},
      {ByteData{String{"example"}}, ByteData{Double{50987.5987}}},
      {ByteData{String{"example"}}, ByteData{Double{-22902.1023072}}}
    };

    test_shuffle<String, Double>(dataset);
  }

  fs::remove_all(tmpdir);
}