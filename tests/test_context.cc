#include "simplemapreduce/ops/context.h"

#include <memory>
#include <string>
#include <vector>
#include <utility>

#include "catch.hpp"

#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/proc/writer.h"

using namespace mapreduce;
using namespace mapreduce::data;
using namespace mapreduce::proc;

/** Helper writer to test Context. */
class TestWriter : public Writer
{
 public:
  TestWriter(std::vector<BytePair>& v) : vec_(v) {}
  ~TestWriter() {}

  void write(ByteData&& key, ByteData&& value)
  {
    vec_.emplace_back(std::move(key), std::move(value));
  }

 private:
  std::vector<BytePair>& vec_;
};

/** Test runner for Context. */
template <typename K, typename V>
void context_test_runner(K key, std::vector<V> values)
{
  std::vector<BytePair> vec;
  std::unique_ptr<TestWriter> writer = std::make_unique<TestWriter>(vec);

  Context<K, V> context(std::move(writer));

  for (auto& value: values)
  {
    /// Need to copy since writer consume the data
    K key_(key);
    context.write(key_, value);
  }

  /// Check context append data to vector 
  REQUIRE(vec.size() == values.size());

  unsigned int idx = 0;
  for (auto& pair: vec)
  {
    REQUIRE(pair.first.get_data<K>() == key);
    REQUIRE(pair.second.get_data<V>() == values[idx++]);
  }
}

TEST_CASE("Context", "[context]")
{
  SECTION("string/int")
  {
    std::string key{"test"};
    std::vector<int> values{1022, 345, -950, 0, 5578};
    context_test_runner(key, values);
  }

  SECTION("string/long")
  {
    std::string key{"test"};
    std::vector<long> values{123456789l, -123456789l, 0l, 3531509l, -6911024l};
    context_test_runner(key, values);
  }

  SECTION("string/float")
  {
    std::string key{"test"};
    std::vector<float> values{1022.844, 345.2, -950.45, 0, 5578.029};
    context_test_runner(key, values);
  }

  SECTION("string/double")
  {
    std::string key{"test"};
    std::vector<double> values{0.123456789012345, -0.123456789012345, 140.98710222, 35315.0000913, -6911024.2345};
    context_test_runner(key, values);
  }

  SECTION("int/float")
  {
    int key{123};
    std::vector<float> values{1022.844, 345.2, -950.45, 0, 5578.029};
    context_test_runner(key, values);
  }

  SECTION("long/double")
  {
    long key{100000};
    std::vector<double> values{0.123456789012345, -0.123456789012345, 140.98710222, 35315.0000913, -6911024.2345};
    context_test_runner(key, values);
  }
}