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

class TestWriter : public Writer
{
 public:
  TestWriter(std::vector<BytePair> &v) : vec_(v) {}
  ~TestWriter() {}

  void write(const ByteData &key, const ByteData &value)
  {
    vec_.emplace_back(key, value);
  }

 private:
  std::vector<BytePair> &vec_;
};


TEST_CASE("Context", "[context]")
{
  SECTION("string/int")
  {
    std::vector<BytePair> vec;
    std::unique_ptr<TestWriter> writer = std::make_unique<TestWriter>(vec);

    Context<std::string, int> context(std::move(writer));
    std::string key{"test"};

    std::vector<int> values{1022, 345, -950, 0, 5578};

    for (auto &value: values)
      context.write(key, value);

    /// Check context append data to vector 
    REQUIRE(vec.size() == values.size());

    unsigned int idx = 0;
    for (auto &pair: vec)
    {
      REQUIRE(pair.first.get_data<std::string>() == key);
      REQUIRE(pair.second.get_data<int>() == values[idx++]);
    }
  }

  SECTION("string/long")
  {
    std::vector<BytePair> vec;
    std::unique_ptr<TestWriter> writer = std::make_unique<TestWriter>(vec);

    Context<std::string, long> context(std::move(writer));
    std::string key{"test"};

    std::vector<long> values{123456789l, -123456789l, 0l, 3531509l, -6911024l};

    for (auto &value: values)
      context.write(key, value);

    /// Check context append data to vector 
    REQUIRE(vec.size() == values.size());

    unsigned int idx = 0;
    for (auto &pair: vec)
    {
      REQUIRE(pair.first.get_data<std::string>() == key);
      REQUIRE(pair.second.get_data<long>() == values[idx++]);
    }
  }

  SECTION("string/float")
  {
    std::vector<BytePair> vec;
    std::unique_ptr<TestWriter> writer = std::make_unique<TestWriter>(vec);

    Context<std::string, float> context(std::move(writer));
    std::string key{"test"};

    std::vector<float> values{1022.844, 345.2, -950.45, 0, 5578.029};

    for (auto &value: values)
      context.write(key, value);

    /// Check context append data to vector 
    REQUIRE(vec.size() == values.size());

    unsigned int idx = 0;
    for (auto &pair: vec)
    {
      REQUIRE(pair.first.get_data<std::string>() == key);
      REQUIRE(pair.second.get_data<float>() == values[idx++]);
    }
  }

  SECTION("string/double")
  {
    std::vector<BytePair> vec;
    std::unique_ptr<TestWriter> writer = std::make_unique<TestWriter>(vec);

    Context<std::string, double> context(std::move(writer));
    std::string key{"test"};

    std::vector<double> values{0.123456789012345, -0.123456789012345, 140.98710222, 35315.0000913, -6911024.2345};

    for (auto &value: values)
      context.write(key, value);

    /// Check context append data to vector 
    REQUIRE(vec.size() == values.size());

    unsigned int idx = 0;
    for (auto &pair: vec)
    {
      REQUIRE(pair.first.get_data<std::string>() == key);
      REQUIRE(pair.second.get_data<double>() == values[idx++]);
    }
  }
}