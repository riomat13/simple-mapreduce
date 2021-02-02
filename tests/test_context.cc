#include "simplemapreduce/ops/context.h"

#include <memory>
#include <string>
#include <vector>
#include <utility>

#include "catch.hpp"

#include "simplemapreduce/proc/writer.h"

using namespace mapreduce;
using namespace mapreduce::proc;

template <typename K, typename V>
class TestWriter : public Writer
{
 public:
  typedef std::pair<K, V> KV;

  TestWriter(std::vector<KV> &v) : vec_(v) {}
  ~TestWriter() {}

  void write(std::string &key, int &value)
  {
    vec_.emplace_back(key, value);
  }
  void write(std::string &key, long &value)
  {
    vec_.emplace_back(key, value);
  }
  void write(std::string &key, float &value)
  {
    vec_.emplace_back(key, value);
  }
  void write(std::string &key, double &value)
  {
    vec_.emplace_back(key, value);
  }

 private:
  std::vector<KV> &vec_;
};


TEST_CASE("Context", "[context]")
{
  SECTION("string_int")
  {
    typedef std::pair<std::string, int> KV;
    typedef TestWriter<std::string, int> TWriter;

    std::vector<KV> vec;
    std::unique_ptr<TWriter> writer = std::make_unique<TWriter>(vec);

    Context context(std::move(writer));
    std::string key{"test"};

    std::vector<int> values{1022, 345, -950, 0, 5578};

    for (auto &value: values)
      context.write(key, value);

    /// Check context append data to vector 
    REQUIRE(vec.size() == values.size());

    unsigned int idx = 0;
    for (auto &pair: vec)
    {
      REQUIRE(pair.first == key);
      REQUIRE(pair.second == values[idx++]);
    }
  }

  SECTION("string_long")
  {
    typedef std::pair<std::string, long> KV;
    typedef TestWriter<std::string, long> TWriter;

    std::vector<KV> vec;
    std::unique_ptr<TWriter> writer = std::make_unique<TWriter>(vec);

    Context context(std::move(writer));
    std::string key{"test"};

    std::vector<long> values{1234567890, -1234567890, 0, 3531509, -6911024};

    for (auto &value: values)
      context.write(key, value);

    /// Check context append data to vector 
    REQUIRE(vec.size() == values.size());

    unsigned int idx = 0;
    for (auto &pair: vec)
    {
      REQUIRE(pair.first == key);
      REQUIRE(pair.second == values[idx++]);
    }
  }

  SECTION("string_float")
  {
    typedef std::pair<std::string, float> KV;
    typedef TestWriter<std::string, float> TWriter;

    std::vector<KV> vec;
    std::unique_ptr<TWriter> writer = std::make_unique<TWriter>(vec);

    Context context(std::move(writer));
    std::string key{"test"};

    std::vector<float> values{1022.844, 345.2, -950.45, 0, 5578.029};

    for (auto &value: values)
      context.write(key, value);

    /// Check context append data to vector 
    REQUIRE(vec.size() == values.size());

    unsigned int idx = 0;
    for (auto &pair: vec)
    {
      REQUIRE(pair.first == key);
      REQUIRE(pair.second == values[idx++]);
    }
  }

  SECTION("string_double")
  {
    typedef std::pair<std::string, double> KV;
    typedef TestWriter<std::string, double> TWriter;

    std::vector<KV> vec;
    std::unique_ptr<TWriter> writer = std::make_unique<TWriter>(vec);

    Context context(std::move(writer));
    std::string key{"test"};

    std::vector<double> values{0.123456789012345, -0.123456789012345, 140.98710222, 35315.0000913, -6911024.2345};

    for (auto &value: values)
      context.write(key, value);

    /// Check context append data to vector 
    REQUIRE(vec.size() == values.size());

    unsigned int idx = 0;
    for (auto &pair: vec)
    {
      REQUIRE(pair.first == key);
      REQUIRE(pair.second == values[idx++]);
    }
  }
}