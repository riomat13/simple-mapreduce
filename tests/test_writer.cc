#include "simplemapreduce/proc/writer.h"

#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/queue.h"

namespace fs = std::filesystem;

using namespace mapreduce::proc;

/**
 * Abstract wrapper class of file stream for read.
 */
class BaseFileIStream
{
 public:
  ~BaseFileIStream()
  {
    if (ifs.is_open())
      ifs.close();
  }

  std::ifstream& get_stream() { return ifs; }

 protected:
  std::ifstream ifs;
};

/**
 * Wrapper class of file stream for read.
 */
class FileIStream : public BaseFileIStream
{
 public:
  FileIStream(const fs::path& path)
  {
    ifs.open(path);
  }
};

/**
 * Wrapper class of binary file stream for read.
 */
class BinFileIStream : public BaseFileIStream
{
 public:
  BinFileIStream(const fs::path& path)
  {
    ifs.open(path, std::ios::binary);
  }
};

/**
 * Read data from binary file.
 *
 *  @param path   target binary file path
 */
template <typename V>
std::pair<std::string, V> read_binary(const fs::path& path)
{
  BinFileIStream ifs(path);

  size_t keysize;
  ifs.get_stream().read(reinterpret_cast<char*>(&keysize), sizeof(size_t));

  char kw[keysize];
  ifs.get_stream().read(kw, sizeof(char) * keysize);
  std::string key(kw, keysize);

  V value;
  ifs.get_stream().read(reinterpret_cast<char*>(&value), sizeof(V));

  return std::make_pair<std::string, V>(std::move(key), std::move(value));
}

TEST_CASE("BinaryFileWriter", "[writer]")
{
  fs::path testdir = tmpdir / "writer";
  fs::create_directories(testdir);
  fs::path fpath{"binout"};

  std::string key{"test"};

  SECTION("write_string/int")
  {
    int val = 1;
    {
      BinaryFileWriter<std::string, int> writer(testdir / fpath);
      writer.write(ByteData{std::string(key)}, ByteData(val));
    }

    /// Check read key and value pair matchs items writtin by the writer
    std::pair<std::string, int> res = read_binary<int>(testdir / fpath);
    REQUIRE(res.first == key);
    REQUIRE(res.second == val);
  }

  SECTION("write_string/long")
  {
    long val = 123456789l;
    {
      BinaryFileWriter<std::string, long> writer(testdir / fpath);
      ByteData key_{std::string(key)}, value_(val);
      writer.write(key_, value_);
    }

    /// Check read key and value pair matchs items writtin by the writer
    std::pair<std::string, long> res = read_binary<long>(testdir / fpath);
    REQUIRE(res.first == key);
    REQUIRE(res.second == val);
  }

  SECTION("write_string/float")
  {
    float val = 0.1;
    {
      BinaryFileWriter<std::string, float> writer(testdir / fpath);
      ByteData key_{std::string(key)}, value_(val);
      writer.write(key_, value_);
    }

    /// Check read key and value pair matchs items writtin by the writer
    std::pair<std::string, float> res = read_binary<float>(testdir / fpath);
    REQUIRE(res.first == key);
    REQUIRE(res.second == val);
  }

  SECTION("write_string/double")
  {
    double val = 1.23456789;
    {
      BinaryFileWriter<std::string, double> writer(testdir / fpath);
      ByteData key_{std::string(key)}, value_(val);
      writer.write(key_, value_);
    }

    /// Check read key and value pair matchs items writtin by the writer
    std::pair<std::string, double> res = read_binary<double>(testdir / fpath);
    REQUIRE(res.first == key);
    REQUIRE(res.second == val);
  }

  fs::remove_all(testdir);
}

template <typename Writer, typename MQ, typename K, typename V>
bool mqwriter_test(Writer& writer, MQ& mq,
                   std::vector<K>& keys, std::vector<V>& values)
{
  /// Use copy string since the string is moved by the implementation
  for(auto& kw: keys)
  {
    for (auto& val: values)
    {
      ByteData key{std::string(kw)}, value(std::move(val));
      writer.write(key, value);
    }
  }

  for(auto& kw: keys)
  {
    for (auto& val: values)
    {
      BytePair item = mq->receive();
      if (item.first.get_data<K>() != kw || item.second.get_data<V>() != val)
        return false;
    }
  }
  return true;
}

TEST_CASE("MQWriter", "[writer]")
{
  typedef MessageQueue MQ;
  std::vector<std::string> keys{"test", "example", "mq", "writer"};

  std::shared_ptr<MQ> mq = std::make_shared<MQ>();
  MQWriter writer(mq);

  SECTION("write_string/int")
  {
    std::vector<int> values{1, 2, 4};
    REQUIRE(mqwriter_test(writer, mq, keys, values));
  }

  SECTION("write_string/long")
  {
    std::vector<long> values{123019, -223910, 45555};
    REQUIRE(mqwriter_test(writer, mq, keys, values));
  }

  SECTION("write_string/float")
  {
    std::vector<float> values{-1.12, 10.595, 3.14};
    REQUIRE(mqwriter_test(writer, mq, keys, values));
  }

  SECTION("write_string/double")
  {
    std::vector<double> values{-1.12, 10.595, 3.14};
    REQUIRE(mqwriter_test(writer, mq, keys, values));
  }
}

TEST_CASE("OutputWriter", "[writer]")
{
  fs::path testdir = tmpdir / "writer";
  fs::create_directories(testdir);
  std::string fname{"tmpout"};
  std::string key{"test"};

  SECTION("write_string/int")
  {
    int value = 1;

    {
      OutputWriter<std::string, int> writer(testdir / fname);
      ByteData key_{std::string(key)}, value_(value);
      writer.write(key_, value_);
    }

    FileIStream fis(testdir / fname);
    std::string line;
    while (std::getline(fis.get_stream(), line))
    {
      std::string k;
      int v;
      std::istringstream linestream(line);
      linestream >> k >> v;
      REQUIRE(k == key);
      REQUIRE(v == value);
    }
  }

  SECTION("write_string/long")
  {
    long value = 123456789l;

    {
      OutputWriter<std::string, long> writer(testdir / fname);
      ByteData key_{std::string(key)}, value_(value);
      writer.write(key_, value_);
    }

    FileIStream fis(testdir / fname);
    std::string line;
    while (std::getline(fis.get_stream(), line))
    {
      std::string k;
      long v;
      std::istringstream linestream(line);
      linestream >> k >> v;
      REQUIRE(k == key);
      REQUIRE(v == value);
    }
  }

  SECTION("write_string/float")
  {
    float value = 0.9;

    {
      OutputWriter<std::string, float> writer(testdir / fname);
      ByteData key_{std::string(key)}, value_(value);
      writer.write(key_, value_);
    }

    FileIStream fis(testdir / fname);
    std::string line;
    while (std::getline(fis.get_stream(), line))
    {
      std::string k;
      float v;
      std::istringstream linestream(line);
      linestream >> k >> v;
      REQUIRE(k == key);
      REQUIRE(v == value);
    }
  }

  SECTION("write_string/double")
  {
    double value = 10.123456789012345;

    {
      OutputWriter<std::string, double> writer(testdir / fname);
      ByteData key_{std::string(key)}, value_(value);
      writer.write(key_, value_);
    }

    FileIStream fis(testdir / fname);
    std::string line;
    while (std::getline(fis.get_stream(), line))
    {
      std::string k;
      double v;
      std::istringstream linestream(line);
      linestream >> k >> v;
      REQUIRE(k == key);
      REQUIRE(v == value);
    }
  }

  fs::remove_all(testdir);
}