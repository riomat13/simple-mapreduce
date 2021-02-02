#include "simplemapreduce/proc/writer.h"

#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "catch.hpp"

#include "utils.h"
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

  std::ifstream &get_stream() { return ifs; }

 protected:
  std::ifstream ifs;
};

/**
 * Wrapper class of file stream for read.
 */
class FileIStream : public BaseFileIStream
{
 public:
  FileIStream(const fs::path &path)
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
  BinFileIStream(const fs::path &path)
  {
    ifs.open(path, std::ios::binary);
  }
};

/**
 * Read data from binary file.
 *
 *  @param path& target binary file path
 */
template <typename V>
std::pair<std::string, V> read_binary(const fs::path &path)
{
  BinFileIStream ifs(path);

  size_t keysize;
  ifs.get_stream().read(reinterpret_cast<char *>(&keysize), sizeof(size_t));

  char kw[keysize];
  ifs.get_stream().read(kw, sizeof(char) * keysize);
  std::string key(kw, keysize);

  V value;
  ifs.get_stream().read(reinterpret_cast<char *>(&value), sizeof(V));

  return std::make_pair<std::string, V>(std::move(key), std::move(value));
}

TEST_CASE("BinaryFileWriter", "[writer]")
{
  fs::create_directory(testdir);
  fs::path fpath{"binout"};

  std::string key{"test"};

  SECTION("write_string_int")
  {
    int val = 1;
    {
      BinaryFileWriter writer(testdir / fpath);
      std::string kw(key);
      writer.write(kw, val);
    }

    /// Check read key and value pair matchs items writtin by the writer
    std::pair<std::string, int> res = read_binary<int>(testdir / fpath);
    REQUIRE(res.first == key);
    REQUIRE(res.second == val);
  }

  SECTION("write_string_long")
  {
    long val = 1234567890;
    {
      BinaryFileWriter writer(testdir / fpath);
      std::string kw(key);
      writer.write(kw, val);
    }

    /// Check read key and value pair matchs items writtin by the writer
    std::pair<std::string, long> res = read_binary<long>(testdir / fpath);
    REQUIRE(res.first == key);
    REQUIRE(res.second == val);
  }

  SECTION("write_string_float")
  {
    float val = 0.1;
    {
      BinaryFileWriter writer(testdir / fpath);
      std::string kw(key);
      writer.write(kw, val);
    }

    /// Check read key and value pair matchs items writtin by the writer
    std::pair<std::string, float> res = read_binary<float>(testdir / fpath);
    REQUIRE(res.first == key);
    REQUIRE(res.second == val);
  }

  SECTION("write_string_double")
  {
    double val = 1.23456789;
    {
      BinaryFileWriter writer(testdir / fpath);
      std::string kw(key);
      writer.write(kw, val);
    }

    /// Check read key and value pair matchs items writtin by the writer
    std::pair<std::string, double> res = read_binary<double>(testdir / fpath);
    REQUIRE(res.first == key);
    REQUIRE(res.second == val);
  }
}

template <typename Writer, typename MQ, typename KW, typename ValueType>
void mqwriter_test(Writer &writer, MQ &mq, KW &keywords, ValueType &value)
{
  /// Use copy string since the string is moved by the implementation
  for(auto kw: keywords)
    writer.write(kw, value);

  for(auto &kw: keywords)
  {
    auto item = mq->receive();
    REQUIRE(item.first == kw);
    REQUIRE(item.second == value);
  }
}

TEST_CASE("MQWriter", "[writer]")
{
  std::vector<std::string> keywords{"test", "example", "mq", "writer"};

  SECTION("write_string_int")
  {
    typedef MessageQueue<std::string, int> MQ;
    std::shared_ptr<MQ> mq = std::make_shared<MQ>();
    MQWriter<std::string, int> writer(mq);

    int val = 1;
    mqwriter_test(writer, mq, keywords, val);
  }

  SECTION("write_string_long")
  {
    typedef MessageQueue<std::string, long> MQ;
    std::shared_ptr<MQ> mq = std::make_shared<MQ>();
    MQWriter<std::string, long> writer(mq);

    long val = 1;
    mqwriter_test(writer, mq, keywords, val);
  }

  SECTION("write_string_float")
  {
    typedef MessageQueue<std::string, float> MQ;
    std::shared_ptr<MQ> mq = std::make_shared<MQ>();
    MQWriter<std::string, float> writer(mq);

    float val = 1.0;
    mqwriter_test(writer, mq, keywords, val);
  }

  SECTION("write_string_double")
  {
    typedef MessageQueue<std::string, double> MQ;
    std::shared_ptr<MQ> mq = std::make_shared<MQ>();
    MQWriter<std::string, double> writer(mq);

    double val = 1.0;
    mqwriter_test(writer, mq, keywords, val);
  }
}

TEST_CASE("OutputWriter", "[writer]")
{
  fs::create_directory(testdir);
  std::string fname{"tmpout"};
  std::string key{"test"};

  SECTION("write_string_int")
  {
    int value = 1;

    {
      OutputWriter writer(testdir / fname);
      std::string key_(key);
      writer.write(key_, value);
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

  SECTION("write_string_long")
  {
    long value = 1234567890;

    {
      OutputWriter writer(testdir / fname);
      std::string key_(key);
      writer.write(key_, value);
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

  SECTION("write_string_float")
  {
    float value = 0.9;

    {
      OutputWriter writer(testdir / fname);
      std::string key_(key);
      writer.write(key_, value);
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

  SECTION("write_string_double")
  {
    double value = 10.123456789012345;

    {
      OutputWriter writer(testdir / fname);
      std::string key_(key);
      writer.write(key_, value);
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
}