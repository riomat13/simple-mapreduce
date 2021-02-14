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
#include "simplemapreduce/util/log.h"
using namespace mapreduce::util;

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
 * Abstract wrapper class of file stream for write.
 */
class BaseFileOStream
{
 public:
  ~BaseFileOStream()
  {
    if (ofs.is_open())
      ofs.close();
  }

  std::ofstream& get_stream() { return ofs; }

 protected:
  std::ofstream ofs;
};

/**
 * Wrapper class of file stream for write.
 */
class FileOStream : public BaseFileOStream
{
 public:
  FileOStream(const fs::path& path)
  {
    ofs.open(path);
  }
};

/**
 * Wrapper class of binary file stream for read.
 */
class BinFileOStream : public BaseFileOStream
{
 public:
  BinFileOStream(const fs::path& path)
  {
    ofs.open(path, std::ios::binary);
  }
};

/** Read binary data from file. */
template <typename T>
T read_binary(std::ifstream&);

template<typename T>
T read_binary(std::ifstream& fs)
{
  T data;
  fs.read(reinterpret_cast<char*>(&data), sizeof(T));
  return data;
}

/** Read binary data as string from file. */
std::string read_string_binary(std::ifstream& fs)
{
  size_t data_size;
  fs.read(reinterpret_cast<char*>(&data_size), sizeof(size_t));

  char data[data_size];
  fs.read(data, sizeof(char) * data_size);
  return std::string(data, data_size);
}

TEST_CASE("write_binary", "[write][binary]")
{
  fs::path testdir = tmpdir / "writer";
  fs::create_directories(testdir);
  fs::path fpath{"write_binary"};

  SECTION("ByteData(string)")
  {
    std::string target{"test"};
    {
      FileOStream fos{testdir / fpath};
      ByteData bdata{std::string(target)};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_string_binary(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("ByteData(int)")
  {
    int target{1234};
    {
      FileOStream fos{testdir / fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_binary<int>(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("ByteData(long)")
  {
    long target{123456789l};
    {
      FileOStream fos{testdir / fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_binary<long>(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("ByteData(float)")
  {
    float target{0.12345f};
    {
      FileOStream fos{testdir / fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_binary<float>(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("ByteData(double)")
  {
    double target{12340.123456789};
    {
      FileOStream fos{testdir / fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_binary<double>(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("string")
  {
    std::string target{"test"};
    {
      FileOStream fos{testdir / fpath};
      ByteData bdata{std::string(target)};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_string_binary(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("int")
  {
    int target{1234};
    {
      FileOStream fos{testdir / fpath};
      write_binary(fos.get_stream(), int(target));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_binary<int>(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("long")
  {
    long target{123456789l};
    {
      FileOStream fos{testdir / fpath};
      write_binary(fos.get_stream(), long(target));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_binary<long>(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("float")
  {
    float target{0.12345f};
    {
      FileOStream fos{testdir / fpath};
      write_binary(fos.get_stream(), float(target));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_binary<float>(fis.get_stream());
      REQUIRE(res == target);
    }
  }

  SECTION("double")
  {
    double target{12340.123456789};
    {
      FileOStream fos{testdir / fpath};
      write_binary(fos.get_stream(), double(target));
    }
    {
      FileIStream fis{testdir / fpath};
      auto res = read_binary<double>(fis.get_stream());
      REQUIRE(res == target);
    }
  }
}

TEST_CASE("BinaryFileWriter", "[writer]")
{
  fs::path testdir = tmpdir / "writer";
  fs::create_directories(testdir);
  fs::path fpath{"binout"};

  std::string key{"binary"};

  SECTION("write_string/int")
  {
    int val = 1;
    {
      BinaryFileWriter<std::string, int> writer(testdir / fpath);
      writer.write(ByteData{std::string(key)}, ByteData{val});
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(testdir / fpath);
    std::string k = read_string_binary(fis.get_stream());
    int v = read_binary<int>(fis.get_stream());
    REQUIRE(k == key);
    REQUIRE(v == val);
  }

  SECTION("write_string/long")
  {
    long val = 123456789l;
    {
      BinaryFileWriter<std::string, long> writer(testdir / fpath);
      ByteData key_{std::string(key)}, value_(val);
      writer.write(std::move(key_), std::move(value_));
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(testdir / fpath);
    std::string k = read_string_binary(fis.get_stream());
    long v = read_binary<long>(fis.get_stream());
    REQUIRE(k == key);
    REQUIRE(v == val);
  }

  SECTION("write_string/float")
  {
    float val = 0.1;
    {
      BinaryFileWriter<std::string, float> writer(testdir / fpath);
      ByteData key_{std::string(key)}, value_(val);
      writer.write(std::move(key_), std::move(value_));
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(testdir / fpath);
    std::string k = read_string_binary(fis.get_stream());
    float v = read_binary<float>(fis.get_stream());
    REQUIRE(k == key);
    REQUIRE(v == val);
  }

  SECTION("write_string/double")
  {
    double val = 1.23456789;
    {
      BinaryFileWriter<std::string, double> writer(testdir / fpath);
      ByteData key_{std::string(key)}, value_(val);
      writer.write(std::move(key_), std::move(value_));
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(testdir / fpath);
    std::string k = read_string_binary(fis.get_stream());
    double v = read_binary<double>(fis.get_stream());
    REQUIRE(k == key);
    REQUIRE(v == val);
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
      writer.write(std::move(key), std::move(value));
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
      writer.write(std::move(key_), std::move(value_));
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
      writer.write(std::move(key_), std::move(value_));
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
      writer.write(std::move(key_), std::move(value_));
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
      writer.write(std::move(key_), std::move(value_));
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