#include "simplemapreduce/proc/writer.h"

#include <cstdlib>
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
#include "simplemapreduce/data/type.h"

namespace fs = std::filesystem;

using UINT = std::uint64_t;
using namespace mapreduce::data;
using namespace mapreduce::proc;
using namespace mapreduce::type;

/**
 * Abstract wrapper class of file stream for read.
 */
class BaseFileIStream {
 public:
  ~BaseFileIStream() {
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
class FileIStream : public BaseFileIStream {
 public:
  FileIStream(const fs::path& path) {
    ifs.open(path);
  }
};

/**
 * Wrapper class of binary file stream for read.
 */
class BinFileIStream : public BaseFileIStream {
 public:
  BinFileIStream(const fs::path& path) {
    ifs.open(path, std::ios::binary);
  }
};

/**
 * Abstract wrapper class of file stream for write.
 */
class BaseFileOStream {
 public:
  ~BaseFileOStream() {
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
class FileOStream : public BaseFileOStream {
 public:
  FileOStream(const fs::path& path) {
    ofs.open(path);
  }
};

/**
 * Wrapper class of binary file stream for read.
 */
class BinFileOStream : public BaseFileOStream {
 public:
  BinFileOStream(const fs::path& path) {
    ofs.open(path, std::ios::binary);
  }
};

TEST_CASE("write_binary", "[write][binary]") {
  fs::path fpath{tmpdir / "test_writer" / "write_binary"};
  fs::create_directories(fpath.parent_path());

  SECTION("String") {
    String target{"test"};
    {
      FileOStream fos{fpath};
      ByteData bdata{String(target)};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{fpath};
      auto res = read_binary<String>(fis.get_stream());
      REQUIRE(res.get_data<String>() == target);
    }
  }

  SECTION("Int16") {
    Int16 target{123};
    {
      FileOStream fos{fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{fpath};
      auto res = read_binary<Int16>(fis.get_stream());
      REQUIRE(res.get_data<Int16>() == target);
    }
  }

  SECTION("Int") {
    Int target{1234};
    {
      FileOStream fos{fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{fpath};
      auto res = read_binary<Int>(fis.get_stream());
      REQUIRE(res.get_data<Int>() == target);
    }
  }

  SECTION("Long") {
    Long target{123456789l};
    {
      FileOStream fos{fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{fpath};
      auto res = read_binary<Long>(fis.get_stream());
      REQUIRE(res.get_data<Long>() == target);
    }
  }

  SECTION("Float") {
    Float target{0.12345f};
    {
      FileOStream fos{fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{fpath};
      auto res = read_binary<Float>(fis.get_stream());
      REQUIRE(res.get_data<Float>() == target);
    }
  }

  SECTION("Double") {
    Double target{12340.123456789};
    {
      FileOStream fos{fpath};
      ByteData bdata{target};
      write_binary(fos.get_stream(), std::move(bdata));
    }
    {
      FileIStream fis{fpath};
      auto res = read_binary<Double>(fis.get_stream());
      REQUIRE(res.get_data<Double>() == target);
    }
  }

  SECTION("CompositeKey<String, Long>") {
    using KeyType = CompositeKey<String, Long>;
    String primary{"composite"};
    Long val{123456789};
    KeyType target(String(primary), val);

    {
      FileOStream fos{fpath};
      write_binary(fos.get_stream(), target);
    }
    {
      FileIStream fis{fpath};
      auto res = read_binary<KeyType>(fis.get_stream());
      auto pair = res.get_data<KeyType>();
      REQUIRE(pair == target);
    }
  }

  SECTION("CompositeKey<Int32, Int16>") {
    using KeyType = CompositeKey<Int32, Int16>;
    KeyType target(123, -10);

    {
      FileOStream fos{fpath};
      write_binary(fos.get_stream(), target);
    }
    {
      FileIStream fis{fpath};
      auto res = read_binary<KeyType>(fis.get_stream());
      REQUIRE(res.get_data<KeyType>() == target);
    }
  }
}

TEST_CASE("BinaryFileWriter", "[writer]") {
  fs::path fpath{tmpdir / "test_writer" / "binout"};
  fs::create_directories(fpath.parent_path());

  String key{"binary"};

  SECTION("write String/Int") {
    Int32 val = 1;
    {
      BinaryFileWriter<String, Int> writer(fpath);
      writer.write(ByteData{String(key)}, ByteData{val});
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(fpath);
    auto k = read_binary<String>(fis.get_stream());
    auto v = read_binary<Int32>(fis.get_stream());
    REQUIRE(k.get_data<String>() == key);
    REQUIRE(v.get_data<Int32>() == val);
  }

  SECTION("write String/Long") {
    Long val = 123456789l;
    {
      BinaryFileWriter<std::string, Long> writer(fpath);
      ByteData key_{String(key)}, value_(val);
      writer.write(std::move(key_), std::move(value_));
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(fpath);
    auto k = read_binary<String>(fis.get_stream());
    auto v = read_binary<Long>(fis.get_stream());
    REQUIRE(k.get_data<String>() == key);
    REQUIRE(v.get_data<Long>() == val);
  }

  SECTION("write String/Float") {
    Float val = 0.1;
    {
      BinaryFileWriter<String, Float> writer(fpath);
      ByteData key_{String(key)}, value_(val);
      writer.write(std::move(key_), std::move(value_));
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(fpath);
    auto k = read_binary<String>(fis.get_stream());
    auto v = read_binary<Float>(fis.get_stream());
    REQUIRE(k.get_data<String>() == key);
    REQUIRE(v.get_data<Float>() == val);
  }

  SECTION("write String/Double") {
    Double val = 1.23456789;
    {
      BinaryFileWriter<String, Double> writer(fpath);
      ByteData key_{String(key)}, value_(val);
      writer.write(std::move(key_), std::move(value_));
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(fpath);
    auto k = read_binary<String>(fis.get_stream());
    auto v = read_binary<Double>(fis.get_stream());
    REQUIRE(k.get_data<String>() == key);
    REQUIRE(v.get_data<Double>() == val);
  }

  SECTION("write Long/Double") {
    Long key = 123456789l;
    Double val = 1.23456789;
    {
      BinaryFileWriter<Long, Double> writer(fpath);
      ByteData key_(key), value_(val);
      writer.write(std::move(key_), std::move(value_));
    }

    /// Check read key and value pair matchs items writtin by the writer
    BinFileIStream fis(fpath);
    auto k = read_binary<Long>(fis.get_stream());
    auto v = read_binary<Double>(fis.get_stream());
    REQUIRE(k.get_data<Long>() == key);
    REQUIRE(v.get_data<Double>() == val);
  }

  fs::remove_all(tmpdir);
}

template <typename Writer, typename MQ, typename K, typename V>
bool mqwriter_test(Writer& writer, MQ& mq,
                   std::vector<K>& keys, std::vector<V>& values) {
  /// Use copy string since the string is moved by the implementation
  for(auto& kw: keys) {
    for (auto& val: values) {
      ByteData key{std::string(kw)}, value(std::move(val));
      writer.write(std::move(key), std::move(value));
    }
  }

  for(auto& kw: keys) {
    for (auto& val: values) {
      BytePair item = mq->receive();
      if (item.first.get_data<K>() != kw || item.second.get_data<V>() != val)
        return false;
    }
  }
  return true;
}

TEST_CASE("MQWriter", "[writer]") {
  typedef MessageQueue MQ;
  std::vector<std::string> keys{"test", "example", "mq", "writer"};

  std::shared_ptr<MQ> mq = std::make_shared<MQ>();
  MQWriter writer(mq);

  SECTION("write String/Int") {
    std::vector<Int> values{1, 2, 4};
    REQUIRE(mqwriter_test(writer, mq, keys, values));
  }

  SECTION("write String/Long") {
    std::vector<Long> values{123019, -223910, 45555};
    REQUIRE(mqwriter_test(writer, mq, keys, values));
  }

  SECTION("write String/Float") {
    std::vector<Float> values{-1.12, 10.595, 3.14};
    REQUIRE(mqwriter_test(writer, mq, keys, values));
  }

  SECTION("write String/Double") {
    std::vector<Double> values{-1.12, 10.595, 3.14};
    REQUIRE(mqwriter_test(writer, mq, keys, values));
  }
}

TEST_CASE("OutputWriter", "[writer]") {
  fs::path fpath = tmpdir / "writer" / "tmpout";
  fs::create_directories(fpath.parent_path());
  std::string key{"test"};

  SECTION("write String/Int") {
    Int value = 1;

    {
      OutputWriter<String, Int> writer(fpath);
      ByteData key_{String(key)}, value_(value);
      writer.write(std::move(key_), std::move(value_));
    }

    FileIStream fis(fpath);
    std::string line;
    while (std::getline(fis.get_stream(), line)) {
      String k;
      Int v;
      std::istringstream linestream(line);
      linestream >> k >> v;
      REQUIRE(k == key);
      REQUIRE(v == value);
    }
  }

  SECTION("write String/Long") {
    Long value = 123456789l;

    {
      OutputWriter<String, Long> writer(fpath);
      ByteData key_{String(key)}, value_(value);
      writer.write(std::move(key_), std::move(value_));
    }

    FileIStream fis(fpath);
    std::string line;
    while (std::getline(fis.get_stream(), line)) {
      String k;
      Long v;
      std::istringstream linestream(line);
      linestream >> k >> v;
      REQUIRE(k == key);
      REQUIRE(v == value);
    }
  }

  SECTION("write String/Float") {
    Float value = 0.9;

    {
      OutputWriter<String, Float> writer(fpath);
      ByteData key_{String(key)}, value_(value);
      writer.write(std::move(key_), std::move(value_));
    }

    FileIStream fis(fpath);
    std::string line;
    while (std::getline(fis.get_stream(), line)) {
      String k;
      Float v;
      std::istringstream linestream(line);
      linestream >> k >> v;
      REQUIRE(k == key);
      REQUIRE(v == Approx(value));
    }
  }

  SECTION("write String/Double") {
    Double value = 10.123456789012345;

    {
      OutputWriter<String, Double> writer(fpath);
      ByteData key_{String(key)}, value_(value);
      writer.write(std::move(key_), std::move(value_));
    }

    FileIStream fis(fpath);
    std::string line;
    while (std::getline(fis.get_stream(), line)) {
      String k;
      Double v;
      std::istringstream linestream(line);
      linestream >> k >> v;
      REQUIRE(k == key);
      REQUIRE(v == Approx(value));
    }
  }

  fs::remove_all(tmpdir);
}