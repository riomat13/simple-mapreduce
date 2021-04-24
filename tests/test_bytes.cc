#include "simplemapreduce/data/bytes.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "catch.hpp"

#include "utils.h"

namespace fs = std::filesystem;
using namespace mapreduce::data;
using namespace mapreduce::type;

TEST_CASE("ByteData", "[byte][data]") {

  SECTION("Constructor") {
    ByteData bdata1;

    Int16 value = 10;
    bdata1 = ByteData(value);
    REQUIRE(bdata1.get_data<Int16>() == 10);

    ByteData bdata2 = ByteData(value);
    REQUIRE(bdata2.get_data<Int16>() == 10);
  }

  SECTION("Int16") {
    ByteData bdata1, bdata2;
    Int16 value = 10;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Int16>() == value);

    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    REQUIRE(bdata1.get_key() == bdata2.get_key());

    bdata2.set_data(Int16(20));
    REQUIRE(bdata1 < bdata2);
    REQUIRE(bdata2 > bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Int16>() < bdata2.get_data<Int16>());
    REQUIRE(bdata1.get_key() != bdata2.get_key());

    bdata2.set_data(Int16(-20));
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_key() != bdata2.get_key());


    ByteData bdata3{value};
    REQUIRE(bdata3.get_data<Int16>() == value);
    REQUIRE(bdata1 == bdata3);
    REQUIRE(bdata1.get_data<Int16>() == bdata3.get_data<Int16>());
    REQUIRE(bdata1.get_key() == bdata3.get_key());
  }

  SECTION("Int32") {
    ByteData bdata1, bdata2;
    Int32 value = 100;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Int32>() == value);

    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    REQUIRE(bdata1.get_key() == bdata2.get_key());

    Int32 value2 = 200;
    bdata2.set_data(value2);
    REQUIRE(bdata2.get_data<Int32>() == value2);
    REQUIRE(bdata1 < bdata2);
    REQUIRE(bdata2 > bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Int32>() < bdata2.get_data<Int32>());
    REQUIRE(bdata1.get_key() != bdata2.get_key());

    ByteData bdata3{value};
    REQUIRE(bdata3.get_data<Int32>() == value);
    REQUIRE(bdata1 == bdata3);
    REQUIRE(bdata1.get_data<Int32>() == bdata3.get_data<Int32>());
    REQUIRE(bdata1.get_key() == bdata3.get_key());
  }

  SECTION("Int64") {
    ByteData bdata1, bdata2;
    Int64 value = 123456789;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Int64>() == value);

    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    REQUIRE(bdata1.get_key() == bdata2.get_key());

    bdata2.set_data(Int64(20));
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_key() != bdata2.get_key());

    ByteData bdata3{value};
    REQUIRE(bdata3.get_data<long>() == value);
    REQUIRE(bdata1 == bdata3);
    REQUIRE(bdata1.get_data<long>() == bdata3.get_data<long>());
    REQUIRE(bdata1.get_key() == bdata3.get_key());
  }

  SECTION("Float") {
    ByteData bdata1, bdata2;
    Float value = 10.234f;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Float>() == Approx(value));

    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    REQUIRE(bdata1.get_key() == bdata2.get_key());

    bdata2.set_data(1.234f);
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Float>() > bdata2.get_data<Float>());
    REQUIRE(bdata1.get_key() != bdata2.get_key());

    bdata2.set_data(-600.1234f);
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Float>() > bdata2.get_data<Float>());

    ByteData bdata3{value};
    REQUIRE(bdata1.get_data<Float>() == Approx(bdata3.get_data<Float>()));
    REQUIRE(bdata1.get_key() == bdata3.get_key());
  }

  SECTION("Double") {
    ByteData bdata1, bdata2;
    Double value = 1.23456789012345;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Double>() == Approx(value));

    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    REQUIRE(bdata1.get_key() == bdata2.get_key());

    bdata2.set_data(9.123456789012345);
    REQUIRE(bdata1 < bdata2);
    REQUIRE(bdata2 > bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Double>() < bdata2.get_data<Double>());
    REQUIRE(bdata1.get_key() != bdata2.get_key());

    ByteData bdata3{value};
    REQUIRE(bdata1.get_data<Double>() == Approx(bdata3.get_data<Double>()));
    REQUIRE(bdata1.get_key() == bdata3.get_key());
  }

  SECTION("String") {
    using namespace std::string_literals;

    ByteData bdata1, bdata2;

    bdata1.set_data(String("test"));
    REQUIRE(bdata1.get_data<String>() == "test");

    bdata2.set_data("test"s);
    REQUIRE(bdata1 == bdata2);
    REQUIRE(bdata1.get_key() == bdata2.get_key());

    bdata2.set_data("example"s);
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_key() != bdata2.get_key());

    ByteData bdata3{"example"s};
    REQUIRE(bdata2 == bdata3);
    REQUIRE(bdata2.get_data<String>() == bdata3.get_data<String>());
    REQUIRE(bdata2.get_key() == bdata3.get_key());
  }

  SECTION("Int16 array") {
    ByteData bdata1, bdata2;

    std::vector<Int16> arr{1, 2, 3, 4};
    bdata1.set_data(arr.data(), arr.size());

    std::vector<Int16> res = bdata1.get_data<std::vector<Int16>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(arr));
  }

  SECTION("Int array") {
    ByteData bdata1, bdata2;

    std::vector<Int> arr{1, 2, 3, 4};
    bdata1.set_data(arr.data(), arr.size());

    std::vector<Int> res = bdata1.get_data<std::vector<Int>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(arr));
  }

  SECTION("Long array") {
    ByteData bdata1, bdata2;

    std::vector<Long> arr{1, 2, 3, 4};
    bdata1.set_data(arr.data(), arr.size());

    std::vector<Long> res = bdata1.get_data<std::vector<Long>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(arr));
  }

  SECTION("Float array") {
    ByteData bdata1, bdata2;

    std::vector<Float> arr{1.23, 2.34, -3.45, 4.56};
    bdata1.set_data(arr.data(), arr.size());

    std::vector<Float> res = bdata1.get_data<std::vector<Float>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(arr));
  }

  SECTION("Double array") {
    ByteData bdata1, bdata2;

    std::vector<Double> arr{1.23, 2.34, -3.45, 4.56};
    bdata1.set_data(arr.data(), arr.size());

    std::vector<Double> res = bdata1.get_data<std::vector<Double>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(arr));
  }

  SECTION("Double bytes array") {
    ByteData bdata1, bdata2;

    /// Create byte array to store
    std::vector<Double> target{1.23456, 2.34567, -3.45678, 4.56789};
    char *buff = reinterpret_cast<char *>(target.data());
    std::vector<char> buffer(buff, buff + target.size() * sizeof(Double));

    /// Store original bytes
    bdata1.set_bytes<Double>(buffer.data(), buffer.size());

    /// Extract data as array
    std::vector<Double> res = bdata1.get_data<std::vector<Double>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target));
  }
}

TEST_CASE("ByteData CompositeKey", "[byte][data][pair]") {

  SECTION("Int/Int pair") {
    using KeyType = CompositeKey<Int, Int>;

    KeyType pair(123, 456);
    ByteData bdata(std::move(pair));

    REQUIRE(bdata.size() == 2);

    auto [first, second] = bdata.get_data<KeyType>();

    REQUIRE(first == 123);
    REQUIRE(second == 456);

    ByteData bdata2(KeyType(123, 1111));
    REQUIRE(bdata.get_key() == bdata2.get_key());

    ByteData bdata3(KeyType(124, 1111));
    REQUIRE(bdata.get_key() != bdata3.get_key());
  }

  SECTION("String/Long pair") {
    using KeyType = CompositeKey<String, Long>;

    String word{"composite"};
    KeyType pair(String(word), 1234567890);
    ByteData bdata(std::move(pair));

    REQUIRE(bdata.size() == 2);

    auto [first, second] = bdata.get_data<KeyType>();

    REQUIRE(first == word);
    REQUIRE(second == 1234567890l);

    ByteData bdata2(KeyType(String(word), 1111));
    REQUIRE(bdata.get_key() == bdata2.get_key());

    ByteData bdata3(KeyType(String{"test"}, 1111));
    REQUIRE(bdata.get_key() != bdata3.get_key());
  }

  SECTION("Int16/Float pair") {
    using KeyType = CompositeKey<Int16, Float>;

    KeyType pair(123, -123.456);
    ByteData bdata(std::move(pair));

    REQUIRE(bdata.size() == 2);

    auto [first, second] = bdata.get_data<KeyType>();

    REQUIRE(first == 123);
    REQUIRE(second == Approx(-123.456));

    ByteData bdata2(KeyType(123, 1.111));
    REQUIRE(bdata.get_key() == bdata2.get_key());

    ByteData bdata3(KeyType(124, 1.111));
    REQUIRE(bdata.get_key() != bdata3.get_key());
  }

  SECTION("Long/Double pair") {
    using KeyType = CompositeKey<Long, Double>;

    KeyType pair(123, -123.456);
    ByteData bdata(std::move(pair));

    REQUIRE(bdata.size() == 2);

    auto [first, second] = bdata.get_data<KeyType>();

    REQUIRE(first == 123);
    REQUIRE(second == Approx(-123.456));

    ByteData bdata2(KeyType(123, 1.111));
    REQUIRE(bdata.get_key() == bdata2.get_key());

    ByteData bdata3(KeyType(124, 1.111));
    REQUIRE(bdata.get_key() != bdata3.get_key());
  }

  SECTION("Long/String pair") {
    using KeyType = CompositeKey<Long, String>;

    String str{"composite"};
    KeyType pair(123, String(str));
    ByteData bdata(std::move(pair));

    REQUIRE(bdata.size() == 2);

    auto [first, second] = bdata.get_data<KeyType>();

    REQUIRE(first == 123);
    REQUIRE(second == str);

    ByteData bdata2(KeyType(123, String("test")));
    REQUIRE(bdata.get_key() == bdata2.get_key());

    ByteData bdata3(KeyType(124, String("sample")));
    REQUIRE(bdata.get_key() != bdata3.get_key());
  }

  SECTION("String/String pair") {
    using KeyType = CompositeKey<String, String>;

    String str1{"key"};
    String str2{"value"};
    ByteData bdata{KeyType(String(str1), String(str2))};

    REQUIRE(bdata.size() == 2);

    auto [first, second] = bdata.get_data<KeyType>();

    REQUIRE(first == str1);
    REQUIRE(second == str2);

    ByteData bdata2(KeyType(String(str1), String("test")));
    REQUIRE(bdata.get_key() == bdata2.get_key());

    ByteData bdata3(KeyType(String(str2), String("sample")));
    REQUIRE(bdata.get_key() != bdata3.get_key());
  }
}

TEST_CASE("ByteData File", "[byte][data][file]") {
  fs::path fpath = tmpdir / "test_bytes" / "file";
  fs::create_directories(fpath.parent_path());

  /// Write sample test data to file
  {
    std::ofstream ofs(fpath);
    ofs << "filetest";
    ofs.close();
  }

  ByteData bdata;
  bdata.read_file(fpath);
  REQUIRE(bdata.get_data<String>() == "filetest");

  fs::remove_all(tmpdir);
}

TEST_CASE("ByteData Modification", "[byte][data]") {

  SECTION("Int32 array") {
    std::vector<Int32> target{1, 10, 15, 25, -10};
    ByteData bdata;
    for (auto& val: target)
      bdata.push_back(val);

    std::vector<Int32> res = bdata.get_data<std::vector<Int32>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target));
  }

  SECTION("Float array") {
    std::vector<Float> target{1.4, -30.5, 22, 5.4};
    ByteData bdata;
    for (auto& val: target)
      bdata.push_back(val);

    std::vector<Float> res = bdata.get_data<std::vector<Float>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target));
  }
}