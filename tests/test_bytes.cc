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
    ByteData bdata1, bdata2;

    Int16 value = 10;
    bdata1 = ByteData(value);
    REQUIRE(bdata1.get_data<Int16>() == 10);

    ByteData bdata(ByteData(value));
    REQUIRE(bdata1.get_data<Int16>() == 10);
  }

  SECTION("int16") {
    ByteData bdata1, bdata2;
    Int16 value = 10;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Int16>() == value);

    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(Int16(20));
    REQUIRE(bdata1 < bdata2);
    REQUIRE(bdata2 > bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Int16>() < bdata2.get_data<Int16>());
    bdata2.set_data(Int16(-20));
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);

    ByteData bdata3{value};
    REQUIRE(bdata3.get_data<Int16>() == value);
    REQUIRE(bdata1 == bdata3);
    REQUIRE(bdata1.get_data<Int16>() == bdata3.get_data<Int16>());
  }

  SECTION("int32") {
    ByteData bdata1, bdata2;
    Int32 value = 100;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Int32>() == value);

    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    Int32 value2 = 200;
    bdata2.set_data(value2);
    REQUIRE(bdata2.get_data<Int32>() == value2);
    REQUIRE(bdata1 < bdata2);
    REQUIRE(bdata2 > bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Int32>() < bdata2.get_data<Int32>());

    ByteData bdata3{value};
    REQUIRE(bdata3.get_data<Int32>() == value);
    REQUIRE(bdata1 == bdata3);
    REQUIRE(bdata1.get_data<Int32>() == bdata3.get_data<Int32>());
  }

  SECTION("int64") {
    ByteData bdata1, bdata2;
    Int64 value = 123456789;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Int64>() == value);
    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(Int64(20));
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);

    ByteData bdata3{value};
    REQUIRE(bdata3.get_data<long>() == value);
    REQUIRE(bdata1 == bdata3);
    REQUIRE(bdata1.get_data<long>() == bdata3.get_data<long>());
  }

  SECTION("float") {
    ByteData bdata1, bdata2;
    Float value = 10.234f;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Float>() == Approx(value));
    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(1.234f);
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Float>() > bdata2.get_data<Float>());
    bdata2.set_data(-600.1234f);
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Float>() > bdata2.get_data<Float>());

    ByteData bdata3{value};
    REQUIRE(bdata1.get_data<Float>() == Approx(bdata3.get_data<Float>()));
  }

  SECTION("double") {
    ByteData bdata1, bdata2;
    Double value = 1.23456789012345;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<Double>() == Approx(value));
    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(9.123456789012345);
    REQUIRE(bdata1 < bdata2);
    REQUIRE(bdata2 > bdata1);
    REQUIRE(bdata1 != bdata2);
    REQUIRE(bdata1.get_data<Double>() < bdata2.get_data<Double>());

    ByteData bdata3{value};
    REQUIRE(bdata1.get_data<Double>() == Approx(bdata3.get_data<Double>()));
  }

  SECTION("string") {
    using namespace std::string_literals;

    ByteData bdata1, bdata2;

    bdata1.set_data(String("test"));
    REQUIRE(bdata1.get_data<String>() == "test");
    bdata2.set_data("test"s);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data("example"s);
    REQUIRE(bdata1 > bdata2);
    REQUIRE(bdata2 < bdata1);
    REQUIRE(bdata1 != bdata2);

    ByteData bdata3{"example"s};
    REQUIRE(bdata2 == bdata3);
    REQUIRE(bdata2.get_data<String>() == bdata3.get_data<String>());
  }

  SECTION("int16 array") {
    ByteData bdata1, bdata2;

    std::vector<Int16> arr{1, 2, 3, 4};
    bdata1.set_data(arr.data(), arr.size());

    std::vector<Int16> res = bdata1.get_data<std::vector<Int16>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(arr));
  }

  SECTION("double bytes array") {
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

  SECTION("int32 array") {
    std::vector<Int32> target{1, 10, 15, 25, -10};
    ByteData bdata;
    for (auto& val: target)
      bdata.push_back(val);

    std::vector<Int32> res = bdata.get_data<std::vector<Int32>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target));
  }

  SECTION("float array") {
    std::vector<Float> target{1.4, -30.5, 22, 5.4};
    ByteData bdata;
    for (auto& val: target)
      bdata.push_back(val);

    std::vector<Float> res = bdata.get_data<std::vector<Float>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target));
  }
}