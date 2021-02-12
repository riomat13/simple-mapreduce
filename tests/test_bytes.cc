#include "simplemapreduce/data/bytes.h"

#include <string>
#include <vector>

#include "catch.hpp"

using namespace mapreduce::data;

TEST_CASE("ByteData", "[byte][data]")
{
  ByteData bdata1;
  ByteData bdata2;

  SECTION("Constructor")
  {
    int value = 10;
    bdata1 = ByteData(value);
    REQUIRE(bdata1.get_data<int>() == 10);

    ByteData bdata(ByteData(value));
    REQUIRE(bdata1.get_data<int>() == 10);
  }

  SECTION("int")
  {
    bdata1.set_data(10);
    REQUIRE(bdata1.get_data<int>() == 10);

    bdata2.set_data(10);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(20);
    REQUIRE(bdata1 < bdata2);
  }

  SECTION("long")
  {
    bdata1.set_data(123456789l);
    REQUIRE(bdata1.get_data<long>() == 123456789l);
    bdata2.set_data(123456789l);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(20l);
    REQUIRE(bdata1 > bdata2);
  }

  SECTION("float")
  {
    bdata1.set_data(10.234f);
    REQUIRE(bdata1.get_data<float>() == Approx(10.234f));
    bdata2.set_data(10.234f);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(1.234f);
    REQUIRE(bdata1 > bdata2);
  }

  SECTION("double")
  {
    bdata1.set_data(1.23456789012345);
    REQUIRE(bdata1.get_data<double>() == Approx(1.23456789012345));
    bdata2.set_data(1.23456789012345);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(9.123456789012345);
    REQUIRE(bdata1 < bdata2);
  }

  SECTION("string")
  {
    bdata1.set_data("test");
    REQUIRE(bdata1.get_data<std::string>() == "test");
    bdata2.set_data("test");
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data("example");
    REQUIRE(bdata1 > bdata2);
  }

  SECTION("int_array")
  {
    std::vector<int> arr{1, 2, 3, 4};
    bdata1.set_data(arr.data(), arr.size());

    std::vector<int> res = bdata1.get_data<std::vector<int>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(arr));
  }

  SECTION("double_bytes_array")
  {
    /// Create byte array to store
    std::vector<double> target{1.23456, 2.34567, -3.45678, 4.56789};
    char *buff = reinterpret_cast<char *>(target.data());
    std::vector<char> buffer(buff, buff + target.size() * sizeof(double));

    /// Store original bytes
    bdata1.set_bytes<double>(buffer.data(), buffer.size());

    /// Extract data as array
    std::vector<double> res = bdata1.get_data<std::vector<double>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target));
  }
}

TEST_CASE("ByteData Modification", "[byte][data]")
{
  SECTION("int array")
  {
    std::vector<int> target{1, 10, 15, 25, -10};
    ByteData bdata;
    for (auto& val: target)
      bdata.push_back(val);

    std::vector<int> res = bdata.get_data<std::vector<int>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target));
  }

  SECTION("float array")
  {
    std::vector<float> target{1.4, -30.5, 22, 5.4};
    ByteData bdata;
    for (auto& val: target)
      bdata.push_back(val);

    std::vector<float> res = bdata.get_data<std::vector<float>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target));
  }
}