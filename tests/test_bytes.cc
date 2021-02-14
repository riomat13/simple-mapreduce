#include "simplemapreduce/data/bytes.h"

#include <string>
#include <vector>

#include "catch.hpp"

using namespace mapreduce::data;

TEST_CASE("ByteData", "[byte][data]")
{
  SECTION("Constructor")
  {
    ByteData bdata1, bdata2;

    int value = 10;
    bdata1 = ByteData(value);
    REQUIRE(bdata1.get_data<int>() == 10);

    ByteData bdata(ByteData(value));
    REQUIRE(bdata1.get_data<int>() == 10);
  }

  SECTION("int")
  {
    ByteData bdata1, bdata2;
    int value = 10;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<int>() == value);

    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(20);
    REQUIRE(bdata1 < bdata2);

    ByteData bdata3{value};
    REQUIRE(bdata3.get_data<int>() == value);
    REQUIRE(bdata1 == bdata3);
    REQUIRE(bdata1.get_data<int>() == bdata3.get_data<int>());
  }

  SECTION("long")
  {
    ByteData bdata1, bdata2;
    long value = 123456789l;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<long>() == value);
    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(20l);
    REQUIRE(bdata1 > bdata2);

    ByteData bdata3{value};
    REQUIRE(bdata3.get_data<long>() == value);
    REQUIRE(bdata1 == bdata3);
    REQUIRE(bdata1.get_data<long>() == bdata3.get_data<long>());
  }

  SECTION("float")
  {
    ByteData bdata1, bdata2;
    float value = 10.234f;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<float>() == Approx(value));
    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(1.234f);
    REQUIRE(bdata1 > bdata2);

    ByteData bdata3{value};
    REQUIRE(bdata1.get_data<float>() == Approx(bdata3.get_data<float>()));
  }

  SECTION("double")
  {
    ByteData bdata1, bdata2;
    double value = 1.23456789012345;

    bdata1.set_data(value);
    REQUIRE(bdata1.get_data<double>() == Approx(value));
    bdata2.set_data(value);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data(9.123456789012345);
    REQUIRE(bdata1 < bdata2);

    ByteData bdata3{value};
    REQUIRE(bdata1.get_data<double>() == Approx(bdata3.get_data<double>()));
  }

  SECTION("string")
  {
    using namespace std::string_literals;

    ByteData bdata1, bdata2;

    bdata1.set_data(std::string("test"));
    REQUIRE(bdata1.get_data<std::string>() == "test");
    bdata2.set_data("test"s);
    REQUIRE(bdata1 == bdata2);
    bdata2.set_data("example"s);
    REQUIRE(bdata1 > bdata2);

    ByteData bdata3{"example"s};
    REQUIRE(bdata2 == bdata3);
    REQUIRE(bdata2.get_data<std::string>() == bdata3.get_data<std::string>());
  }

  SECTION("int_array")
  {
    ByteData bdata1, bdata2;

    std::vector<int> arr{1, 2, 3, 4};
    bdata1.set_data(arr.data(), arr.size());

    std::vector<int> res = bdata1.get_data<std::vector<int>>();
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(arr));
  }

  SECTION("double_bytes_array")
  {
    ByteData bdata1, bdata2;

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