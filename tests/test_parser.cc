#include "simplemapreduce/util/parser.h"

#include <string>
#include <vector>

#include "catch.hpp"

using namespace mapreduce::util;

TEST_CASE("parse_string", "[parser][string][vector]") {
  auto res = parse_string("test,sample,mapreduce");
  REQUIRE_THAT(res, Catch::Matchers::Equals(std::vector<std::string>{"test", "sample", "mapreduce"}));
}