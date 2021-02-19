#include "simplemapreduce/ops/func.h"

#include <vector>

#include "catch.hpp"

TEST_CASE("REDUCE_SUM", "[sum][reduce]") {
  std::vector<int> values_i{1, 2, 3, 4, 5, -3};
  REQUIRE(REDUCE_SUM(values_i) == 12);

  std::vector<long> values_l{1234567890, 2345678901, -3456789012, 4321098765, 500, -321};
  REQUIRE(REDUCE_SUM(values_l) == 4444556723);

  std::vector<float> values_f{2.1, 0.5, 3.3, -8.2, 6.7};
  REQUIRE(REDUCE_SUM(values_f) == Approx(4.4));

  std::vector<double> values_d{4.105938500012, 2330.5055941, 138.300231, -525.24222};
  REQUIRE(REDUCE_SUM(values_d) == Approx(1947.669543600012));
}

TEST_CASE("REDUCE_AVE", "[average][reduce]") {
  std::vector<int> values_i{3, 2, 1, -2, -3};
  REQUIRE(REDUCE_MEAN(values_i) == Approx(0.2d));

  std::vector<long> values_l{444, 333, 222, 111};
  REQUIRE(REDUCE_MEAN(values_l) == Approx(277.5d));
}