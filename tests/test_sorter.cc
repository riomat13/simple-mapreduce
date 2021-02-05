#include "simplemapreduce/proc/sorter.h"

#include <array>
#include <map>
#include <string>
#include <vector>

#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/ops/conf.h"
#include "simplemapreduce/proc/writer.h"

using namespace mapreduce;
using namespace mapreduce::proc;

/**
 * Compare written items and items read from files.
 * 
 *  @param arr&       array of vectors storing target values
 *  @param map&       map stored sorted data
 *  @param keywords&  keywords vector to get corresponding values from map
 */
template <class Arrays, class Map, class K>
void check_tuple_items(const Arrays &arr, const Map &map, const K &keywords)
{
  /// Check map holds same number of the keywords
  REQUIRE(map.size() == keywords.size());

  /// Compare each values in a keyword with mapped values
  for (size_t i = 0; i < keywords.size(); ++i)
  {
    auto item = map.find(keywords[i]);
    REQUIRE(item->second.size() == arr[i].size());

    for (size_t idx = 0; idx < arr.size(); ++idx)
      REQUIRE(item->second[idx] == arr[i][idx]);
  }
}

TEST_CASE("test_grouping_items", "[sorter]")
{
  JobConf conf;
  conf.n_groups = 1;
  conf.worker_rank = 0;
  conf.worker_size = 1;
  conf.tmpdir = testdir / "test_sorter";

  fs::create_directories(conf.tmpdir);
  fs::path fname{"0000-00000"};

  std::vector<std::string> keywords{"test", "example", "sort"};

  SECTION("long_values")
  {
    clear_file(conf.tmpdir / fname);

    /// Used for value check
    std::vector<std::array<long, 3>> values;

    /// Write binary data to a target file
    {
      BinaryFileWriter writer(conf.tmpdir / fname);

      for (size_t i = 0; i < keywords.size(); ++i)
      {
        long val1 = i + 1;
        long val2 = i + 1234567890;
        long val3 = i - 1234567890;
        writer.write(keywords[i], val1);
        writer.write(keywords[i], val2);
        writer.write(keywords[i], val3);

        std::array<long, 3> vals{val1, val2, val3};
        values.push_back(std::move(vals));
      }
    }

    /// Run sort task and group by the keys
    Sorter<std::string, long> sorter(conf);
    auto res = sorter.run();

    check_tuple_items(values, res, keywords);
  }

  SECTION("int_values")
  {
    clear_file(conf.tmpdir / fname);

    /// Used for value check
    std::vector<std::array<int, 3>> values;

    /// Write binary data to a target file
    {
      BinaryFileWriter writer(conf.tmpdir / fname);

      for (size_t i = 0; i < keywords.size(); ++i)
      {
        int val1 = i + 1;
        int val2 = i + 4321;
        int val3 = i - 1234;
        writer.write(keywords[i], val1);
        writer.write(keywords[i], val2);
        writer.write(keywords[i], val3);

        std::array<int, 3> vals{val1, val2, val3};
        values.push_back(std::move(vals));
      }
    }

    /// Run sort task and group by the keys
    Sorter<std::string, int> sorter(conf);
    auto res = sorter.run();

    check_tuple_items(values, res, keywords);
  }

  SECTION("float_values")
  {
    clear_file(conf.tmpdir / fname);

    /// Used for value check
    std::vector<std::array<float, 3>> values;

    /// Write binary data to a target file
    {
      BinaryFileWriter writer(conf.tmpdir / fname);

      for (size_t i = 0; i < keywords.size(); ++i)
      {
        float val1 = i + 0.1;
        float val2 = i + 10.987;
        float val3 = i - 0.1234;
        writer.write(keywords[i], val1);
        writer.write(keywords[i], val2);
        writer.write(keywords[i], val3);

        std::array<float, 3> vals{val1, val2, val3};
        values.push_back(std::move(vals));
      }
    }

    /// Run sort task and group by the keys
    Sorter<std::string, float> sorter(conf);
    auto res = sorter.run();

    check_tuple_items(values, res, keywords);
  }

  SECTION("double_values")
  {
    clear_file(conf.tmpdir / fname);

    /// Used for value check
    std::vector<std::array<double, 3>> values;

    /// Write binary data to a target file
    {
      BinaryFileWriter writer(conf.tmpdir / fname);

      for (size_t i = 0; i < keywords.size(); ++i)
      {
        double val1 = i + 0.5;
        double val2 = i + 1.23456789;
        double val3 = i - 9.87654321;
        writer.write(keywords[i], val1);
        writer.write(keywords[i], val2);
        writer.write(keywords[i], val3);

        std::array<double, 3> vals{val1, val2, val3};
        values.push_back(std::move(vals));
      }
    }

    /// Run sort task and group by the keys
    Sorter<std::string, double> sorter(conf);
    auto res = sorter.run();

    check_tuple_items(values, res, keywords);
  }

  fs::remove_all(testdir);
}