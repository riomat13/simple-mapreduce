#include "simplemapreduce/util/argparse.h"

#include <getopt.h>

#include "catch.hpp"

using namespace mapreduce;
using namespace mapreduce::util;

TEST_CASE("ArgParser", "[argparse][options][config]")
{
  /// Reset getopt
  optind = 0;

  SECTION("Input directory")
  {
    int argc = 3;
    char name[]{"test_argparse"};
    char opt[]{"--input"};
    char value[]{"./inputs"};
    char* argv[]{&name[0], &opt[0], &value[0]};
    ArgParser parser{argc, argv};

    REQUIRE(parser.get_option("input") == "./inputs");
  }

  SECTION("Input directories")
  {
    int argc = 3;
    char name[]{"test_argparse"};
    char opt[]{"--input"};
    char value[]{"./inputs1,./inputs2,./inputs3"};
    char* argv[]{&name[0], &opt[0], &value[0]};
    ArgParser parser{argc, argv};

    REQUIRE(parser.get_option("input") == "./inputs1,./inputs2,./inputs3");
  }

  SECTION("Output directory")
  {
    int argc = 3;
    char name[]{"test_argparse"};
    char opt[]{"--output"};
    char value[]{"./outputs"};
    char* argv[]{&name[0], &opt[0], &value[0]};
    ArgParser parser{argc, argv};

    REQUIRE(parser.get_option("output") == "./outputs");
  }
}