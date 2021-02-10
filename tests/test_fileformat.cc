#include "simplemapreduce/ops/fileformat.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <vector>

#include "catch.hpp"

#include "utils.h"

namespace fs = std::filesystem;

using namespace mapreduce;

TEST_CASE("FileFormat", "[file]")
{
  FileFormat ffmt;

  SECTION("input_file_paths")
  {
    fs::path testdir = tmpdir / "test_fileformat";
    fs::create_directories(testdir);

    std::vector<fs::path> dirs{fs::path{"testdir1"}, fs::path{"testdir2"}};
    std::vector<fs::path> files{fs::path{"text1"}, fs::path{"text2"}, fs::path{"text3"}};

    std::vector<std::string> targets;

    for (auto &dir: dirs)
    {
      fs::path directory = testdir / dir;
      fs::create_directory(directory);

      for (auto &file: files)
      {
        std::ofstream ost(directory / file);
        ost.close();

        targets.push_back((directory / file).string());
      }
      
      /// Register multiple directories
      ffmt.add_input_path(directory);
    }

    std::vector<std::string> paths = ffmt.get_input_file_paths();

    /// Check two vectors contains the same paths
    REQUIRE(paths.size() == (dirs.size() * files.size()));
    REQUIRE_THAT(paths, Catch::Matchers::UnorderedEquals(targets));

    fs::remove_all(testdir);
  }

  SECTION("output_directory_path")
  {
    fs::path outdir{"testout"};
    ffmt.set_output_path(outdir);

    REQUIRE(ffmt.get_output_path() == outdir);
  }
}