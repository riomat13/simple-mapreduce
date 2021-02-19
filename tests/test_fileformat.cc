#include "simplemapreduce/ops/fileformat.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <vector>

#include "catch.hpp"

#include "utils.h"

namespace fs = std::filesystem;

using namespace mapreduce;

/** Create test empty files */
void create_test_files(fs::path& dirpath, unsigned int n) {
  for (unsigned int i = 0; i < n; ++i) {
    std::ofstream ost(dirpath / std::to_string(i));
    ost.close();
  }
}

TEST_CASE("FileFormat", "[file]") {
  FileFormat ffmt;

  SECTION("add_input_path") {
    unsigned int count = 10;
    fs::path testdir = tmpdir / "test_fileformat";
    fs::create_directories(testdir);

    std::vector<fs::path> dirs{fs::path{"testdir1"}, fs::path{"testdir2"}};

    std::vector<std::string> targets;

    for (auto& dir: dirs) {
      fs::path directory = testdir / dir;
      fs::create_directory(directory);

      create_test_files(directory, count);

      for (unsigned int i = 0; i < count; ++i)
        targets.push_back(directory / std::to_string(i));
      
      /// Register multiple directories
      ffmt.add_input_path(directory);

      /// Dummy directories
      /// These paths should be ignored
      fs::create_directories(directory / "dummy1");
      fs::create_directories(directory / "dummy2");
    }

    std::vector<std::string> paths;

    for (auto&& p = ffmt.get_filepath(); !p.empty(); p = ffmt.get_filepath())
      paths.push_back(std::move(p));

    /// Check two vectors contains the same paths
    REQUIRE(paths.size() == (dirs.size() * count));
    REQUIRE_THAT(paths, Catch::Matchers::UnorderedEquals(targets));

    /// Reset and take one more time
    paths.clear();
    ffmt.reset_input_paths();

    for (auto&& p = ffmt.get_filepath(); !p.empty(); p = ffmt.get_filepath())
      paths.push_back(std::move(p));

    /// Check two vectors contains the same paths
    REQUIRE(paths.size() == (dirs.size() * count));
    REQUIRE_THAT(paths, Catch::Matchers::UnorderedEquals(targets));

    fs::remove_all(testdir);
  }

  SECTION("add_input_paths") {
    unsigned int count = 10;
    fs::path testdir = tmpdir / "test_fileformat";
    fs::create_directories(testdir);

    std::vector<std::string> dirs{"testdir1", "testdir2"};
    std::vector<std::string> inputs;
    std::vector<std::string> targets;

    for (auto& dir: dirs) {
      fs::path directory = testdir / dir;
      fs::create_directory(directory);
      inputs.push_back(directory);

      create_test_files(directory, count);

      for (unsigned int i = 0; i < count; ++i)
        targets.push_back(directory / std::to_string(i));
      
      /// Dummy directories
      /// These paths should be ignored
      fs::create_directories(directory / "dummy1");
      fs::create_directories(directory / "dummy2");
    }

    /// Register multiple directories at once
    ffmt.add_input_paths(inputs);

    std::vector<std::string> paths;

    for (auto&& p = ffmt.get_filepath(); !p.empty(); p = ffmt.get_filepath())
      paths.push_back(std::move(p));

    /// Check two vectors contains the same paths
    REQUIRE(paths.size() == (dirs.size() * count));
    REQUIRE_THAT(paths, Catch::Matchers::UnorderedEquals(targets));

    /// Reset and take one more time
    paths.clear();
    ffmt.reset_input_paths();

    for (auto&& p = ffmt.get_filepath(); !p.empty(); p = ffmt.get_filepath())
      paths.push_back(std::move(p));

    /// Check two vectors contains the same paths
    REQUIRE(paths.size() == (dirs.size() * count));
    REQUIRE_THAT(paths, Catch::Matchers::UnorderedEquals(targets));

    fs::remove_all(testdir);
  }

  SECTION("output_directory_path") {
    fs::path outdir{"testout"};
    ffmt.set_output_path(outdir);

    REQUIRE(ffmt.get_output_path() == outdir);

    /// Disallow override
    ffmt.set_output_path("notvalid");
    REQUIRE(ffmt.get_output_path() == outdir);
  }
}