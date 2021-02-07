#include "simplemapreduce/ops/job.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <mpi.h>
#include "catch.hpp"

#include "utils.h"
#include "simplemapreduce/mapper.h"
#include "simplemapreduce/reducer.h"
#include "simplemapreduce/ops/context.h"
#include "simplemapreduce/ops/fileformat.h"

namespace fs = std::filesystem;

using namespace mapreduce;

class TestMapper: public Mapper<std::string, long, std::string, int>
{
 public:
  void map(const std::string &ikey, const long&, const Context &context)
  {
    std::string key(ikey);
    int value = 1;
    context.write(key, value);
  }
};

class TestReducer: public Reducer<std::string, int, std::string, int>
{
 public:
  void reduce(const std::string& ikey, const std::vector<int> &ivalues, const Context &context)
  {
    std::string key(ikey);
    int value = ivalues.size();
    context.write(key, value);
  }
};

TEST_CASE("Integration_Test", "[job][mapreduce][integrate]")
{
  fs::path input_dir = testdir / "test_job" / "inputs";
  fs::path output_dir = testdir / "test_job" / "outputs";

  FileFormat ffmt;
  ffmt.add_input_path(input_dir);
  ffmt.set_output_path(output_dir);

  SECTION("Job")
  {
    /// Setup MapReduce Job
    Job<TestMapper, TestReducer> job;
    job.set_file_format(ffmt);
    job.set_config("log_level", 4);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /// Test only on root node
    if (rank == 0)
    {
      /// Setup input files
      fs::create_directories(input_dir);

      unsigned int count = 3;
      std::vector<std::string> targets{"test", "example", "mapreduce"};
      std::vector<std::string> res;

      /// Write input data
      for (unsigned int i = 0; i < count; ++i)
      {
        std::ofstream ofs(input_dir / std::to_string(i));
        ofs << targets[i];
        ofs.close();
      }

      job.run();

      /// Parse output data
      for (auto &path: fs::directory_iterator(output_dir))
      {
        std::ifstream ifs(path.path());
        std::string key, line;
        int value;
        while (std::getline(ifs, line))
        {
          std::istringstream iss(line);
          iss >> key >> value;
          REQUIRE(value == 1);
          res.push_back(std::move(key));
        }
      }

      /// Check the result
      compare_vector(res, targets);

      fs::remove_all(testdir);
    } else {
      /// For child nodes
      job.run();
    }
  }

  fs::remove_all(testdir);
}