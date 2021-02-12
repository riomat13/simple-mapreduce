#include "simplemapreduce/ops/job.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <type_traits>
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

/**
 * Helper function to process key data in mapper.
 */
template <typename T>
T convert_key(const std::string&);

template<> int convert_key(const std::string& key) { return std::stoi(key); }
template<> long convert_key(const std::string& key) { return std::stol(key); }
template<> std::string convert_key(const std::string& key) { return std::string(key); }

template <typename K, typename V>
class TestMapper: public Mapper<std::string, long, K, V>
{
 public:
  void map(const std::string& ikey, const long&, const Context<K, V>& context)
  {
    std::string line, word;
    std::istringstream iss(ikey);
    while (std::getline(iss, line))
    {
      std::istringstream linestream(line);
      while (linestream >> word)
      {
        K key = convert_key<K>(word);
        V value = 1;
        context.write(key, value);
      }
    }
  }
};

template <typename K, typename V>
class TestCombiner: public Reducer<K, V, K, V>
{
 public:
  void reduce(const K& ikey, const std::vector<V>& ivalues, const Context<K, V>& context)
  {
    K key(ikey);
    /// In reducer, only counts length of vector
    /// so that the result on this process does not affect the result
    V value = 0;
    context.write(key, value);
  }
};

template <typename K, typename V>
class TestReducer: public Reducer<K, V, K, V>
{
 public:
  void reduce(const K& ikey, const std::vector<V>& ivalues, const Context<K, V>& context)
  {
    K key(ikey);
    /// For testing, only returns vector size
    V value = static_cast<V>(ivalues.size());
    context.write(key, value);
  }
};

/**
 * Integration test runner.
 * This will test with given types.
 *
 *  @param target_keys&   data used as key
 *  @param count&         number of times to generate data per key
 */
template <typename K, typename V>
void test_runner(std::vector<K>& target_keys, const unsigned int& count)
{
  fs::path input_dir = tmpdir / "test_job" / "inputs";
  fs::path output_dir = tmpdir / "test_job" / "outputs";

  FileFormat ffmt;
  ffmt.add_input_path(input_dir);
  ffmt.set_output_path(output_dir);

  /// Setup MapReduce Job
  Job job;
  job.set_file_format(ffmt);
  job.set_config("log_level", 4);

  job.template set_mapper<TestMapper<K, V>>();
  job.template set_reducer<TestReducer<K, V>>();

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /// Test only on root node
  if (rank == 0)
  {
    /// Setup input files
    fs::remove_all(input_dir);
    fs::create_directories(input_dir);

    /// Store keys processed by MapReduce
    std::vector<K> res;

    /// Write input data
    for (unsigned int i = 0; i < count; ++i)
    {
      std::ofstream ofs(input_dir / std::to_string(i));
      for (auto& key: target_keys)
        ofs << key << " ";
      ofs.close();
    }

    job.run();

    /// Check if output directory is created
    REQUIRE(fs::is_directory(output_dir));

    /// Parse output data
    for (auto& path: fs::directory_iterator(output_dir))
    {
      std::ifstream ifs(path.path());
      std::string line;
      K key;
      V value;
      while (std::getline(ifs, line))
      {
        std::istringstream iss(line);
        iss >> key >> value;
        res.push_back(std::move(key));

        /// All files contains the same number of words(=count)
        REQUIRE(value == count);
      }
    }

    /// Check the result
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target_keys));
  } else {
    /// For child nodes
    job.run();
  }
}

/**
 * Integration test runner.
 * This will test with given types.
 *
 *  @param target_keys&   data used as key
 *  @param count&         number of times to generate data per key
 */
template <typename K, typename V>
void test_runner_with_combiner(std::vector<K>& target_keys, const unsigned int& count)
{
  fs::path input_dir = tmpdir / "test_job" / "inputs";
  fs::path output_dir = tmpdir / "test_job" / "outputs";

  FileFormat ffmt;
  ffmt.add_input_path(input_dir);
  ffmt.set_output_path(output_dir);

  /// Setup MapReduce Job
  Job job;
  job.set_file_format(ffmt);
  job.set_config("log_level", 4);

  job.template set_mapper<TestMapper<K, V>>();
  job.template set_combiner<TestCombiner<K, V>>();
  job.template set_reducer<TestReducer<K, V>>();

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /// Test only on root node
  if (rank == 0)
  {
    /// Setup input files
    fs::remove_all(input_dir);
    fs::create_directories(input_dir);

    /// Store keys processed by MapReduce
    std::vector<K> res;

    /// Write input data
    for (unsigned int i = 0; i < count; ++i)
    {
      std::ofstream ofs(input_dir / std::to_string(i));
      for (auto& key: target_keys)
        ofs << key << " ";
      ofs.close();
    }

    job.run();

    /// Check if output directory is created
    REQUIRE(fs::is_directory(output_dir));

    /// Count workers
    int target_count;
    MPI_Comm_size(MPI_COMM_WORLD, &target_count);
    --target_count;

    /// Parse output data
    for (auto& path: fs::directory_iterator(output_dir))
    {
      std::ifstream ifs(path.path());
      std::string line;
      K key;
      V value;
      while (std::getline(ifs, line))
      {
        std::istringstream iss(line);
        iss >> key >> value;
        res.push_back(std::move(key));

        /// All files contains the same number of workers
        /// because of combiner
        REQUIRE(value == target_count);
      }
    }

    /// Check the result
    REQUIRE_THAT(res, Catch::Matchers::UnorderedEquals(target_keys));
  } else {
    /// For child nodes
    job.run();
  }
}

TEST_CASE("Integration Test", "[job][mapreduce][integrate]")
{
  #ifdef INTEGRATION1
  SECTION("Job:string/int")
  {
    std::vector<std::string> keys{"test", "example", "mapreduce"};
    test_runner<std::string, int>(keys, 3);
  }
  #endif  // INTEGRATION1
  #ifdef INTEGRATION2
  SECTION("Job:int/double")
  {
    std::vector<int> keys{100, 200, 300};
    test_runner<int, double>(keys, 5);
  }
  #endif  // INTEGRATION2
  #ifdef INTEGRATION3
  SECTION("Job:long/int")
  {
    std::vector<long> keys{100000, 200000, 300000};
    test_runner<long, int>(keys, 10);
  }
  #endif  // INTEGRATION3
  fs::remove_all(tmpdir);
}

TEST_CASE("Integration Test with Combiner", "[job][mapreduce][combiner][integrate]")
{
  #ifdef INTEGRATION4
  SECTION("Job:string/int")
  {
    std::vector<std::string> keys{"test", "example", "mapreduce"};
    test_runner_with_combiner<std::string, int>(keys, 3);
  }
  #endif  // INTEGRATION4
  #ifdef INTEGRATION5
  SECTION("Job:int/double")
  {
    std::vector<int> keys{100, 200, 300};
    test_runner_with_combiner<int, double>(keys, 5);
  }
  #endif  // INTEGRATION5
  #ifdef INTEGRATION6
  SECTION("Job:long/int")
  {
    std::vector<long> keys{100000, 200000, 300000};
    test_runner_with_combiner<long, int>(keys, 10);
  }
  #endif  // INTEGRATION6
}