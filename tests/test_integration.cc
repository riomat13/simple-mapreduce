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
#include "simplemapreduce/data/type.h"
#include "simplemapreduce/ops/context.h"

namespace fs = std::filesystem;

using namespace mapreduce;
using namespace mapreduce::type;

/**
 * Helper function to process key data in mapper.
 */
template <typename T>
T convert_key(const std::string&);

template<> int convert_key(const std::string& key) { return std::stoi(key); }
template<> long convert_key(const std::string& key) { return std::stol(key); }
template<> std::string convert_key(const std::string& key) { return std::string(key); }

template <typename K, typename V>
class TestMapper: public Mapper<String, Long, K, V> {
 public:
  void map(const String& ikey, const Long&, const Context<K, V>& context) {
    std::string line, word;
    std::istringstream iss(ikey);
    while (std::getline(iss, line)) {
      std::istringstream linestream(line);
      while (linestream >> word) {
        K key = convert_key<K>(word);
        V value = 1;
        context.write(key, value);
      }
    }
  }
};

template <typename K, typename V>
class TestCombiner: public Reducer<K, V, K, V> {
 public:
  void reduce(const K& ikey, const std::vector<V>& ivalues, const Context<K, V>& context) {
    K key(ikey);
    /// In reducer, only counts length of vector
    /// so that the result on this process does not affect the result
    V value = 0;
    context.write(key, value);
  }
};

template <typename IK, typename IV, typename OK, typename OV>
class TestReducer: public Reducer<IK, IV, OK, OV> {
 public:
  void reduce(const IK& ikey, const std::vector<IV>& ivalues, const Context<OK, OV>& context) {
    OK key(ikey);
    /// For testing, only returns vector size
    OV value = static_cast<OV>(ivalues.size());
    context.write(key, value);
  }
};

/**
 * Integration test.
 * This will test with given types.
 *
 *  @param target_keys&   data used as key
 *  @param count&         number of times to generate data per key
 */
template <typename IK, typename IV, typename OK, typename OV>
void test_mapreduce(std::vector<OK>& target_keys, const unsigned int& count) {
  fs::path input_dir = tmpdir / "test_job" / "inputs";
  fs::path output_dir = tmpdir / "test_job" / "outputs";

  /// Setup MapReduce Job
  Job job;
  job.add_input_path(input_dir);
  job.set_output_path(output_dir);

  job.set_config(Config::log_level, 4);

  job.template set_mapper<TestMapper<IK, IV>>();
  job.template set_reducer<TestReducer<IK, IV, OK, OV>>();

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /// Test only on root node
  if (rank == 0) {
    /// Setup input files
    fs::remove_all(input_dir);
    fs::create_directories(input_dir);

    /// Store keys processed by MapReduce
    std::vector<OK> res;

    /// Write input data
    for (unsigned int i = 0; i < count; ++i) {
      std::ofstream ofs(input_dir / std::to_string(i));
      for (auto& key: target_keys)
        ofs << key << " ";
      ofs.close();
    }

    job.run();

    /// Check if output directory is created
    REQUIRE(fs::is_directory(output_dir));

    /// Parse output data
    for (auto& path: fs::directory_iterator(output_dir)) {
      std::ifstream ifs(path.path());
      std::string line;
      OK key;
      OV value;
      while (std::getline(ifs, line)) {
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
 * Integration test with the same key/value type in mapper and reducer outputs.
 * This will test with given types.
 *
 *  @param target_keys&   data used as key
 *  @param count&         number of times to generate data per key
 */
template <typename K, typename V>
void test_mapreduce(std::vector<K>& target_keys, const unsigned int& count) {
  test_mapreduce<K, V, K, V>(target_keys, count);
}

/**
 * Integration test.
 * This will test with given types.
 *
 *  @param target_keys&   data used as key
 *  @param count&         number of times to generate data per key
 */
template <typename K, typename V>
void test_mapreduce_with_combiner(std::vector<K>& target_keys, const unsigned int& count) {
  fs::path input_dir = tmpdir / "test_job" / "inputs";
  fs::path output_dir = tmpdir / "test_job" / "outputs";

  /// Setup MapReduce Job
  Job job;
  job.add_input_path(input_dir);
  job.set_output_path(output_dir);
  job.set_config(Config::log_level, 4);

  job.template set_mapper<TestMapper<K, V>>();
  job.template set_combiner<TestCombiner<K, V>>();
  job.template set_reducer<TestReducer<K, V, K, V>>();

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /// Test only on root node
  if (rank == 0) {
    /// Setup input files
    fs::remove_all(input_dir);
    fs::create_directories(input_dir);

    /// Store keys processed by MapReduce
    std::vector<K> res;

    /// Write input data
    for (unsigned int i = 0; i < count; ++i) {
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
    for (auto& path: fs::directory_iterator(output_dir)) {
      std::ifstream ifs(path.path());
      std::string line;
      K key;
      V value;
      while (std::getline(ifs, line)) {
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

TEST_CASE("Integration Test", "[job][mapreduce][integrate]") {
#ifdef INTEGRATION1
  SECTION("Job:String/Int") {
    std::vector<String> keys{"test", "example", "mapreduce"};
    test_mapreduce<String, Int>(keys, 3);
  }
#endif  // INTEGRATION1
#ifdef INTEGRATION2
  SECTION("Job:Int/Double") {
    std::vector<Int> keys{100, 200, 300};
    test_mapreduce<Int, Double>(keys, 5);
  }
#endif  // INTEGRATION2
#ifdef INTEGRATION3
  SECTION("Job:Long/Int") {
    std::vector<Long> keys{100000, 200000, 300000};
    test_mapreduce<Long, Int>(keys, 10);
  }
#endif  // INTEGRATION3
#ifdef INTEGRATION4
  SECTION("Job:String/Int to String/Long") {
    std::vector<String> keys{"test", "example", "mapreduce"};
    test_mapreduce<String, Int, String, Long>(keys, 3);
  }
#endif  // INTEGRATION4
  fs::remove_all(tmpdir);
}

TEST_CASE("Integration Test with Combiner", "[job][mapreduce][combiner][integrate]") {
#ifdef INTEGRATION5
  SECTION("Job:String/Int") {
    std::vector<String> keys{"test", "example", "mapreduce"};
    test_mapreduce_with_combiner<String, Int>(keys, 3);
  }
#endif  // INTEGRATION5
#ifdef INTEGRATION6
  SECTION("Job:Int/Double") {
    std::vector<Int> keys{100, 200, 300};
    test_mapreduce_with_combiner<Int, Double>(keys, 5);
  }
#endif  // INTEGRATION6
#ifdef INTEGRATION7
  SECTION("Job:Long/Int") {
    std::vector<Long> keys{100000, 200000, 300000};
    test_mapreduce_with_combiner<Long, Int>(keys, 10);
  }
#endif  // INTEGRATION7
  fs::remove_all(tmpdir);
}