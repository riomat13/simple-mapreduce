#ifndef TESTS_UTILS_H_
#define TESTS_UTILS_H_

#include <filesystem>
#include <vector>

#include "catch.hpp"

namespace fs = std::filesystem;

/// Directory to store all temporary files for testing
extern fs::path testdir;

/**
 * Clear all content in a file with given path.
 *
 *  @param path&  target file path to clear data
 */
void clear_file(const fs::path &path);

/**
 * Extract files in the given directory.
 * The files will be pushed back to the given vector.
 * This will return files which are true by `std::filesystem::is_regular_file`
 *
 *  @param path&        directory path to parse
 *  @param container&   vector to store extracted file paths
 */
void extract_files(const fs::path&, std::vector<fs::path> &);

/**
 * Compare two vectors.
 * This will sort both vectors and change the states,
 * so that if want to keep the order, pass by value.
 */
template <typename T>
void compare_vector(std::vector<T> &a, std::vector<T> &b)
{
  REQUIRE(a.size() == b.size());
  std::sort(a.begin(), a.end());
  std::sort(b.begin(), b.end());
  REQUIRE(std::equal(a.begin(), a.end(), b.begin()));
}

#endif