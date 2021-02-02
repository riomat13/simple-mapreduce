#ifndef TESTS_UTILS_H_
#define TESTS_UTILS_H_

#include <filesystem>

namespace fs = std::filesystem;

/// Directory to store all temporary files for testing
extern fs::path testdir;

/**
 * Clear all content in a file with given path.
 *
 *  @param path&  target file path to clear data
 */
void clear_file(const fs::path &path);

#endif