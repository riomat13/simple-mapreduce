#ifndef SIMPLEMAPREDUCE_BASE_FILE_FORMAT_H_
#define SIMPLEMAPREDUCE_BASE_FILE_FORMAT_H_

#include <filesystem>
#include <string>
#include <vector>

namespace mapreduce {
namespace base {

class FileFormat {
 public:
  // FileFormat() {}
  virtual ~FileFormat() {};

  /**
   * Add directory path containing input files.
   *
   *  @param path   input directory path
   */
  virtual void add_input_path(const std::string&) = 0;

  /**
   * Add multiple directory paths containing input files.
   *
   *  @param paths  input directory paths
   */
  virtual void add_input_paths(const std::vector<std::string>&) = 0;

  /** Get next file. */
  virtual std::string get_filepath() = 0;

  /** Reset input file path extraction. */
  void reset_input_paths() { input_idx_ = 0; };

  /** Set target directory path to save output files */
  virtual void set_output_path(const std::string& path) = 0;

  /** Get target output path */
  virtual std::filesystem::path get_output_path() const = 0;

 protected:
  /// Current input file path index in input_dirs_
  size_t input_idx_{0};

  /// Current directory iterator
  std::filesystem::directory_iterator curr_;

  /// directory paths to read input files
  std::vector<std::filesystem::path> input_dirs_;

  /// target directory path to save files
  std::filesystem::path output_path_;
};

} // namespace base
} // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_BASE_FILE_FORMAT_H_