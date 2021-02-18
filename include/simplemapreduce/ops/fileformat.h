#ifndef SIMPLEMAPREDUCE_OPS_FILE_FORMAT_H_
#define SIMPLEMAPREDUCE_OPS_FILE_FORMAT_H_

#include <filesystem>
#include <string>
#include <vector>

namespace mapreduce {

class FileFormat final
{
 public:
  FileFormat() {};
  ~FileFormat() {};

  FileFormat& operator=(const FileFormat&);

  /// not allowed to copy nor move constructor/assignments
  /// since there is no useful cases
  FileFormat(const FileFormat&) = delete;
  FileFormat& operator=(FileFormat&&) = delete;

  /**
   * Add directory path containing input files.
   *
   *  @param path   input directory path
   */
  void add_input_path(const std::string&);

  /**
   * Add multiple directory paths containing input files.
   *
   *  @param paths  input directory paths
   */
  void add_input_paths(const std::vector<std::string>&);

  /** Get next file. */
  std::string get_filepath();

  /** Reset input file path extraction. */
  void reset_input_paths() { input_idx_ = 0; };

  /** Set target directory path to save output files */
  void set_output_path(const std::string& path);

  /** Get target output path */
  std::filesystem::path get_output_path() const { return output_path_; }

 private:
  /// Current input file path index in input_dirs_
  size_t input_idx_{0};

  /// Current directory iterator
  std::filesystem::directory_iterator curr_;

  /// directory paths to read input files
  std::vector<std::filesystem::path> input_dirs_;

  /// target directory path to save files
  std::filesystem::path output_path_;
};

} // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_OPS_FILE_FORMAT_H_