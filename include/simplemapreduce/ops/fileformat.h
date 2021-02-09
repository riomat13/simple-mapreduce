#ifndef SIMPLEMAPREDUCE_OPS_FILE_FORMAT_H
#define SIMPLEMAPREDUCE_OPS_FILE_FORMAT_H

#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace mapreduce {

class FileFormat final
{
 public:
  FileFormat() {};
  ~FileFormat() {};

  FileFormat &operator=(const FileFormat &rhs);

  /// not allowed to copy nor move constructor/assignments
  /// since there is no useful cases
  FileFormat(FileFormat const &rhs) = delete;
  FileFormat &operator=(FileFormat &&rhs) = delete;

  /** Add directory path containing input files */
  void add_input_path(const std::string &path) { input_paths_.push_back(std::move(path)); }

  /**
   * Get all input files from registered paths.
   * Each call incurs parsing files.
   * 
   * This is intended to use only once in the entire execution,
   * therefore, this will not save the data into memory such as array.
   */
  std::vector<std::string> get_input_file_paths();

  /** Set target directory path to save output files */
  void set_output_path(const std::string &path) { output_path_ = std::move(path); }

  /** Get target output path */
  fs::path get_output_path() const { return output_path_; }

 private:
  /// directory paths to read input files
  std::vector<std::string> input_paths_;
  /// target directory path to save files
  fs::path output_path_;
};

} // namespace mapreduce

#endif