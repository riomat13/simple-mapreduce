#ifndef SIMPLEMAPREDUCE_LOCAL_FILE_FORMAT_H_
#define SIMPLEMAPREDUCE_LOCAL_FILE_FORMAT_H_

#include <filesystem>
#include <string>
#include <vector>

#include "simplemapreduce/base/fileformat.h"

namespace mapreduce {
namespace local {

class LocalFileFormat : public mapreduce::base::FileFormat {
 public:
  LocalFileFormat() {}

  /// not allowed to copy nor move constructor/assignments
  /// since there is no useful cases
  LocalFileFormat(const LocalFileFormat&) = delete;
  LocalFileFormat& operator=(LocalFileFormat&&) = delete;

  /**
   * Add directory path containing input files.
   *
   *  @param path   input directory path
   */
  void add_input_path(const std::string&) override;

  /**
   * Add multiple directory paths containing input files.
   *
   *  @param paths  input directory paths
   */
  void add_input_paths(const std::vector<std::string>&) override;

  /** Set target directory path to save output files */
  void set_output_path(const std::string& path) override;

  /** Get target output path */
  std::filesystem::path get_output_path() const override { return output_path_; }

  /** Get next file. */
  std::string get_filepath() override;
};

} // namespace local
} // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_LOCAL_FILE_FORMAT_H_