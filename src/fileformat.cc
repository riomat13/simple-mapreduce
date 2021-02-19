#include "simplemapreduce/ops/fileformat.h"

#include <mutex>
#include <thread>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/util/log.h"

namespace fs = std::filesystem;

using namespace mapreduce::util;

namespace mapreduce {
  
  /// Used for passing this object to Job
  FileFormat& FileFormat::operator=(const FileFormat& rhs) {
    if (this == &rhs)
      return *this;
    
    this->input_dirs_.clear();
    this->input_dirs_ = std::move(rhs.input_dirs_);
    this->curr_ = std::move(rhs.curr_);
    this->output_path_ = std::move(rhs.output_path_);

    return *this;
  }

  void FileFormat::add_input_path(const std::string& path) {
    input_dirs_.emplace_back(std::move(path));
  }

  void FileFormat::add_input_paths(const std::vector<std::string>& paths) {
    input_dirs_.insert(input_dirs_.end(), paths.begin(), paths.end());
  }

  void FileFormat::set_output_path(const std::string& path) {
    if (!output_path_.empty()) {
      logger.warning("Output path is already set.");
    } else {
      output_path_ = std::move(path);
    }
  }

  std::string FileFormat::get_filepath() {
    std::lock_guard<std::mutex> lock_(mapreduce::commons::mr_mutex);

    while (true) {
      if (curr_ == fs::end(curr_)) {
        if (input_idx_ == input_dirs_.size())
          return "";

        curr_ = fs::directory_iterator(input_dirs_[input_idx_++]);
      } else if (!fs::is_regular_file((*curr_).path())) {
        ++curr_;
      } else {
        return (*(curr_++)).path().string();
      }
    }
  }

} // namespace mapreduce