#include "simplemapreduce/local/fileformat.h"

#include <mutex>
#include <thread>

#include "simplemapreduce/commons.h"

namespace fs = std::filesystem;

using namespace mapreduce::util;

namespace mapreduce {
namespace local {

void LocalFileFormat::add_input_path(const std::string& path) {
  input_dirs_.emplace_back(std::move(path));
}

void LocalFileFormat::add_input_paths(const std::vector<std::string>& paths) {
  input_dirs_.insert(input_dirs_.end(), paths.begin(), paths.end());
}

void LocalFileFormat::set_output_path(const std::string& path) {
  if (!output_path_.empty()) {
    logger.warning("Output path is already set.");
  } else {
    output_path_ = std::move(path);
  }
}

std::string LocalFileFormat::get_filepath() {
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

} // namespace local
} // namespace mapreduce