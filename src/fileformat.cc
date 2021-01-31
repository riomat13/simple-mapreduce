#include "simplemapreduce/ops/fileformat.h"

namespace fs = std::filesystem;

namespace mapreduce {
  
  /// Used for passing this object to Job
  FileFormat &FileFormat::operator=(const FileFormat &rhs)
  {
    if (this == &rhs)
      return *this;
    
    this->input_paths_.clear();
    this->input_paths_ = std::move(rhs.input_paths_);
    this->output_path_ = std::move(rhs.output_path_);

    return *this;
  }

  std::vector<std::string> FileFormat::get_input_file_paths()
  {
    std::vector<std::string> paths;

    /// parse each directory and store all regular file paths
    for (std::string &dirpath: input_paths_)
    {
      for (auto &fpath: fs::directory_iterator(dirpath))
      {
        if (!fpath.is_regular_file())
          continue;

        paths.push_back(fpath.path());
      }
    }
    return paths;
  }

} // namespace mapreduce