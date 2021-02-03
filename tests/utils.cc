#include "utils.h"

#include <fstream>

fs::path testdir = fs::temp_directory_path() / "test_smr";

void clear_file(const fs::path &path)
{
  std::ifstream ifs;
  ifs.open(path, std::istream::trunc);
  ifs.close();
}

void extract_files(const fs::path &dirpath, std::vector<fs::path> &container)
{
  for (auto &path: fs::directory_iterator(dirpath))
    if (fs::is_regular_file(path))
      container.push_back(std::move(path));
}