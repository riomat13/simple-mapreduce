#include "utils.h"

#include <algorithm>
#include <fstream>

namespace fs = std::filesystem;

fs::path tmpdir = fs::temp_directory_path() / "test_smr";

bool ends_with(std::string_view sequence, std::string_view target) {
  if (sequence.size() < target.size()) {
    return false;
  }
  return std::equal(target.begin(), target.end(), sequence.end() - target.size());
}

void clear_file(const fs::path& path) {
  std::ifstream ifs;
  ifs.open(path, std::istream::trunc);
  ifs.close();
}

void extract_files(const fs::path& dirpath, std::vector<fs::path>& container) {
  for (auto& path: fs::directory_iterator(dirpath)) {
    if (fs::is_regular_file(path))
      container.push_back(std::move(path));
  }
}