#include "utils.h"

#include <fstream>

fs::path testdir = fs::temp_directory_path() / "test_smr";

void clear_file(const fs::path &path)
{
  std::ifstream ifs;
  ifs.open(path, std::istream::trunc);
  ifs.close();
}