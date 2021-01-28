// This is used for baseline to compare the performance.
// As can be seen the script below, this is running on a single thread.

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <thread>
#include <string>
#include <vector>

using namespace std;

namespace fs = std::filesystem;


int main()
{
  std::ofstream ofs("outputs.txt");
  std::map<std::string, long> counter;

  for (auto &path: fs::directory_iterator("./inputs"))
  {
    if (!path.is_regular_file())
      continue;

    std::ifstream ifs(path.path());
    std::string line;

    while (std::getline(ifs, line)) {
      std::replace_if(line.begin(), line.end(),
                    [](unsigned char c){ return std::ispunct(c); }, ' ');
      std::istringstream iss(line);
      std::string word;
      while (iss >> word)
        ++counter[word];
    }

  }

  for (auto it = counter.begin(); it != counter.end(); ++it)
  {
    ofs << it->first << " " << it->second << '\n';
  }

  return 0;
}
