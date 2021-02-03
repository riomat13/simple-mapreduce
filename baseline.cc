// This is used for baseline to compare the performance.
// As can be seen the script below, this is running on a single thread.

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <map>
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

      /// Read word one by one separated by spaces/tabs
      std::istringstream iss(line);
      std::string word;
      while (iss >> word)
        ++counter[word];
    }
  }

  /// Write key and value(count) to the output file
  for (auto it = counter.begin(); it != counter.end(); ++it)
  {
    ofs << it->first << " " << it->second << '\n';
  }

  return 0;
}
