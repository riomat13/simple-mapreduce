#include "simplemapreduce/util/argparse.h"

#include <filesystem>
#include <getopt.h>
#include <iostream>
#include <vector>

namespace fs = std::filesystem;

namespace mapreduce {
namespace util {

ArgParser::ArgParser(int& argc, char* argv[]) {
  const struct option longopts[] = {
    {"input", 1, 0, 'i'},
    {"output", 1, 0, 'o'},
    {"help", 0, 0, 'h'},
    {0, 0, 0, 0}
  };

  int longindex;
  int iarg = 0;
  while (iarg != -1) {
    iarg = getopt_long(argc, argv, "i:o:h", longopts, &longindex);

    switch (iarg) {
      case 'h':
        std::cout << "Usage: ./${script} [arguments]\n\n";
        std::cout << "  Arguments:\n";
        std::cout << "    -i/--input:   Input directories(comma separated)\n";
        std::cout << "    -o/--output:  Output directory\n";
        is_help_ = true;
        return;
      case 'i':
        optvalues_.insert({"input", optarg});
        break;
      case 'o':
        optvalues_.insert({"output", optarg});
        break;
    }
  }
}

std::string ArgParser::get_option(const std::string& key) {
  auto search = optvalues_.find(key);
  if (search == optvalues_.end())
    return std::string{};

  /// Assumes only one time used
  return std::move(search->second);
}

}  // namespace util
}  // namespace mapreduce