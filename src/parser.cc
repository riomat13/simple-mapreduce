#include "simplemapreduce/util/parser.h"

#include <sstream>

namespace mapreduce {
namespace util {

std::vector<std::string> parse_string(const std::string& data, const char& delimiter)
{
  std::istringstream iss(std::move(data));
  std::string chunk;

  std::vector<std::string> res;

  while (std::getline(iss, chunk, delimiter))
    res.push_back(chunk);

  return res;
}

}  // namespace util
}  // namespace mapreduce