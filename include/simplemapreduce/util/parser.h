#ifndef SIMPLEMAPREDUCE_UTIL_PARSER_H_
#define SIMPLEMAPREDUCE_UTIL_PARSER_H_

#include <string>
#include <vector>

namespace mapreduce {
namespace util {

/**
 * Parse string and split comma separated data into each chunk.
 *
 *  @param data       input string with comma
 *  @param delimiter  delimiter to split string
 */
std::vector<std::string> parse_string(const std::string& data, const char& = ',');

}  // namespace util
}  // namespace mapreduce


#endif  // SIMPLEMAPREDUCE_UTIL_PARSER_H_