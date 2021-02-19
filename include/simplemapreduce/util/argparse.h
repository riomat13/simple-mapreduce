#ifndef SIMPLEMAPREDUCE_UTIL_ARGPARSE_H_
#define SIMPLEMAPREDUCE_UTIL_ARGPARSE_H_

#include <map>
#include <string>

namespace mapreduce {
namespace util {

class ArgParser {
 public:
  ArgParser(int&, char*[]);

  /**
   * Get option value by key.
   *
   *  @param key    option name
   */
  std::string get_option(const std::string&);

  /** Whether help option is passed */
  bool is_help() { return is_help_; }

 private:
  /// Flag true if passed -h/--help
  bool is_help_{false};
  /// Store all options and corresponding values
  std::map<std::string, std::string> optvalues_;
};

}  // namespace util
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_UTIL_ARGPARSE_H_