#include "simplemapreduce/util/log.h"

#include <chrono>

namespace mapreduce {
namespace util {

  std::string LogBuffer::to_string() { return oss.str(); }

  void Logger::set_log_level(LogLevel&& level) {
    get_log_level() = std::move(level);
  }

  void Logger::log_append_time_tag(LogBuffer& buff) {
    auto curr = std::chrono::system_clock::now();
    std::time_t tm = std::chrono::system_clock::to_time_t(curr);

    auto truncate = curr.time_since_epoch().count() / 1000000;
    auto millisecs = truncate % 1000;

    buff << "[" << std::put_time(std::localtime(&tm), "%F %T")
      << "." << std::setw(3) << std::setfill('0') << millisecs << "] ";
  }

  Logger logger{};

} // namespace util
} // namespace mapreduce