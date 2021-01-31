#include "simplemapreduce/util/log.h"

#include <chrono>

namespace mapreduce {
namespace util {

  void Logger::log_append_time_tag(OSS &oss)
  {
    auto curr = std::chrono::system_clock::now();
    std::time_t tm = std::chrono::system_clock::to_time_t(curr);

    auto truncate = curr.time_since_epoch().count() / 1000000;
    auto millisecs = truncate % 1000;

    oss << "[" << std::put_time(std::localtime(&tm), "%F %T")
      << "." << std::setw(3) << std::setfill('0') << millisecs << "] ";
  }

} // namespace util
} // namespace mapreduce

/// For globally use
mapreduce::util::Logger logger{};