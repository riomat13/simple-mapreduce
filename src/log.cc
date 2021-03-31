#include "simplemapreduce/util/log.h"

#include <chrono>

namespace fs = std::filesystem;

namespace mapreduce {
namespace util {

  std::string LogBuffer::to_string() { return oss.str(); }

  LogWriter::LogWriter(const std::string& path) {
    ofs_.open(path);
  }
  LogWriter::LogWriter(const fs::path& path) {
    ofs_.open(path);
  }

  LogWriter::~LogWriter() {
    if (ofs_.is_open()) {
      ofs_.close();
    }
  }

  void LogWriter::write(const std::string& log) {
    ofs_ << log << '\n';
  }

  void LogWriter::flush() {
    ofs_.flush();
  }

  void Logger::set_log_level(LogLevel&& level) {
    get_log_level() = level;
    if (!is_set_file) {
      get_file_log_level() = level;
    }
  }

  void Logger::set_filepath(const std::string& filepath) {
    set_filepath(fs::path(filepath));
  }

  void Logger::set_filepath(const fs::path& filepath) {
    log_writer_ = std::make_unique<LogWriter>(filepath);
  }

  void Logger::set_log_level_for_file(LogLevel&& level) {
    get_file_log_level() = level;
    is_set_file = true;
  }

  /// Reference: https://stackoverflow.com/a/50923834
  void Logger::log_append_time_tag(LogBuffer& buff) {
    auto curr = std::chrono::system_clock::now();
    std::time_t tm = std::chrono::system_clock::to_time_t(curr);

    auto truncate = curr.time_since_epoch().count() / 1000000;
    auto millisecs = truncate % 1000;

    buff << "[" << std::put_time(std::localtime(&tm), "%F %T")
      << "." << std::setw(3) << std::setfill('0') << millisecs << "] ";
  }

} // namespace util
} // namespace mapreduce