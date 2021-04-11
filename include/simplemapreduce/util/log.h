#ifndef IS_COMMONS
#define IS_COMMONS
#include "simplemapreduce/commons.h"
#endif  // IS_COMMONS

#ifndef SIMPLEMAPREDUCE_UTIL_LOG_H_
#define SIMPLEMAPREDUCE_UTIL_LOG_H_

#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace {
  using OSS = std::ostringstream;
}

namespace mapreduce {
namespace util {

enum LogLevel {
  NONSET,
  DEBUG,
  INFO,
  WARNING,
  ERROR,
  CRITICAL,
  DISABLE,
};

class LogBuffer {
 public:
  ~LogBuffer() { oss.clear(); }

  /** Insert data into stream. */
  template <typename Arg>
  LogBuffer& operator<<(Arg&&);

  /** Get stream data as string. */
  std::string to_string();

 private:
  /// Log string stream to store formatted data
  OSS oss;
};

/**
 * Writer class for log.
 * This is a simple wrapper class of file stream.
 */
class LogWriter {
 public:
  /**
   * Constructor of log writer.
   *
   *  @param path   file path to write out logs
   */
  LogWriter(const std::string&);
  LogWriter(const std::filesystem::path&);
  ~LogWriter();

  /**
   * Write data to a file.
   * New line will be appended to the end of string on each call.
   */
  void write(const std::string&);

  /**
   * Flush stream to file.
   * This calls `ofstream::flush()`.
   */
  void flush();

 private:
  std::ofstream ofs_;
};

/**
 * Logger class.
 *
 * Example usage:
 *  >>> logger.info("sample log: ", 10);
 *    => "[INFO] [%Y-%m-%s %H-%M-%S.%s] sample log: 10"
 */
class Logger {
  public:
  /**
   * Logger constructor.
   *
   *  @param path file path to write out logs
   */
  Logger() {}
  Logger(const std::string&);

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(Logger&&) = delete;

  /**
   * Set logging level.
   * This will be applied to all logger methods.
   * This is also be used for writing out to a file
   * unless specify the level by `set_log_level_for_file`.
   */
  void set_log_level(LogLevel&&);

  /**
   * Set file path to write logs.
   *
   *  @param path   target file path
   */
  void set_filepath(const std::string&);
  void set_filepath(const std::filesystem::path&);

  /**
   * Set logging level specifically for file.
   */
  void set_log_level_for_file(LogLevel&&);

  /**
   * Simple logger.
   * 
   *  @param log_level  log level
   *    0: NOTSET
   *    1: DEBUG
   *    2: INFO
   *    3: WARNING
   *    4: ERROR
   *    5: CRITICAL
   *       DISABLE (not recommended since even CRITICAL will be suppressed)
   *
   *  @params args      variable numbers of inputs to display as log
   */
  template <typename ...Args>
  std::unique_ptr<LogBuffer> log(const LogLevel&, Args&&...);

  /** Debug log for LogLevel::DEBUG or lower */
  template <typename ...Args>
  std::unique_ptr<LogBuffer> debug(Args&&...);

  /** Info log for LogLevel::INFO or lower */
  template <typename ...Args>
  std::unique_ptr<LogBuffer> info(Args&&...);

  /** Warning log for LogLevel::WARNING or lower */
  template <typename ...Args>
  std::unique_ptr<LogBuffer> warning(Args&&...);

  /** Error log for LogLevel::ERROR or lower */
  template <typename ...Args>
  std::unique_ptr<LogBuffer> error(Args&&...);

  /** Critical log for LogLevel::CRITICAL or lower */
  template <typename ...Args>
  std::unique_ptr<LogBuffer> critical(Args&&...);

  private:
  /** Helper function to add timestamp tag. */
  void log_append_time_tag(LogBuffer&);

  /** Root helper function to output log data on stdout. */
  template <typename ...Args>
  void log_stdout_root(LogBuffer&, Args&&...);

  /** Root helper function to output log data on stderr. */
  template <typename ...Args>
  void log_stderr_root(LogBuffer&, Args&&...);

  /** Helper function to output log data on stdout. */
  template <typename T>
  void log_stdout(LogBuffer&, T&);

  /** Helper function to output log data on stdout. */
  template <typename T, typename ...Args>
  void log_stdout(LogBuffer&, T&, Args&&...);

  /** Helper function to output log data on stderr. */
  template <typename T>
  void log_stderr(LogBuffer&, T&);

  /** Helper function to output log data on stderr. */
  template <typename T, typename ...Args>
  void log_stderr(LogBuffer&, T&, Args...);

  /** Get log level reference to update or check the current status */
  LogLevel& get_log_level() {
    static LogLevel log_level = LogLevel::INFO;
    return log_level;
  }

  LogLevel& get_file_log_level() {
    static LogLevel log_level = LogLevel::INFO;
    return log_level;
  }

  bool is_set_file = false;
  std::unique_ptr<LogWriter> log_writer_ = nullptr;
};

/** Logger used in globally. */
Logger& get_logger();

} // namespace util
} // namespace mapreduce

#include "simplemapreduce/util/log-inl.h"

#endif  // SIMPLEMAPREDUCE_UTIL_LOG_H_