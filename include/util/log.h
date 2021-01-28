#ifndef IS_COMMONS
#define IS_COMMONS
#include "commons.h"
#endif

#ifndef UTIL_LOG_H_
#define UTIL_LOG_H_

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace mapreduce {
namespace util {

  enum level {
    nonset,
    debug,
    info,
    warning,
    error,
    critical,
  };

  /// Reference: https://stackoverflow.com/a/50923834
  /**
   * Logger class.
   * 
   * Example usage:
   *  >>> logger.info("sample log: ", 10);
   *    => "[INFO] [%Y-%m-%s %H-%M-%S.%s] sample log: 10"
   */
  class Logger
  {
   public:
    /// Used for aggregate messages to make it atomic
    typedef std::ostringstream OSS;

    Logger(const Logger &) = delete;
    Logger &operator=(const Logger &) = delete;

    /**
     * Set logging level.
     * This will be applied to all logger methods.
     */
    void set_log_level(const int &level)
    {
      get_log_level() = level;
    }

    /**
     * Simple logger.
     * 
     *  @param log_level& log level
     *    0: NOTSET
     *    1: INFO
     *    2: DEBUG
     *    3: WARNING
     *    4: ERROR
     *    5: CRITICAL
     * 
     *  @params args      variable numbers of inputs to display as log
     */
    template <typename ...Args>
    void log(const unsigned int &log_level, Args&& ...args);

    template <typename ...Args>
    void info(Args&& ...args);

    template <typename ...Args>
    void debug(Args&& ...args);

    template <typename ...Args>
    void warning(Args&& ...args);

    template <typename ...Args>
    void error(Args&& ...args);

    template <typename ...Args>
    void critical(Args&& ...args);

   private:

    /**
     * Helper function to add timestamp tag
     */
    void log_append_time_tag(OSS &oss);

    /**
     * Root helper function to output log data on stdout
     */
    template <typename ...Args>
    void log_stdout_root(OSS &oss, Args&& ...args);

    /**
     * Root helper function to output log data on stderr
     */
    template <typename ...Args>
    void log_stderr_root(OSS &oss, Args&& ...args);

    /**
     * Helper function to output log data on stdout
     */
    template <typename T>
    void log_stdout(OSS &oss, T &t);

    /**
     * Helper function to output log data on stdout
     */
    template <typename T, typename ...Args>
    void log_stdout(OSS &oss, T &t, Args&& ...args);

    /**
     * Helper function to output log data on stderr
     */
    template <typename T>
    void log_stderr(OSS &oss, T &t);

    /**
     * Helper function to output log data on stderr
     */
    template <typename T, typename ...Args>
    void log_stderr(OSS &oss, T &t, Args ...args);

    int& get_log_level()
    {
      static int log_level = 1;
      return log_level;
    }

  };

} // namespace util
} // namespace mapreduce

#include "util/log.tcc"

#endif