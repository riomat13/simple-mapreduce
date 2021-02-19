#include <memory>

namespace mapreduce {
namespace util {

  template <typename Arg>
  LogBuffer& LogBuffer::operator<<(Arg&& arg) {
    oss << arg;
    return *this;
  }

  /// Reference: https://stackoverflow.com/a/50923834
  template <typename ...Args>
  std::unique_ptr<LogBuffer> Logger::log(const LogLevel& log_level, Args&&... args) {
    switch (log_level) {
      case LogLevel::INFO:
        return info(args...);
      case LogLevel::DEBUG:
        return debug(args...);
      case LogLevel::WARNING:
        return warning(args...);
      case LogLevel::ERROR:
        return error(args...);
      case LogLevel::CRITICAL:
        return critical(args...);
      default:
        std::unique_ptr<LogBuffer> buff = std::make_unique<LogBuffer>();
        log_stdout_root(*buff, args...);
        return buff;
    }
  }

  template <typename ...Args>
  std::unique_ptr<LogBuffer> Logger::info(Args&&... args) {
    if (get_log_level() > LogLevel::INFO)
      return nullptr;

    std::unique_ptr<LogBuffer> buff = std::make_unique<LogBuffer>();

    std::lock_guard<std::mutex> lock(mapreduce::commons::mr_mutex);
    *buff << "\033[0;92m[INFO] ";
    log_stdout_root(*buff, args...);
    return buff;
  }

  template <typename ...Args>
  std::unique_ptr<LogBuffer> Logger::debug(Args&&... args) {
    if (get_log_level() > LogLevel::DEBUG)
      return nullptr;

    std::unique_ptr<LogBuffer> buff = std::make_unique<LogBuffer>();

    std::lock_guard<std::mutex> lock(mapreduce::commons::mr_mutex);
    *buff << "[DEBUG] ";
    log_stdout_root(*buff, args...);
    return buff;
  }

  template <typename ...Args>
  std::unique_ptr<LogBuffer> Logger::warning(Args&&... args) {
    if (get_log_level() > LogLevel::WARNING)
      return nullptr;

    std::unique_ptr<LogBuffer> buff = std::make_unique<LogBuffer>();

    std::lock_guard<std::mutex> lock(mapreduce::commons::mr_mutex);
    *buff << "\033[0;33m[WARNING] ";
    log_stderr_root(*buff, args...);
    return buff;
  }

  template <typename ...Args>
  std::unique_ptr<LogBuffer> Logger::error(Args&&... args) {
    if (get_log_level() > LogLevel::ERROR)
      return nullptr;

    std::unique_ptr<LogBuffer> buff = std::make_unique<LogBuffer>();

    std::lock_guard<std::mutex> lock(mapreduce::commons::mr_mutex);
    *buff << "\033[0;91m[ERROR] ";
    log_stderr_root(*buff, args...);
    return buff;
  }

  template <typename ...Args>
  std::unique_ptr<LogBuffer> Logger::critical(Args&&... args) {
    if (get_log_level() > LogLevel::CRITICAL)
      return nullptr;

    std::unique_ptr<LogBuffer> buff = std::make_unique<LogBuffer>();

    std::lock_guard<std::mutex> lock(mapreduce::commons::mr_mutex);
    *buff << "\033[1;35m[URGENT] ";
    log_stderr_root(*buff, args...);
    return buff;
  }

  template <typename ...Args>
  void Logger::log_stdout_root(LogBuffer &buff, Args&&... args) {
    /// add timestamp to log stream
    log_append_time_tag(buff);

    log_stdout(buff, args...);
    buff << "\033[0m";

    std::cout << buff.to_string() << std::endl;
  }

  template <typename ...Args>
  void Logger::log_stderr_root(LogBuffer &buff, Args&&... args) {
    /// add timestamp to log stream
    log_append_time_tag(buff);

    log_stdout(buff, args...);
    buff << "\033[0m";

    std::cerr << buff.to_string() << std::endl;
  }

  template <typename T>
  void Logger::log_stdout(LogBuffer &buff, T &t) {
    buff << t;
  }

  template <typename T, typename ...Args>
  void Logger::log_stdout(LogBuffer &buff, T &t, Args&&... args) {
    buff << t;
    log_stdout(buff, args...);
  }

  template <typename T>
  void Logger::log_stderr(LogBuffer &buff, T &t) {
    buff << t;
  }

  template <typename T, typename ...Args>
  void Logger::log_stderr(LogBuffer &buff, T &t, Args... args) {
    buff << t;
    log_stderr(buff, args...);
  }

} // namespace util
} // namespace mapreduce