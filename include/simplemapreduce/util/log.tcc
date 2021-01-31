namespace mapreduce {
namespace util {

  /// Reference: https://stackoverflow.com/a/50923834
  template <typename ...Args>
  void Logger::log(const unsigned int &log_level, Args&& ...args)
  {
    if (log_level > 5 || log_level < get_log_level())
    {
      std::cerr << "[WARNING] Invalid input for logger" << std::endl;
      return;
    }

    if (log_level == 2)
      info(args...);
    else if (log_level == 1)
      debug(args...);
    else if (log_level == 4)
      error(args...);
    else if (log_level == 3)
      warning(args...);
    else if (log_level == 5)
      critical(args...);
    else
      log_stdout_root(args...);
  }

  template <typename ...Args>
  void Logger::info(Args&& ...args)
  {
    if (get_log_level() > 2) return;

    std::lock_guard<std::mutex> lock(mr_mutex_);
    OSS oss{};
    oss << "\033[0;92m[INFO] ";
    log_stdout_root(oss, args...);
  }

  template <typename ...Args>
  void Logger::debug(Args&& ...args)
  {
    if (get_log_level() > 1) return;

    std::lock_guard<std::mutex> lock(mr_mutex_);
    OSS oss{};
    oss << "[DEBUG] ";
    log_stdout_root(oss, args...);
  }

  template <typename ...Args>
  void Logger::warning(Args&& ...args)
  {
    if (get_log_level() > 3) return;

    std::lock_guard<std::mutex> lock(mr_mutex_);
    OSS oss{};
    oss << "\033[0;33m[WARNING] ";
    log_stderr_root(oss, args...);
  }

  template <typename ...Args>
  void Logger::error(Args&& ...args)
  {
    if (get_log_level() > 4) return;

    std::lock_guard<std::mutex> lock(mr_mutex_);
    OSS oss{};
    oss << "\033[0;91m[ERROR] ";
    log_stderr_root(oss, args...);
  }

  template <typename ...Args>
  void Logger::critical(Args&& ...args)
  {
    std::lock_guard<std::mutex> lock(mr_mutex_);
    OSS oss{};
    oss << "\033[1;31m[UREGENT] ";
    log_stderr_root(oss, args...);
  }

  template <typename ...Args>
  void Logger::log_stdout_root(OSS &oss, Args&& ...args)
  {
    /// add timestamp to log stream
    log_append_time_tag(oss);

    log_stdout(oss, args...);
    oss << "\033[0m";

    std::cout << oss.str() << std::endl;
    oss.clear();
  }

  template <typename ...Args>
  void Logger::log_stderr_root(OSS &oss, Args&& ...args)
  {
    /// add timestamp to log stream
    log_append_time_tag(oss);

    log_stdout(oss, args...);
    oss << "\033[0m";

    std::cerr << oss.str() << std::endl;
    oss.clear();
  }

  template <typename T>
  void Logger::log_stdout(OSS &oss, T &t)
  {
    oss << t;
  }

  template <typename T, typename ...Args>
  void Logger::log_stdout(OSS &oss, T &t, Args&& ...args)
  {
    oss << t;
    log_stdout(oss, args...);
  }

  template <typename T>
  void Logger::log_stderr(OSS &oss, T &t)
  {
    oss << t;
  }

  template <typename T, typename ...Args>
  void Logger::log_stderr(OSS &oss, T &t, Args ...args)
  {
    oss << t;
    log_stderr(oss, args...);
  }

} // namespace util
} // namespace mapreduce

extern mapreduce::util::Logger logger;