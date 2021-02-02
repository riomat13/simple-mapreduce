#include "simplemapreduce/proc/sorter.h"

#include "simplemapreduce/ops/job.h"

namespace mapreduce {
namespace proc {

  /* --------------------------------------------------
   *   FileLoader
   * -------------------------------------------------- */
  template<>
  std::pair<std::string, int> FileLoader<std::string, int>::get_item()
  {
    /// Load data
    std::string key;
    int value{0};
    get_item_(key, value);

    return std::make_pair<std::string, int>(std::move(key), std::move(value));
  }

  template<>
  std::pair<std::string, long> FileLoader<std::string, long>::get_item()
  {
    /// Load data
    std::string key;
    long value{0};
    get_item_(key, value);

    return std::make_pair<std::string, long>(std::move(key), std::move(value));
  }

  template<>
  std::pair<std::string, float> FileLoader<std::string, float>::get_item()
  {
    /// Load data
    std::string key;
    float value{0.0};
    get_item_(key, value);

    return std::make_pair<std::string, float>(std::move(key), std::move(value));
  }

  template<>
  std::pair<std::string, double> FileLoader<std::string, double>::get_item()
  {
    /// Load data
    std::string key;
    double value{0.0};
    get_item_(key, value);

    return std::make_pair<std::string, double>(std::move(key), std::move(value));
  }

  /* --------------------------------------------------
   *   Sorter
   * -------------------------------------------------- */
  template<>
  std::map<std::string, std::vector<int>> Sorter<std::string, int>::run()
  {
    kvmap container = run_();
    return container;
  }

  template<>
  std::map<std::string, std::vector<long>> Sorter<std::string, long>::run()
  {
    kvmap container = run_();
    return container;
  }

  template<>
  std::map<std::string, std::vector<float>> Sorter<std::string, float>::run()
  {
    kvmap container = run_();
    return container;
  }

  template<>
  std::map<std::string, std::vector<double>> Sorter<std::string, double>::run()
  {
    kvmap container = run_();
    return container;
  }

} // namespace proc
} // namespace mapreduce