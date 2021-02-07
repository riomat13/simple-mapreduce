#include "simplemapreduce/proc/sorter.h"

#include "simplemapreduce/ops/job.h"

namespace mapreduce {
namespace proc {

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