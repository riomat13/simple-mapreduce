#ifndef SIMPLEMAPREDUCE_OPS_FUNC_H_
#define SIMPLEMAPREDUCE_OPS_FUNC_H_

#include <functional>
#include <numeric>

namespace mapreduce {

#ifdef HAS_TBB
#include <execution>

// Macro to calculate sum using tbb
// This can be used for int, long, float, double
#define REDUCE_SUM(a) std::reduce(std::execution::par, a.cbegin(), a.cend())
#else
// Macro to calculate sum
// This can be used for int, long, float, double
#define REDUCE_SUM(a) std::reduce(a.cbegin(), a.cend())
#endif  // HAS_TBB

// Macro to calculate average
// The output will be double
#define REDUCE_MEAN(a) static_cast<double>(REDUCE_SUM(a)) / a.size()

}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_OPS_FUNC_H_