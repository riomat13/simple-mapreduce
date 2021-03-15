#ifndef SIMPLEMAPREDUCE_DATA_TYPE_H_
#define SIMPLEMAPREDUCE_DATA_TYPE_H_

#include <cstdlib>
#include <string>

namespace mapreduce {
namespace type {

using Int16 = std::int16_t;
using Int = std::int32_t;
using Int32 = std::int32_t;
using Long = std::int64_t;
using Int64 = std::int64_t;
using Float = float;
using Double = double;
using String = std::string;

template <typename T1, typename T2>
using CompositeKey = std::pair<T1, T2>;

}  // namespace type
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_DATA_TYPE_H_