#ifndef SIMPLEMAPREDUCE_UTIL_TYPE_CHECK_H_
#define SIMPLEMAPREDUCE_UTIL_TYPE_CHECK_H_

#include <type_traits>

namespace mapreduce {
namespace util {

/** Helper type checking for vector. */
template <typename T>
using is_vector = std::is_same<T, std::vector<typename T::value_type>>;

template <typename T>
using is_compositekey = std::is_same<T, mapreduce::type::CompositeKey<typename T::first_type, typename T::second_type>>;

}  // namespace util
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_UTIL_TYPE_CHECK_H_