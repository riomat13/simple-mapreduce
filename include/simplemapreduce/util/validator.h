#ifndef SIMPLEMAPREDUCE_UTIL_VALIDATOR_H_
#define SIMPLEMAPREDUCE_UTIL_VALIDATOR_H_

namespace mapreduce {
namespace util {

/**
 * Check input data is valid.
 *    Input type:
 *      std::string: true when data is not empty.
 * 
 *  @param data&  input data to validate
 *  @return bool
 */
template <typename T>
bool is_valid_data(const T&);

} // namespace util
} // namespace mapreduce

#endif