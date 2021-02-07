#include "simplemapreduce/util/validator.h"

#include <string>

namespace mapreduce {
namespace util {

/**
 * Check input string data is valid.
 * If data is empty, the result will be false, otherwise true.
 * 
 *  @param data&  input data to validate
 *  @return bool
 */
template<>
bool is_valid_data(const std::string &data)
{
  return !data.empty();
}

} // namespace util
} // namespace mapreduce