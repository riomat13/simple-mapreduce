#ifndef SIMPLEMAPREDUCE_COMMONS_H_
#define SIMPLEMAPREDUCE_COMMONS_H_

#include <cstdlib>
#include <mutex>
#include <thread>

namespace mapreduce {
namespace commons {

static std::mutex mr_mutex;

}  // namespace commons

namespace type {

using Size_t = std::uint64_t;

}  // namespace type
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_COMMONS_H_