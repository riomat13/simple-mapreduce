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

#include "simplemapreduce/util/log.h"

namespace mapreduce {
namespace util {

extern Logger logger;

}  // namespace util
}  // namespace mapreduce

#endif  // SIMPLEMAPREDUCE_COMMONS_H_