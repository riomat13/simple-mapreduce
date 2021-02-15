#ifndef SIMPLEMAPREDUCE_COMMONS_H_
#define SIMPLEMAPREDUCE_COMMONS_H_

#include <mutex>
#include <thread>

namespace mapreduce {
namespace commons {

static std::mutex mr_mutex;

}  // namespace commons
}  // namespace mapreduce

#endif