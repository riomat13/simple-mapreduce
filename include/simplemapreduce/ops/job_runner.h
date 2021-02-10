#ifndef SIMPLEMAPREDUCE_OPS_JOB_RUNNER_H_
#define SIMPLEMAPREDUCE_OPS_JOB_RUNNER_H_

#include <vector>

namespace mapreduce {
namespace job {

class JobRunner
{
 public:
  JobRunner();
  ~JobRunner();

 protected:
  /**
   * Run Map/Reduce task
   */
  virtual void run(char* key, size_t key_size, char* value, size_t value_size) = 0;
};

}  // namespace job
}  // namespace mapreduce

#endif