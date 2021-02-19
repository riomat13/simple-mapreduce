#ifndef SIMPLEMAPREDUCE_OPS_CONTEXT_H_
#define SIMPLEMAPREDUCE_OPS_CONTEXT_H_

#include <fstream>
#include <memory>
#include <string>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/proc/writer.h"

namespace mapreduce {

/**
 * Write key and value items to send through network
 */
template <typename K, typename V>
class Context {
 public:
  Context(std::unique_ptr<mapreduce::proc::Writer> writer) : writer_(std::move(writer)) {}

  Context(const Context&) = delete;
  Context &operator=(const Context&) = delete;
  Context(Context&&);
  Context &operator=(Context&&);

  /**
   * Send provided key and value item
   * This will convery to binary format and then pass to processor.
   * 
   *  @param key   key item to be written
   *  @param value value item to be written
   */
  void write(K& key, V& value) const ;

 private:
  std::unique_ptr<mapreduce::proc::Writer> writer_ = nullptr;
};

} // namespace mapreduce

#include "simplemapreduce/ops/context-inl.h"

#endif  // SIMPLEMAPREDUCE_OPS_CONTEXT_H_