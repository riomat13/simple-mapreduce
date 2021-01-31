#ifndef SIMPLEMAPREDUCE_OPS_CONTEXT_H_
#define SIMPLEMAPREDUCE_OPS_CONTEXT_H_

#include <fstream>
#include <memory>
#include <string>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/proc/writer.h"

using namespace mapreduce::proc;

namespace mapreduce {

  /**
   * Write key and value items to send through network
   */
  class Context
  {
   public:
    Context(std::unique_ptr<Writer> writer) : writer_(std::move(writer)) {}

    Context(const Context&) = delete;
    Context &operator=(const Context&) = delete;
    Context(Context&&);
    Context &operator=(Context&&);

    /**
     * Send provided key and value item
     * This does not accept rvalue reference to prevent confusing.
     * 
     *  @param key&   key item to be written
     *  @param value& value item to be written
     */
    /// TODO: add patterns
    void write(std::string &key, int &value) const ;
    void write(std::string &key, long &value) const ;
    void write(std::string &key, float &value) const ;
    void write(std::string &key, double &value) const ;

   private:
    std::unique_ptr<Writer> writer_ = nullptr;
  };

} // namespace mapreduce

#endif