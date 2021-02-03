#include "simplemapreduce/ops/context.h"

#include <filesystem>
#include <fstream>
#include <sstream>

#include <mpi.h>

#include "simplemapreduce/util/log.h"

namespace fs = std::filesystem;

namespace mapreduce {

  Context::Context(Context &&rhs)
  {
    this->writer_ = std::move(rhs.writer_);
  }

  Context &Context::operator=(Context &&rhs)
  {
    this->writer_ = std::move(rhs.writer_);
    return *this;
  }

  void Context::write(std::string &key, int &value) const
  {
    writer_->write(key, value);
  }

  void Context::write(std::string &key, long &value) const
  {
    writer_->write(key, value);
  }

  void Context::write(std::string &key, float &value) const
  {
    writer_->write(key, value);
  }

  void Context::write(std::string &key, double &value) const
  {
    writer_->write(key, value);
  }

} // namespace mapreduce