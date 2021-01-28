#include "ops/context.h"

#include <filesystem>
#include <fstream>
#include <sstream>

#include <mpi.h>

#include "util/log.h"

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

  void Context::write(std::string &, float &) const
  {
    logger.error("Not implemented");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  void Context::write(std::string &, double &) const
  {
    logger.error("Not implemented");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

} // namespace mapreduce