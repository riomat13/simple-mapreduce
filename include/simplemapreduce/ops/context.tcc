#include "simplemapreduce/ops/context.h"

#include <filesystem>
#include <fstream>
#include <sstream>

#include <mpi.h>

#include "simplemapreduce/util/log.h"

namespace fs = std::filesystem;

namespace mapreduce {

  template <typename K, typename V>
  Context<K, V>::Context(Context &&rhs)
  {
    this->writer_ = std::move(rhs.writer_);
  }

  template <typename K, typename V>
  Context<K, V> &Context<K, V>::operator=(Context &&rhs)
  {
    this->writer_ = std::move(rhs.writer_);
    return *this;
  }

  template <typename K, typename V>
  void Context<K, V>::write(K &key, V &value) const
  {
    writer_->write(key, value);
  }

} // namespace mapreduce