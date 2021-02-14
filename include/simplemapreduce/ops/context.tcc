#include "simplemapreduce/ops/context.h"

#include <filesystem>
#include <fstream>
#include <sstream>

#include <mpi.h>

#include "simplemapreduce/data/bytes.h"

namespace fs = std::filesystem;

using namespace mapreduce::data;

namespace mapreduce {

template <typename K, typename V>
Context<K, V>::Context(Context&& rhs)
{
  this->writer_ = std::move(rhs.writer_);
}

template <typename K, typename V>
Context<K, V>& Context<K, V>::operator=(Context&& rhs)
{
  this->writer_ = std::move(rhs.writer_);
  return *this;
}

template <typename K, typename V>
void Context<K, V>::write(K& key, V& value) const
{
  ByteData k(std::move(key)), v(std::move(value));
  writer_->write(k, v);
}

} // namespace mapreduce