#include <string>
#include <utility>

#include "simplemapreduce/commons.h"
namespace mapreduce {
namespace proc {

template <typename K, typename V>
BinaryFileWriter<K, V>::BinaryFileWriter(const fs::path &path) : path_(std::move(path))
{
  fout_.open(path_, std::ios::binary | std::ios::trunc);
};

template <typename K, typename V>
BinaryFileWriter<K, V>::BinaryFileWriter(const std::string &path) : path_(std::move(path))
{
  fout_.open(path_, std::ios::binary | std::ios::trunc);
};

template <typename K, typename V>
BinaryFileWriter<K, V>::~BinaryFileWriter()
{
  fout_.close();
}

template <typename K, typename V>
void BinaryFileWriter<K, V>::write(const ByteData &key, const ByteData &value)
{
  std::lock_guard<std::mutex> lock(mr_mutex_);

  /// TODO: directory write to file
  K keydata = key.get_data<K>();
  write_binary<K>(fout_, keydata);
  V valuedata = value.get_data<V>();
  write_binary<V>(fout_, valuedata);
}

template <typename K, typename V>
OutputWriter<K, V>::OutputWriter(const fs::path &path) {
  fout_.open(path, std::ios::out | std::ios::ate);
};

template <typename K, typename V>
OutputWriter<K, V>::OutputWriter(const std::string &path) {
  fout_.open(path, std::ios::out | std::ios::ate);
};

template <typename K, typename V>
OutputWriter<K, V>::~OutputWriter()
{
  fout_.close();
}

template <typename K, typename V>
void OutputWriter<K, V>::write(const ByteData &key, const ByteData &value)
{
  std::lock_guard<std::mutex> lock(mr_mutex_);

  K keydata = key.get_data<K>();
  write_output<K>(fout_, keydata);
  fout_ << "\t";
  V valuedata = value.get_data<V>();
  write_output<V>(fout_, valuedata);
  fout_ << "\n";
}

} // namespace proc
} // namespace mapreduce