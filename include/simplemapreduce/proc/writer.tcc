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
  void BinaryFileWriter<K, V>::write(K &key, V &value)
  {
    std::lock_guard<std::mutex> lock(mr_mutex_);

    write_binary<K>(fout_, key);
    write_binary<V>(fout_, value);
  }

  template <typename K, typename V>
  void MQWriter<K, V>::write(K &key, V &value)
  {
    mq_->send(std::make_pair<K, V>(std::move(key), std::move(value)));
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
  void OutputWriter<K, V>::write(K &key, V &value)
  {
    std::lock_guard<std::mutex> lock(mr_mutex_);

    write_output<K>(fout_, key);
    fout_ << "\t";
    write_output<V>(fout_, value);
    fout_ << "\n";
  }

} // namespace proc
} // namespace mapreduce