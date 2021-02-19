#include <string>
#include <utility>

#include "simplemapreduce/commons.h"
namespace mapreduce {
namespace proc {

template <typename K, typename V>
BinaryFileWriter<K, V>::BinaryFileWriter(const std::filesystem::path& path) : path_(std::move(path)) {
  fout_.open(path_, std::ios::binary | std::ios::trunc);
};

template <typename K, typename V>
BinaryFileWriter<K, V>::BinaryFileWriter(const std::string& path) : path_(std::move(path)) {
  fout_.open(path_, std::ios::binary | std::ios::trunc);
};

template <typename K, typename V>
BinaryFileWriter<K, V>::~BinaryFileWriter() {
  fout_.close();
}

template <typename K, typename V>
void BinaryFileWriter<K, V>::write(mapreduce::data::ByteData&& key, mapreduce::data::ByteData&& value) {
  std::lock_guard<std::mutex> lock(mapreduce::commons::mr_mutex);

  write_binary(fout_, std::move(key));
  write_binary(fout_, std::move(value));
}

template <typename K, typename V>
OutputWriter<K, V>::OutputWriter(const std::filesystem::path& path) {
  fout_.open(path, std::ios::out | std::ios::ate);
};

template <typename K, typename V>
OutputWriter<K, V>::OutputWriter(const std::string& path) {
  fout_.open(path, std::ios::out | std::ios::ate);
};

template <typename K, typename V>
OutputWriter<K, V>::~OutputWriter() {
  fout_.close();
}

template <typename K, typename V>
void OutputWriter<K, V>::write(mapreduce::data::ByteData&& key, mapreduce::data::ByteData&& value) {
  std::lock_guard<std::mutex> lock(mapreduce::commons::mr_mutex);

  write_output<K>(fout_, key.get_data<K>());
  fout_ << "\t";
  write_output<V>(fout_, value.get_data<V>());
  fout_ << "\n";
}

}  // namespace proc
}  // namespace mapreduce