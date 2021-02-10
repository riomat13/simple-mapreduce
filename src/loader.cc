#include "simplemapreduce/proc/loader.h"

#include <string>

namespace mapreduce {
namespace proc {

template <typename T>
ByteData load_byte_data_(std::ifstream &fin)
{
  T value;
  fin.read(reinterpret_cast<char *>(&value), sizeof(T));
  if (fin.eof())
    return ByteData();
  return ByteData(value);
}

template<>
ByteData load_byte_data<int>(std::ifstream &fin)
{
  return load_byte_data_<int>(fin);
}

template<>
ByteData load_byte_data<long>(std::ifstream &fin)
{
  return load_byte_data_<long>(fin);
}

template<>
ByteData load_byte_data<float>(std::ifstream &fin)
{
  return load_byte_data_<float>(fin);
}

template<>
ByteData load_byte_data<double>(std::ifstream &fin)
{
  return load_byte_data_<double>(fin);
}

template<>
ByteData load_byte_data<std::string>(std::ifstream &fin)
{
  size_t key_size;
  fin.read(reinterpret_cast<char*>(&key_size), sizeof(size_t));

  if (fin.eof())
    return ByteData();

  char keydata[key_size];
  fin.read(keydata, sizeof(char) * key_size);

  ByteData data;
  data.set_bytes<std::string>(&keydata[0], key_size);
  return data;
}

BytePair MQDataLoader::get_item()
{
  /// Fetch data from MessageQueue
  /// A key of last element will be empty.
  return mq_->receive();
}

} // namespace proc
} // namespace mapreduce