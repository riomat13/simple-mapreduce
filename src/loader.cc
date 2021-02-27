#include "simplemapreduce/proc/loader.h"

#include <string>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/type.h"

using namespace mapreduce::data;
using namespace mapreduce::type;

namespace mapreduce {
namespace proc {

template <typename T>
inline ByteData load_byte_data_(std::ifstream& fin) {
  char buffer[sizeof(T)];
  fin.read(&buffer[0], sizeof(T));
  if (fin.eof())
    return ByteData();

  ByteData data;
  data.set_bytes<T>(buffer, sizeof(T));
  return data;
}

template<>
ByteData load_byte_data<Int16>(std::ifstream& fin) {
  return load_byte_data_<Int16>(fin);
}

template<>
ByteData load_byte_data<Int>(std::ifstream& fin) {
  return load_byte_data_<Int>(fin);
}

template<>
ByteData load_byte_data<Long>(std::ifstream& fin) {
  return load_byte_data_<Long>(fin);
}

template<>
ByteData load_byte_data<Float>(std::ifstream& fin) {
  return load_byte_data_<Float>(fin);
}

template<>
ByteData load_byte_data<Double>(std::ifstream& fin) {
  return load_byte_data_<Double>(fin);
}

template<>
ByteData load_byte_data<String>(std::ifstream& fin) {
  Size_t data_size;
  fin.read(reinterpret_cast<char*>(&data_size), sizeof(Size_t));

  if (fin.eof())
    return ByteData();

  char buffer[data_size];
  fin.read(buffer, sizeof(char) * data_size);

  ByteData data;
  data.set_bytes<String>(&buffer[0], data_size);
  return data;
}

BytePair MQDataLoader::get_item() {
  /// Fetch data from MessageQueue
  /// A key of last element will be empty.
  return mq_->receive();
}

} // namespace proc
} // namespace mapreduce