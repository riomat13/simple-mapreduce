#include <cstring>

namespace mapreduce {
namespace data {

template <typename T>
void ByteData::set_bytes(char *data, const size_t &size)
{
  data_ = std::vector<char>(data, data + size);
  size_ = size / sizeof(T);
}

template <typename T>
void ByteData::set_data_(T &data)
{
  data_.clear();
  char *buff = reinterpret_cast<char *>(&data);
  data_ = std::vector<char>(buff, buff + sizeof(T));

  size_ = 1;
}

template <typename T>
void ByteData::set_data_(T *data, const size_t &size)
{
  data_.clear();
  char *buff = reinterpret_cast<char *>(data);
  data_ = std::vector<char>(buff, buff + sizeof(T) * size);
  size_ = size;
}

template<typename T>
T ByteData::get_data_() const
{
  T data;
  /// Only used on the same machine so no need to consider endianness
  std::memcpy(&data, data_.data(), sizeof(T) * size_);
  return data;
}

} // namespace data
} // namespace mapreduce