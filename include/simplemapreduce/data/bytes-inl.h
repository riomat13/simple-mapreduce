#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace mapreduce {
namespace data {

template <typename T1, typename T2>
ByteData::ByteData(mapreduce::type::CompositeKey<T1, T2>&& data) {
  set_data(std::move(data.first));
  // use SOH as separator of first and second data
  data_.push_back('\1');
  push_back<T2>(data.second);
  size_ = 2;
}

template <typename T>
void ByteData::set_bytes(char* data, const size_t& size) {
  data_ = std::vector<char>(data, data + size);
  size_ = size / sizeof(T);
}

template <typename T>
void ByteData::set_data_(T& data) {
  data_.clear();
  char* buff = reinterpret_cast<char*>(&data);
  data_ = std::vector<char>(buff, buff + sizeof(T));
  size_ = 1;
}

template <typename T>
void ByteData::set_data_(T* data, const size_t& size) {
  data_.clear();
  char* buff = reinterpret_cast<char*>(data);
  data_ = std::vector<char>(buff, buff + sizeof(T) * size);
  size_ = size;
}

template <typename T>
T ByteData::get_data_() const {
  T data;
  /// Only used on the same machine so no need to consider endianness
  size_ ? std::memcpy(&data, data_.data(), sizeof(T) * size_)
        : std::memcpy(&data, data_.data(), sizeof(T));

  return data;
}

template <typename T>
T ByteData::get_data_(size_t offset) const {
  T data;
  std::memcpy(&data, data_.data() + offset, sizeof(T));
  return data;
}

template <typename T>
T ByteData::get_data_(size_t start, size_t end) const {
  T data;
  std::memcpy(&data, data_.data() + start, end - start);
  return data;
}

template<> mapreduce::type::String ByteData::get_data_(size_t, size_t) const;

template <typename T, std::enable_if_t<mapreduce::util::is_compositekey<T>::value, bool>>
T ByteData::get_data() const {
  typename T::first_type first;
  auto divider = std::find(data_.begin(), data_.end(), '\1');
  if (divider == data_.end()) {
    throw std::runtime_error("Invalid call: this is not a pair.");
  }

  if (std::is_same<typename T::first_type, mapreduce::type::String>()) {
    first = get_data_<typename T::first_type>(0, divider - data_.begin());
  } else {
    first = get_data_<typename T::first_type>(0, sizeof(typename T::first_type));
  }

  typename T::second_type second;
  auto start = divider - data_.begin() + 1;
  if (std::is_same<typename T::second_type, mapreduce::type::String>()) {
    second = get_data_<typename T::second_type>(start, data_.size());
  } else {
    second = get_data_<typename T::second_type>(start, start + sizeof(typename T::second_type));
  }

  return mapreduce::type::CompositeKey<typename T::first_type, typename T::second_type>(first, second);
}

template <typename T>
void ByteData::push_back_(T& data) {
  char* buff = reinterpret_cast<char*>(&data);
  data_.insert(data_.end(), buff, buff + sizeof(T));
  ++size_;
}

}  // namespace data
}  // namespace mapreduce