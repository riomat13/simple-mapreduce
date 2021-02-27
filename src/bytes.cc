#include "simplemapreduce/data/bytes.h"

#include <cstring>
#include <fstream>

namespace fs = std::filesystem;

using namespace mapreduce::type;

namespace mapreduce {
namespace data {

/// Constructor
ByteData::ByteData(Int16 data) { set_data_<Int16>(data); }
ByteData::ByteData(Int data) { set_data_<Int>(data); }
ByteData::ByteData(Long data) { set_data_<Long>(data); }
ByteData::ByteData(Float data) { set_data_<Float>(data); }
ByteData::ByteData(Double data) { set_data_<Double>(data); }
ByteData::ByteData(String&& data) { set_data(std::move(data)); }

/// Copy
ByteData::ByteData(const ByteData& rhs) {
  this->data_ = rhs.data_;
  this->size_ = rhs.size_;
}

ByteData& ByteData::operator=(const ByteData& rhs) {
  if (this == &rhs)
    return *this;

  this->data_ = rhs.data_;
  this->size_ = rhs.size_;
  return *this;
}

/// Move
ByteData::ByteData(ByteData&& rhs) {
  this->data_ = std::move(rhs.data_);
  this->size_ = rhs.size_;
}

ByteData& ByteData::operator=(ByteData&& rhs) {
  this->data_ = std::move(rhs.data_);
  this->size_ = rhs.size_;
  return *this;
}

/// Single values
void ByteData::set_data(Int16 data) noexcept { set_data_<Int16>(data); }
void ByteData::set_data(Int data) noexcept { set_data_<Int>(data); }
void ByteData::set_data(Long data) noexcept { set_data_<Long>(data); }
void ByteData::set_data(Float data) noexcept { set_data_<Float>(data); }
void ByteData::set_data(Double data) noexcept { set_data_<Double>(data); }
void ByteData::set_data(String&& data) noexcept
{
  data_.clear();
  data_ = std::vector<char>(data.begin(), data.end());
  size_ = data.size();
}
void ByteData::set_data(Int16* data, const size_t& size) { set_data_<Int16>(data, size); }
void ByteData::set_data(Int* data, const size_t& size) { set_data_<Int>(data, size); }
void ByteData::set_data(Long* data, const size_t& size) { set_data_<Long>(data, size); }
void ByteData::set_data(Float* data, const size_t& size) { set_data_<Float>(data, size); }
void ByteData::set_data(Double* data, const size_t& size) { set_data_<Double>(data, size); }

void ByteData::read_file(const std::string& path) {
  auto data_size = fs::file_size(path);
  data_ = std::vector<char>(data_size);
  std::ifstream ifs(path);
  ifs.read(data_.data(), data_size);
  size_ = data_size;
}

void ByteData::read_file(const fs::path& path) {
  auto data_size = fs::file_size(path);
  data_ = std::vector<char>(data_size);
  std::ifstream ifs(path);
  ifs.read(data_.data(), data_size);
  size_ = data_size;
}

template<> Int16 ByteData::get_data() const { return get_data_<Int16>(); }
template<> Int ByteData::get_data() const { return get_data_<Int>(); }
template<> Long ByteData::get_data() const { return get_data_<Long>(); }
template<> Float ByteData::get_data() const { return get_data_<Float>(); }
template<> Double ByteData::get_data() const { return get_data_<Double>(); }
template<> String ByteData::get_data() const { return String(data_.begin(), data_.end()); }

/// Array
template<>
std::vector<Int16> ByteData::get_data() const {
  std::vector<Int16> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(Int16));
  return out;
}
template<>
std::vector<Int> ByteData::get_data() const {
  std::vector<Int> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(Int));
  return out;
}
template<>
std::vector<Long> ByteData::get_data() const {
  std::vector<Long> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(Long));
  return out;
}
template<> std::vector<Float> ByteData::get_data() const {
  std::vector<Float> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(Float));
  return out;
}
template<>
std::vector<Double> ByteData::get_data() const {
  std::vector<Double> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(Double));
  return out;
}

template<> void ByteData::push_back(Int16& value) { push_back_<Int16>(value); }
template<> void ByteData::push_back(Int& value) { push_back_<Int>(value); }
template<> void ByteData::push_back(Long& value) { push_back_<Long>(value); }
template<> void ByteData::push_back(Float& value) { push_back_<Float>(value); }
template<> void ByteData::push_back(Double& value) { push_back_<Double>(value); }

bool ByteData::operator==(const ByteData& rhs) const {
  return data_ == rhs.data_;
}

bool ByteData::operator!=(const ByteData& rhs) const {
  return !(data_ == rhs.data_);
}

bool ByteData::operator<(const ByteData& rhs) const {
  if (size_ == 1 && rhs.size() == 1 && data_.size() == rhs.bsize()) {
    if (data_.size() == 2) {
      return get_data<Int16>() < rhs.get_data<Int16>();
    } else if (data_.size() == 4) {
      return get_data<Int32>() < rhs.get_data<Int32>();
    } else if (data_.size() == 8) {
      return get_data<Int64>() < rhs.get_data<Int64>();
    }
  }

  return data_ < rhs.data_;
}

bool ByteData::operator>(const ByteData& rhs) const {
  if (size_ == 1 && rhs.size() == 1 && data_.size() == rhs.bsize()) {
    if (data_.size() == 2) {
      return get_data<Int16>() > rhs.get_data<Int16>();
    } else if (data_.size() == 4) {
      return get_data<Int32>() > rhs.get_data<Int32>();
    } else if (data_.size() == 8) {
      return get_data<Int64>() > rhs.get_data<Int64>();
    }
  }

  return data_ > rhs.data_;
}

}  // namespace data
}  // namespace mapreduce