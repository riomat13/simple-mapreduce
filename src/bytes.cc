#include "simplemapreduce/data/bytes.h"

#include <cstring>

namespace mapreduce {
namespace data {

/// Constructor
ByteData::ByteData(int data) { set_data_<int>(data); }
ByteData::ByteData(long data) { set_data_<long>(data); }
ByteData::ByteData(float data) { set_data_<float>(data); }
ByteData::ByteData(double data) { set_data_<double>(data); }
ByteData::ByteData(const std::string &data) { set_data(std::move(data)); }

/// Copy
ByteData::ByteData(const ByteData &rhs)
{
  this->data_ = rhs.data_;
  this->size_ = rhs.size_;
}

ByteData &ByteData::operator=(const ByteData &rhs)
{
  if (this == &rhs)
    return *this;

  this->data_ = rhs.data_;
  this->size_ = rhs.size_;
  return *this;
}

/// Move
ByteData::ByteData(ByteData &&rhs)
{
  this->data_ = std::move(rhs.data_);
  this->size_ = rhs.size_;
}

ByteData &ByteData::operator=(ByteData &&rhs)
{
  this->data_ = std::move(rhs.data_);
  this->size_ = rhs.size_;
  return *this;
}

/// Single values
void ByteData::set_data(int data) { set_data_<int>(data); }
void ByteData::set_data(long data) { set_data_<long>(data); }
void ByteData::set_data(float data) { set_data_<float>(data); }
void ByteData::set_data(double data) { set_data_<double>(data); }
void ByteData::set_data(std::string data)
{
  data_.clear();
  data_ = std::vector<char>(data.data(), data.data() + data.size());
  size_ = data.size();
}
void ByteData::set_data(int *data, const size_t &size) { set_data_<int>(data, size); }
void ByteData::set_data(long *data, const size_t &size) { set_data_<long>(data, size); }
void ByteData::set_data(float *data, const size_t &size) { set_data_<float>(data, size); }
void ByteData::set_data(double *data, const size_t &size) { set_data_<double>(data, size); }

template<> int ByteData::get_data() const { return get_data_<int>(); }
template<> long ByteData::get_data() const { return get_data_<long>(); }
template<> float ByteData::get_data() const { return get_data_<float>(); }
template<> double ByteData::get_data() const { return get_data_<double>(); }
template<> std::string ByteData::get_data() const { return std::string(data_.begin(), data_.end()); }

/// Array
template<>
std::vector<int> ByteData::get_data() const
{
  std::vector<int> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(int));
  return out;
}
template<>
std::vector<long> ByteData::get_data() const
{
  std::vector<long> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(long));
  return out;
}
template<> std::vector<float> ByteData::get_data() const
{
  std::vector<float> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(float));
  return out;
}
template<>
std::vector<double> ByteData::get_data() const
{
  std::vector<double> out(size_);
  std::memcpy(out.data(), data_.data(), size_ * sizeof(double));
  return out;
}

template<> void ByteData::push_back(int &value) { push_back_<int>(value); }
template<> void ByteData::push_back(long &value) { push_back_<long>(value); }
template<> void ByteData::push_back(float &value) { push_back_<float>(value); }
template<> void ByteData::push_back(double &value) { push_back_<double>(value); }

}  // namespace data
}  // namespace mapreduce