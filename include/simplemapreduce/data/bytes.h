#ifndef SIMPLEMAPREDUCE_DATA_BYTES_H_
#define SIMPLEMAPREDUCE_DATA_BYTES_H_

#include <string>
#include <utility>
#include <vector>

namespace mapreduce {
namespace data {

class ByteData
{
 public:
  ByteData() {}
  explicit ByteData(int data);
  explicit ByteData(long data);
  explicit ByteData(float data);
  explicit ByteData(double data);
  ByteData(const std::string &data);

  ByteData(const ByteData&);
  ByteData &operator=(const ByteData&);
  ByteData(ByteData&&);
  ByteData &operator=(ByteData&&);

  bool operator==(const ByteData &rhs) const { return data_ == rhs.data_; }
  bool operator<(const ByteData &rhs) const { return data_ < rhs.data_; }
  bool operator>(const ByteData &rhs) const { return data_ > rhs.data_; }

  void set_data(int data);
  void set_data(long data);
  void set_data(float data);
  void set_data(double data);
  void set_data(std::string data);

  /**
   * Set array data.
   *  (e.g. vector)
   * 
   *  Example:
   *    std::vector<int> arr{1, 2, 3};
   *    ByteData bdata;
   *    bdata.set_data(arr.data(), arr.size());
   *
   *  @param *data  pointer to the array
   *  @param size   size of the array in the data type
   */
  void set_data(int *data, const size_t &size);
  void set_data(long *data, const size_t &size);
  void set_data(float *data, const size_t &size);
  void set_data(double *data, const size_t &size);

  template <typename T>
  void set_bytes(char *data, const size_t &size);

  template <typename T>
  T get_data() const;

  const char* get_byte() { return data_.data(); }

  /** Whether the data is empty */
  bool empty() { return data_.empty(); }

  /**
   * Append new value to the container as array.
   * The data can be retrieved as vector.
   *
   *  @param data&  new data to append back
   */
  template <typename T>
  void push_back(T &data);

 private:
  /**
   * Helper function to set data
   */
  template <typename T>
  void set_data_(T &data);

  /**
   * Helper function to set data
   * 
   *  @param *data  pointer to array
   *  @param size   length of data in given data type
   */
  template <typename T>
  void set_data_(T *data, const size_t &size);

  /**
   * Helper function to get data
   */
  template <typename T>
  T get_data_() const;

  /**
   * Helper function to append byte data
   */
  template <typename T>
  void push_back_(T &data);

  std::vector<char> data_;

  /// Length of values in the original data type
  /// If the data size is unknown, set as 0.
  size_t size_{0};
};

typedef std::pair<ByteData, ByteData> BytePair;

} // namespace data
} // namespace mapreduce

#include "simplemapreduce/data/bytes.tcc"

#endif