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
  explicit ByteData(int);
  explicit ByteData(long);
  explicit ByteData(float);
  explicit ByteData(double);
  ByteData(const std::string&);

  ByteData(const ByteData&);
  ByteData &operator=(const ByteData&);
  ByteData(ByteData&&);
  ByteData &operator=(ByteData&&);

  bool operator==(const ByteData& rhs) const { return data_ == rhs.data_; }
  bool operator<(const ByteData& rhs) const { return data_ < rhs.data_; }
  bool operator>(const ByteData& rhs) const { return data_ > rhs.data_; }

  /**
   * Set data.
   *
   *  @param data   input data (int, long, float, double, string)
   */
  void set_data(int);
  void set_data(long);
  void set_data(float);
  void set_data(double);
  void set_data(std::string);

  /**
   * Set array data.
   *  (e.g. vector)
   * 
   *  Example:
   *    std::vector<int> arr{1, 2, 3};
   *    ByteData bdata;
   *    bdata.set_data(arr.data(), arr.size());
   *
   *  @param data   pointer to a target array
   *  @param size   size of the array in the data type
   */
  void set_data(int*, const size_t&);
  void set_data(long*, const size_t&);
  void set_data(float*, const size_t&);
  void set_data(double*, const size_t&);

  /**
   * Set data from bytes.
   *
   *  @param data   pointer to a bytes array
   *  @param size   size of the array
   */
  template <typename T>
  void set_bytes(char*, const size_t&);

  /**
   * Get data as given type.
   */
  template <typename T>
  T get_data() const;

  /** Get data as bytes in char array. */
  const char* get_byte() { return data_.data(); }

  /** Check if data is empty. */
  bool empty() { return data_.empty(); }

  /**
   * Append new value to the container as array.
   * The data can be retrieved as vector.
   *
   *  @param data   new data to append back
   */
  template <typename T>
  void push_back(T& data);

 private:
  /** Helper function to set data */
  template <typename T>
  inline void set_data_(T& data);

  /**
   * Helper function to set data
   * 
   *  @param data   pointer to array
   *  @param size   length of data in given data type
   */
  template <typename T>
  inline void set_data_(T* data, const size_t& size);

  /** Helper function to get data */
  template <typename T>
  inline T get_data_() const;

  /** Helper function to append byte data */
  template <typename T>
  inline void push_back_(T& data);

  std::vector<char> data_;

  /// Length of values in the original data type
  /// If the data size is unknown, set as 0.
  size_t size_{0};
};

typedef std::pair<ByteData, ByteData> BytePair;

}  // namespace data
}  // namespace mapreduce

#include "simplemapreduce/data/bytes.tcc"

#endif  // SIMPLEMAPREDUCE_DATA_BYTES_H_