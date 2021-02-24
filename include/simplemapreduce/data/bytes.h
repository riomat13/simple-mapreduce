#ifndef SIMPLEMAPREDUCE_DATA_BYTES_H_
#define SIMPLEMAPREDUCE_DATA_BYTES_H_

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

namespace mapreduce {
namespace data {

class ByteData {
 public:
  ByteData() {}
  explicit ByteData(int);
  explicit ByteData(long);
  explicit ByteData(float);
  explicit ByteData(double);
  /// Only reference is accepted to avoid copy
  ByteData(std::string&&);

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
  void set_data(int) noexcept;
  void set_data(long) noexcept;
  void set_data(float) noexcept;
  void set_data(double) noexcept;
  /// Only reference is accepted to avoid copy
  void set_data(std::string&&) noexcept;

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
   * Read and set data from a file.
   * All data will be assumed as char type.
   *
   *  @param path   read data from a file
   */
  void read_file(const std::string&);
  void read_file(const std::filesystem::path&);

  /**
   * Get data as given type.
   */
  template <typename T>
  T get_data() const;

  /** Get data as bytes in char array. */
  const char* get_byte() { return data_.data(); }

  /** Get a length of the data. */
  size_t size() const { return size_; }
  /** Get a size of the byte array. */
  size_t bsize() const { return data_.size(); }

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

  /// Length of values in the original data type.
  /// If the data is single primitive type, the size is set as 0.
  size_t size_{0};
};

using BytePair =  std::pair<ByteData, ByteData>;

}  // namespace data
}  // namespace mapreduce

#include "simplemapreduce/data/bytes-inl.h"

#endif  // SIMPLEMAPREDUCE_DATA_BYTES_H_