#ifndef SIMPLEMAPREDUCE_DATA_BYTES_H_
#define SIMPLEMAPREDUCE_DATA_BYTES_H_

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "simplemapreduce/data/type.h"

namespace mapreduce {
namespace data {

class ByteData {
 public:
  ByteData() {}
  explicit ByteData(mapreduce::type::Int16);
  explicit ByteData(mapreduce::type::Int);
  explicit ByteData(mapreduce::type::Long);
  explicit ByteData(mapreduce::type::Float);
  explicit ByteData(mapreduce::type::Double);
  /// Only reference is accepted to avoid copy
  ByteData(mapreduce::type::String&&);

  template <typename T1, typename T2>
  ByteData(mapreduce::type::CompositeKey<T1, T2>&&);

  ByteData(const ByteData&);
  ByteData &operator=(const ByteData&);
  ByteData(ByteData&&);
  ByteData &operator=(ByteData&&);

  bool operator==(const ByteData& rhs) const;
  bool operator!=(const ByteData& rhs) const;
  bool operator<(const ByteData& rhs) const;
  bool operator>(const ByteData& rhs) const;

  /**
   * Set data.
   *
   *  @param data   input data
   *    valid types: Int16, Int(Int32), Long(Int64), Float, Double, string
   */
  void set_data(mapreduce::type::Int16) noexcept;
  void set_data(mapreduce::type::Int) noexcept;
  void set_data(mapreduce::type::Long) noexcept;
  void set_data(mapreduce::type::Float) noexcept;
  void set_data(mapreduce::type::Double) noexcept;
  /// Only reference is accepted to avoid copy
  void set_data(mapreduce::type::String&&) noexcept;

  template <typename T1, typename T2>
  void set_data(mapreduce::type::CompositeKey<T1, T2>&&) noexcept;

  /**
   * Set array data.
   *  (e.g. vector)
   * 
   *  Example:
   *    std::vector<Int> arr{1, 2, 3};
   *    ByteData bdata;
   *    bdata.set_data(arr.data(), arr.size());
   *
   *  @param data   pointer to a target array
   *  @param size   size of the array in the data type
   */
  void set_data(mapreduce::type::Int16*, const size_t&);
  void set_data(mapreduce::type::Int*, const size_t&);
  void set_data(mapreduce::type::Long*, const size_t&);
  void set_data(mapreduce::type::Float*, const size_t&);
  void set_data(mapreduce::type::Double*, const size_t&);

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

  /**
   * Get primary key data from byte data.
   * This is used for shuffling process with hashing by the key
   * and will not directly convert numeric value to the string
   * so that it cannot be used for converting value to string.
   */
  mapreduce::type::String get_key() const;

  template <typename T1, typename T2>
  mapreduce::type::CompositeKey<T1, T2> get_pair() const;

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
  /** Helper function to set data. */
  template <typename T>
  inline void set_data_(T& data);

  /**
   * Helper function to set data.
   * 
   *  @param data   pointer to array
   *  @param size   length of data in given data type
   */
  template <typename T>
  inline void set_data_(T* data, const size_t& size);

  /** Helper function to get data. */
  template <typename T>
  inline T get_data_() const;

  /**
   * Helper function to get data with offset.
   * This will read data from `begin() + offset` to `end()`.
   *
   *  @param offset  offset size from begin()
   */
  template <typename T>
  inline T get_data_(size_t) const;

  /**
   * Helper function to get data with range.
   * This will read data from `begin() + start` to `begin() + end()`.
   * This is used for reading string data.
   *
   *  @param start  start index to read data
   *  @param end    end index to read data exclusive
   */
  template <typename T>
  inline T get_data_(size_t, size_t) const;

  template <typename T1, typename T2>
  inline mapreduce::type::CompositeKey<T1, T2> get_pair_() const;

  /** Helper function to append byte data. */
  template <typename T>
  inline void push_back_(T& data);

  std::vector<char> data_;

  /// Length of values in the original data type.
  size_t size_{0};
};

using BytePair =  std::pair<ByteData, ByteData>;

}  // namespace data
}  // namespace mapreduce

#include "simplemapreduce/data/bytes-inl.h"

#endif  // SIMPLEMAPREDUCE_DATA_BYTES_H_