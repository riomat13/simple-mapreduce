#ifndef SIMPLEMAPREDUCE_PROC_LOADER_H_
#define SIMPLEMAPREDUCE_PROC_LOADER_H_

#include <fstream>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/queue.h"
#include "simplemapreduce/ops/conf.h"

namespace mapreduce {
namespace proc {

/**
 * Load data from binary file.
 *
 *  @param fin  input binary file stream
 */
template <typename T, std::enable_if_t<std::is_arithmetic<T>::value, bool> = true>
mapreduce::data::ByteData load_byte_data(std::ifstream&);

template <typename T, std::enable_if_t<!std::is_arithmetic<T>::value, bool> = true>
mapreduce::data::ByteData load_byte_data(std::ifstream&);

/**
 * Base abstract class of data loader.
 * The data is pair of mapreduce::data::ByteData,
 * which represents key/value data.
 */
class DataLoader {
 public:
  virtual ~DataLoader() {}

  /**
   * Return key-value pair item until read all data from a file.
   * Return nullptr when reached eof.
   */
  virtual mapreduce::data::BytePair get_item() = 0;
};

/**
 * Helper class to load data from intermediate state files.
 * The data is pair of mapreduce::data::ByteData.
 */
template <typename K, typename V>
class BinaryFileDataLoader : public DataLoader {
 public:
  BinaryFileDataLoader(std::shared_ptr<mapreduce::JobConf>);
  ~BinaryFileDataLoader()
  {
    if (fin_.is_open())
      fin_.close();
  }

  /// Copy/Move are not allowed
  BinaryFileDataLoader(const BinaryFileDataLoader&) = delete;
  BinaryFileDataLoader &operator=(const BinaryFileDataLoader&) = delete;
  BinaryFileDataLoader(BinaryFileDataLoader&&) = delete;
  BinaryFileDataLoader &operator=(BinaryFileDataLoader&&) = delete;

  /**
   * Return key-value pair item until read all data from a file.
   * Once finished reading, return with invalid key.
   */
  mapreduce::data::BytePair get_item();
  
 private:
  /** Read key-value data from files. */
  void extract_target_files();

  /// Intermediate file directory
  std::vector<std::filesystem::path> fpaths_;

  std::ifstream fin_;

  /// Job configuration
  std::shared_ptr<mapreduce::JobConf> conf_;
};

/**
 * Data extractor from MessageQueue.
 * The data is pair of mapreduce::data::ByteData.
 */
class MQDataLoader : public DataLoader {
 public:
  MQDataLoader() {}
  MQDataLoader(std::shared_ptr<mapreduce::data::MessageQueue> mq) : mq_(mq) {}

  /// Copy/Move are not allowed
  MQDataLoader(const MQDataLoader&) = delete;
  MQDataLoader& operator=(const MQDataLoader&) = delete;
  MQDataLoader(MQDataLoader&&) = delete;
  MQDataLoader& operator=(MQDataLoader&&) = delete;

  /**
   * Return key-value pair item from MessageQueue.
   * Once fetched all data, return with empty key.
   */
  mapreduce::data::BytePair get_item();

 private:
  std::shared_ptr<mapreduce::data::MessageQueue> mq_;
};

}  // namespace proc
}  // namespace mapreduce

#include "simplemapreduce/proc/loader-inl.h"

#endif  // SIMPLEMAPREDUCE_PROC_LOADER_H_