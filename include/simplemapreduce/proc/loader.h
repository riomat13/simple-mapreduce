#ifndef SIMPLEMAPREDUCE_PROC_LOADER_H_
#define SIMPLEMAPREDUCE_PROC_LOADER_H_

#include <fstream>
#include <memory>
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
template <typename T>
mapreduce::data::ByteData load_byte_data(std::ifstream&);

class DataLoader
{
 public:
  virtual ~DataLoader() {}

  /**
   * Return key-value pair item until read all data from a file.
   * Return nullptr when reached eof.
   */
  virtual mapreduce::data::BytePair get_item() = 0;
};

/**
 * Helper class to load data from intermediate state files
 */
template <typename K, typename V>
class BinaryFileDataLoader : public DataLoader
{
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

class MQDataLoader : public DataLoader
{
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

#include "simplemapreduce/proc/loader.tcc"

#endif  // SIMPLEMAPREDUCE_PROC_LOADER_H_