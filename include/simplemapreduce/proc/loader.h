#ifndef SIMPLEMAPREDUCE_PROC_LOADER_H_
#define SIMPLEMAPREDUCE_PROC_LOADER_H_

#include <filesystem>
#include <fstream>
#include <utility>
#include <vector>

#include "simplemapreduce/ops/conf.h"

namespace fs = std::filesystem;

using namespace mapreduce;

namespace mapreduce {
namespace proc {

  template <typename K, typename V>
  class DataLoader
  {
   public:
    /**
     * Return key-value pair item until read all data from a file.
     * Return nullptr when reached eof.
     */
    virtual std::pair<K, V> get_item() = 0;
  };

  /**
   * Helper class to load data from intermediate state files
   */
  template <typename K, typename V>
  class BinaryFileDataLoader : public DataLoader<K, V>
  {
   public:

    BinaryFileDataLoader(const JobConf &conf);
    ~BinaryFileDataLoader()
    {
      if (fin_.is_open())
        fin_.close();
    }

    /// Copy is not allowed
    BinaryFileDataLoader(const BinaryFileDataLoader &) = delete;
    BinaryFileDataLoader &operator=(const BinaryFileDataLoader &) = delete;

    BinaryFileDataLoader(BinaryFileDataLoader &&) = delete;
    BinaryFileDataLoader &operator=(BinaryFileDataLoader &&) = delete;

    /**
     * Return key-value pair item until read all data from a file.
     * Return nullptr when reached eof.
     */
    std::pair<K, V> get_item();
   
   private:

    /**
     * Read key-value data from files.
     */
    void extract_target_files();

    /// Intermediate file directory
    std::vector<fs::path> fpaths_;

    std::ifstream fin_;

    /// Job configuration
    const JobConf &conf_;
  };

} // namespace proc
} // namespace mapreduce

#include "simplemapreduce/proc/loader.tcc"

#endif