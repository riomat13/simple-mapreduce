#ifndef SIMPLEMAPREDUCE_PROC_SORTER_H_
#define SIMPLEMAPREDUCE_PROC_SORTER_H_

#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/ops/conf.h"

namespace fs = std::filesystem;

namespace mapreduce {
namespace proc {

  /**
   * Helper class to load data from intermediate state files
   */
  template <typename K, typename V>
  class FileLoader
  {
   public:
    FileLoader(const std::string &path);
    FileLoader(const fs::path &path);
    ~FileLoader() { fin_.close(); };

    /// Copy is not allowed
    FileLoader(const FileLoader &) = delete;
    FileLoader &operator=(const FileLoader &) = delete;

    FileLoader(FileLoader &&);
    FileLoader &operator=(FileLoader &&) = delete;

    /**
     * Return key value pair item until read all data from a file.
     * Return nullptr when reached eof.
     */
    std::pair<K, V> get_item();

    /**
     * Get path this loader is opening.
     */
    fs::path &get_path() { return fpath_; }
   
   private:
    /// File read stream
    std::ifstream fin_;

    /// File path being read
    fs::path fpath_;

  };

  /**
   * Sort operating class
   */
  template <typename K, typename V>
  class Sorter
  {
   public:
    /**
     * Constructor of Sorter class.
     * This is grouping items by keywords and pass to reducer.
     * 
     * @param dirpath&  directory pass which stores target files to process
     * @param conf&     configuration of the job executing this sorter
     */
    Sorter(const std::string &dirpath, const JobConf &conf);
    Sorter(const fs::path &dirpath, const JobConf &conf);

    /**
     * Execute sorting.
     * Each execution handles each file groped ID.
     * This will create a new map with values grouped by the given key.
     * 
     *  @return map&    map of vectors grouped by sorting process
     */
    std::map<K, std::vector<V>> run();

   private:
    /// File data loader
    std::vector<std::vector<FileLoader<K, V>>> loader_groups_;

    /// Configuration
    const JobConf &conf_;
  };

} // namespace proc
} // namespace mapreduce

#include "simplemapreduce/proc/sorter.tcc"

#endif