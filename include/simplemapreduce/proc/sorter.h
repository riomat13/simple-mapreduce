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

    /**
     * Helper function for get_item().
     * This will read an one item pair (key, value),
     * and assign to the references respectively.
     * If reaches eof and no more data to read,
     * return without modification.
     * 
     *  @param K& key
     *  @param V& value
     */
     void get_item_(K &, V &);

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
    typedef std::map<K, std::vector<V>> kvmap;

    /**
     * Constructor of Sorter class.
     * This is grouping items by keywords and pass to reducer.
     * 
     * @param conf&     configuration of the job executing this sorter
     */
    Sorter(const JobConf &conf);

    /**
     * Execute sorting.
     * Each execution handles each file groped ID.
     * This will create a new map with values grouped by the given key.
     * 
     *  @return map<K, std::vector<V>>&  map of vectors grouped by sorting process
     */
    kvmap run();

   private:

    /**
     * Helper function for run().
     * This will store data to a map.
     * The map will be returned to the caller by the run() function.
     * 
     *  @return map<K, std::vector<V>>&  target map to store data
     */
    kvmap run_();

    /// File data loader
    std::vector<std::vector<FileLoader<K, V>>> loader_groups_;

    /// Configuration set by Job
    const JobConf &conf_;
  };

} // namespace proc
} // namespace mapreduce

#include "simplemapreduce/proc/sorter.tcc"

#endif