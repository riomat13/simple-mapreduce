#ifndef SIMPLEMAPREDUCE_PROC_SORTER_H_
#define SIMPLEMAPREDUCE_PROC_SORTER_H_

#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
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
  class FileDataLoader
  {
   public:

    FileDataLoader(const JobConf &conf);
    // FileDataLoader(std::vector<fs::path> &path);
    ~FileDataLoader()
    {
      if (fin_.is_open())
        fin_.close();
    }

    /// Copy is not allowed
    FileDataLoader(const FileDataLoader &) = delete;
    FileDataLoader &operator=(const FileDataLoader &) = delete;

    FileDataLoader(FileDataLoader &&) = delete;
    FileDataLoader &operator=(FileDataLoader &&) = delete;

    /**
     * Return key value pair item until read all data from a file.
     * Return nullptr when reached eof.
     */
    std::pair<K, V> get_item();
   
   private:

    /**
     * Read key/value data from files.
     */
    void extract_target_files();

    /// Intermediate file directory
    std::vector<fs::path> fpaths_;

    std::ifstream fin_;

    /// Job configuration
    const JobConf &conf_;
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
    std::unique_ptr<FileDataLoader<K, V>> loader_ = nullptr;

    /// Configuration set by Job
    const JobConf &conf_;
  };

} // namespace proc
} // namespace mapreduce

#include "simplemapreduce/proc/sorter.tcc"

#endif