#include "simplemapreduce/util/log.h"

using namespace mapreduce::util;

namespace mapreduce {
namespace proc {

  /// Convert string to path and store as member variable
  template <typename K, typename V>
  FileLoader<K, V>::FileLoader(const std::string &path)
      : fpath_(fs::path(std::move(path)))
  {
    fin_.open(path, std::ios::binary);
  };

  template <typename K, typename V>
  FileLoader<K, V>::FileLoader(const fs::path &path)
      : fpath_(std::move(path))
  {
    fin_.open(path, std::ios::binary);
  };

  /// Move constructor for passing to vector
  template <typename K, typename V>
  FileLoader<K, V>::FileLoader(FileLoader &&rhs)
  {
    this->fin_ = std::move(rhs.fin_);
    this->fpath_ = std::move(rhs.fpath_);
  }

  template <typename K, typename V>
  void FileLoader<K, V>::get_item_(K &key, V &value)
  {
    /// Load key data size
    size_t key_size;
    fin_.read(reinterpret_cast<char*>(&key_size), sizeof(size_t));

    if (fin_.eof())
    {
      return;
    }

    /// Load key data
    char keydata[key_size];
    fin_.read(keydata, sizeof(char) * key_size);
    key = std::string(keydata, key_size);

    /// Load value data
    fin_.read(reinterpret_cast<char *>(&value), sizeof(V));
  }

  template <typename K, typename V>
  Sorter<K, V>::Sorter(const fs::path &dirpath, const JobConf &conf)
      : conf_(conf)
  {
    /// Initialize file loader container
    loader_groups_ = std::vector<std::vector<FileLoader<K, V>>>(
      (conf.n_groups + conf.worker_size - conf.worker_rank - 1) / conf.worker_size
    );

    /// Parse directory to find files to be processed by this node
    for (auto &path: fs::directory_iterator(dirpath))
    {
      /// Safe-guard. This should not be captured.
      if (!path.is_regular_file())
        continue;

      /// Get file id from file name
      std::string fname = path.path().filename().string();

      size_t idx = fname.find_last_of("-");
      
      /// Continue if a given file name is not valid format.
      if (idx > fname.length())
        continue;

      /// Get file id from file name "{worker_id}-{file_id}"
      int file_id = std::stoi(fname.substr(idx+1));

      /// Find an index which the file will be assigned to store
      int loader_index = file_id / conf.worker_size;

      if ((file_id % conf.worker_size) == conf.worker_rank)
        loader_groups_[loader_index].emplace_back(path.path().string());
    }
  }

  template <typename K, typename V>
  Sorter<K, V>::Sorter(const std::string &dirpath, const JobConf &conf)
  {
    Sorter(fs::path(std::move(dirpath)), conf);
  }

  template <typename K, typename V>
  std::map<K, std::vector<V>> Sorter<K, V>::run_()
  {
    kvmap container;
    for (auto &loaders: loader_groups_)
    {
      for (auto &loader: loaders)
      {
        auto data = loader.get_item();

        /// store values to vector of the associated key in map
        while (!data.first.empty())
        {
          container[data.first].push_back(data.second);
          data = loader.get_item();
        }
      }
    }
    return container;
  }

} // namespace proc
} // namespace mapreduce