#include "proc/sorter.h"

#include "ops/job.h"

namespace mapreduce {
namespace proc {

  /* --------------------------------------------------
   *   FileLoader
   * -------------------------------------------------- */
  template<>
  std::pair<std::string, int> FileLoader<std::string, int>::get_item()
  {
    /// Load key data size
    size_t key_size;
    fin_.read(reinterpret_cast<char*>(&key_size), sizeof(size_t));

    if (fin_.eof())
    {
      /// Return once reached end
      return std::make_pair<std::string, int>("", 0);
    }

    /// Load key data
    char keydata[key_size];
    fin_.read(keydata, sizeof(char) * key_size);
    std::string key(keydata, key_size);

    /// Load value data
    int value;
    fin_.read(reinterpret_cast<char *>(&value), sizeof(int));

    return std::make_pair<std::string, int>(std::move(key), std::move(value));
  }

  template<>
  std::pair<std::string, long> FileLoader<std::string, long>::get_item()
  {
    /// Load key data size
    size_t key_size;
    fin_.read(reinterpret_cast<char*>(&key_size), sizeof(size_t));

    if (fin_.eof())
    {
      /// Return once reached end
      return std::make_pair<std::string, long>("", 0);
    }

    /// Load key data
    char keydata[key_size];
    fin_.read(keydata, sizeof(char) * key_size);
    std::string key(keydata, key_size);

    /// Load value data
    int value;
    fin_.read(reinterpret_cast<char *>(&value), sizeof(long));

    return std::make_pair<std::string, long>(std::move(key), std::move(value));
  }

  /* --------------------------------------------------
   *   Sorter
   * -------------------------------------------------- */
  // TODO read data from file
  // void Sorter<std::string, int>::run(std::map<std::string, std::vector<int>> &container)
  template<>
  std::map<std::string, std::vector<int>> Sorter<std::string, int>::run()
  {
    std::map<std::string, std::vector<int>> container;

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

  template<>
  std::map<std::string, std::vector<long>> Sorter<std::string, long>::run()
  {
    std::map<std::string, std::vector<long>> container;

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