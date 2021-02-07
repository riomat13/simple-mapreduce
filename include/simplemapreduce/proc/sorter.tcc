namespace mapreduce {
namespace proc {

  template <typename K, typename V>
  FileDataLoader<K, V>::FileDataLoader(const JobConf &conf) : conf_(conf)
  {
    extract_target_files();
    fin_.open(fpaths_.back(), std::ios::binary);
    fpaths_.pop_back();
  };

  template <typename K, typename V>
  void FileDataLoader<K, V>::extract_target_files()
  {
    for (auto &p: fs::directory_iterator(conf_.tmpdir))
    {
      if (!p.is_regular_file())
        continue;

      /// Get file id from file name
      std::string fname = p.path().string();

      size_t idx = fname.find_last_of("-");
      
      /// Continue if a given file name is not valid format.
      if (idx > fname.length())
        continue;

      /// Get file id from file name "{worker_id}-{file_id}"
      int file_id = std::stoi(fname.substr(idx+1));

      if ((file_id % conf_.worker_size) == conf_.worker_rank)
        fpaths_.push_back(std::move(p.path()));
    }
  }

  template <typename K, typename V>
  std::pair<K, V> FileDataLoader<K, V>::get_item()
  {
    size_t key_size;
    K key;
    V value;

    while (true)
    {
      /// Load key data size
      fin_.read(reinterpret_cast<char*>(&key_size), sizeof(size_t));

      if (fin_.eof())
      {
        fin_.close();
        if (fpaths_.empty())
          return std::make_pair(key, value);

        fin_.open(fpaths_.back(), std::ios::binary);
        fpaths_.pop_back();
        continue;
      }

      break;
    }

    /// Load key data
    char keydata[key_size];
    fin_.read(keydata, sizeof(char) * key_size);
    key = std::string(keydata, key_size);

    /// Load value data
    fin_.read(reinterpret_cast<char *>(&value), sizeof(V));

    return std::make_pair(std::move(key), std::move(value));
  }

  template <typename K, typename V>
  Sorter<K, V>::Sorter(const JobConf &conf)
      : conf_(conf)
  {
    loader_ = std::make_unique<FileDataLoader<K, V>>(conf_);
  }

  template <typename K, typename V>
  std::map<K, std::vector<V>> Sorter<K, V>::run_()
  {
    kvmap container;
    auto data = loader_->get_item();

    /// store values to vector of the associated key in map
    while (!data.first.empty())
    {
      container[data.first].push_back(data.second);
      data = loader_->get_item();
    }

    return container;
  }

} // namespace proc
} // namespace mapreduce