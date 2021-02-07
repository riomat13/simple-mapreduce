namespace mapreduce {
namespace proc {

  template <typename K, typename V>
  BinaryFileDataLoader<K, V>::BinaryFileDataLoader(const JobConf &conf) : conf_(conf)
  {
    extract_target_files();
    fin_.open(fpaths_.back(), std::ios::binary);
    fpaths_.pop_back();
  };

  template <typename K, typename V>
  void BinaryFileDataLoader<K, V>::extract_target_files()
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
  std::pair<K, V> BinaryFileDataLoader<K, V>::get_item()
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
  std::pair<K, V> MQDataLoader<K, V>::get_item()
  {
    /// Fetch data from MessageQueue
    /// A key of last element will be invalid. (e.g. empty for string)
    return mq_->receive();
  }

} // namespace proc
} // namespace mapreduce