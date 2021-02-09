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

    unsigned int idx = fname.find_last_of("-");

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
BytePair BinaryFileDataLoader<K, V>::get_item()
{
  /// TODO: update to be generic
  size_t key_size;

  while (true)
  {
    /// Load key data size
    fin_.read(reinterpret_cast<char*>(&key_size), sizeof(size_t));

    if (fin_.eof())
    {
      fin_.close();
      if (fpaths_.empty())
        return std::make_pair(ByteData(), ByteData());

      fin_.open(fpaths_.back(), std::ios::binary);
      fpaths_.pop_back();
      continue;
    }

    break;
  }

  /// Load key data
  char keydata[key_size];
  fin_.read(keydata, sizeof(char) * key_size);
  ByteData key;
  key.set_bytes<K>(&keydata[0], key_size);

  /// Load value data
  V valuedata;
  fin_.read(reinterpret_cast<char *>(&valuedata), sizeof(V));
  ByteData value(valuedata);

  return std::make_pair(std::move(key), std::move(value));
}

} // namespace proc
} // namespace mapreduce