namespace mapreduce {
namespace proc {

template <typename K, typename V>
BinaryFileDataLoader<K, V>::BinaryFileDataLoader(std::shared_ptr<mapreduce::JobConf> conf) : conf_(conf) {
  extract_target_files();
  fin_.open(fpaths_.back(), std::ios::binary);
  fpaths_.pop_back();
};

template <typename K, typename V>
void BinaryFileDataLoader<K, V>::extract_target_files() {
  for (auto& p: std::filesystem::directory_iterator(conf_->tmpdir)) {
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

    if ((file_id % conf_->worker_size) == conf_->worker_rank)
      fpaths_.push_back(std::move(p.path()));
  }
}

template <typename K, typename V>
mapreduce::data::BytePair BinaryFileDataLoader<K, V>::get_item() {
  /// Load key data
  mapreduce::data::ByteData key;
  while (true) {
    key = load_byte_data<K>(fin_);
    if (fin_.eof()) {
      fin_.close();

      /// Return empty data once all data is extracted
      if (fpaths_.empty())
        return std::make_pair(std::move(key), mapreduce::data::ByteData());

      fin_.open(fpaths_.back(), std::ios::binary);
      fpaths_.pop_back();
      continue;
    }
    break;
  }
  return std::make_pair(std::move(key), std::move(load_byte_data<V>(fin_)));
}

}  // namespace proc
}  // namespace mapreduce