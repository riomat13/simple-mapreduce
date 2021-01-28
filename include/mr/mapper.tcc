namespace mapreduce {

  template <typename K, typename V>
  std::unique_ptr<Context> Mapper<K, V>::create_context()
  {
    std::unique_ptr<MQWriter<K, V>> writer = std::make_unique<MQWriter<K, V>>(MQWriter<K, V>(mq_));
    return std::make_unique<Context>(std::move(Context(std::move(writer))));
  } 

  template <typename K, typename V>
  std::unique_ptr<Shuffle<K, V>> Mapper<K, V>::create_shuffler(fs::path &tmpdir, JobConf &conf)
  {
    return std::make_unique<Shuffle<K, V>>(mq_, tmpdir, conf);
  } 

  template <typename K, typename V>
  std::unique_ptr<Shuffle<K, V>> Mapper<K, V>::create_shuffler(std::string &tmpdir, JobConf &conf)
  {
    fs::path tmpdir_{tmpdir};
    return create_shuffler(tmpdir_, conf);
  } 

  template <typename K, typename V>
  std::unique_ptr<Sorter<K, V>> Mapper<K, V>::create_sorter(fs::path &tmpdir, JobConf &conf)
  {
    return std::make_unique<Sorter<K, V>>(tmpdir, conf);
  }

  template <typename K, typename V>
  std::unique_ptr<Sorter<K, V>> Mapper<K, V>::create_sorter(std::string &tmpdir, JobConf &conf)
  {
    fs::path tmpdir_{tmpdir};
    return create_sorter(tmpdir_, conf);
  }

} // namespace mapreduce