namespace mapreduce {

  template <typename IK, typename IV, typename OK, typename OV>
  std::unique_ptr<Context> Mapper<IK, IV, OK, OV>::create_context()
  {
    std::unique_ptr<MQWriter<OK, OV>> writer = std::make_unique<MQWriter<OK, OV>>(mq_);
    return std::make_unique<Context>(std::move(Context(std::move(writer))));
  } 

  template <typename IK, typename IV, typename OK, typename OV>
  std::unique_ptr<Shuffle<OK, OV>> Mapper<IK, IV, OK, OV>::create_shuffler(fs::path &tmpdir, JobConf &conf)
  {
    return std::make_unique<Shuffle<OK, OV>>(mq_, tmpdir, conf);
  } 

  template <typename IK, typename IV, typename OK, typename OV>
  std::unique_ptr<Shuffle<OK, OV>> Mapper<IK, IV, OK, OV>::create_shuffler(std::string &tmpdir, JobConf &conf)
  {
    fs::path tmpdir_{tmpdir};
    return create_shuffler(tmpdir_, conf);
  } 

  template <typename IK, typename IV, typename OK, typename OV>
  std::unique_ptr<Sorter<OK, OV>> Mapper<IK, IV, OK, OV>::create_sorter(fs::path &tmpdir, JobConf &conf)
  {
    return std::make_unique<Sorter<OK, OV>>(tmpdir, conf);
  }

  template <typename IK, typename IV, typename OK, typename OV>
  std::unique_ptr<Sorter<OK, OV>> Mapper<IK, IV, OK, OV>::create_sorter(std::string &tmpdir, JobConf &conf)
  {
    fs::path tmpdir_{tmpdir};
    return create_sorter(tmpdir_, conf);
  }

} // namespace mapreduce