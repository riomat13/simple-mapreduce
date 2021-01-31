namespace mapreduce {

  template <typename IK, typename IV, typename OK, typename OV>
  std::unique_ptr<Context> Reducer<IK, IV, OK, OV>::create_context(const std::string &path)
  {
    std::unique_ptr<OutputWriter> writer = std::make_unique<OutputWriter>(std::move(path));
    return std::make_unique<Context>(std::move(Context(std::move(writer))));
  } 

} // namespace mapreduce