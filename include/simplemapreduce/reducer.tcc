namespace mapreduce {

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run(const fs::path &outpath)
{
  /// Clean up the target output directory
  auto sorter = this->get_sorter();

  /// Grouping by the keys and store in map<key_type, vector<value_type>>
  std::map<IK, std::vector<IV>> container = sorter->run();

  auto context = this->get_context(outpath);

  for (const auto [key, values] : container)
    reduce(key, values, *(context));
}

} // namespace mapreduce