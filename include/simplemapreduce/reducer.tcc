namespace mapreduce {

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run(std::map<ByteData, std::vector<ByteData>> &container, const fs::path &outpath)
{
  auto context = this->get_context(outpath);

  for (auto it = container.begin(); it != container.end(); ++it)
  {
    /// Run reduce operation for each key
    /// TODO: find better conversion way
    std::vector<IV> values(it->second.size());

    for (unsigned int i = 0; i < it->second.size(); ++i)
      values[i] = it->second[i].get_data<IV>();

    reduce(it->first.get_data<IK>(), values, *(context));
  }
}

} // namespace mapreduce