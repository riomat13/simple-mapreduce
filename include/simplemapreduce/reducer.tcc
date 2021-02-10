namespace mapreduce {

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run(std::map<IK, std::vector<IV>> &container, const fs::path &outpath)
{
  auto context = this->get_context(outpath);

  for (auto it = container.begin(); it != container.end(); ++it)
    reduce(it->first, it->second, *(context));
}

} // namespace mapreduce