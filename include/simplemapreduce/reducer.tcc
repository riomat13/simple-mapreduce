namespace mapreduce {

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run()
{
  if (mq_ == nullptr)
    this->run_(this->conf_->output_fpath);
  else
    this->run_(mq_);
}

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run_(std::shared_ptr<mapreduce::data::MessageQueue> mq)
{
  /// Grouping data by the keys from mapped data
  auto sorter = this->get_sorter(mq);
  std::map<IK, std::vector<IV>> container = sorter->run();

  auto context = this->get_context(mq);

  for (const auto [key, values] : container)
    reduce(key, values, *(context));
  mq->end();
}

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run_(const std::filesystem::path& outpath)
{
  /// Grouping data by the keys from shuffled data
  auto sorter = this->get_sorter();
  std::map<IK, std::vector<IV>> container = sorter->run();

  auto context = this->get_context(outpath);

  for (const auto [key, values] : container)
    reduce(key, values, *(context));
}

}  // namespace mapreduce