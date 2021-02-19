namespace mapreduce {

template <typename IK, typename IV, typename OK, typename OV>
std::unique_ptr<mapreduce::Context<IK, IV>> Reducer<IK, IV, OK, OV>::get_context(const std::string& path) {
  std::unique_ptr<mapreduce::proc::OutputWriter<IK, IV>> writer = std::make_unique<mapreduce::proc::OutputWriter<IK, IV>>(path);
  return std::make_unique<mapreduce::Context<IK, IV>>(std::move(writer));
}

template <typename IK, typename IV, typename OK, typename OV>
std::unique_ptr<mapreduce::Context<IK, IV>> Reducer<IK, IV, OK, OV>::get_context(std::shared_ptr<mapreduce::data::MessageQueue> mq) {
  std::unique_ptr<mapreduce::proc::MQWriter> writer = std::make_unique<mapreduce::proc::MQWriter>(mq);
  return std::make_unique<mapreduce::Context<IK, IV>>(std::move(writer));
}

template <typename IK, typename IV, typename OK, typename OV>
std::unique_ptr<mapreduce::proc::Sorter<IK, IV>> Reducer<IK, IV, OK, OV>::get_sorter() {
  std::unique_ptr<mapreduce::proc::DataLoader> loader = std::make_unique<mapreduce::proc::BinaryFileDataLoader<IK, IV>>(this->conf_);
  return std::make_unique<mapreduce::proc::Sorter<IK, IV>>(std::move(loader));
}

template <typename IK, typename IV, typename OK, typename OV>
std::unique_ptr<mapreduce::proc::Sorter<IK, IV>> Reducer<IK, IV, OK, OV>::get_sorter(std::shared_ptr<mapreduce::data::MessageQueue> mq) {
  std::unique_ptr<mapreduce::proc::DataLoader> loader = std::make_unique<mapreduce::proc::MQDataLoader>(mq);
  return std::make_unique<mapreduce::proc::Sorter<IK, IV>>(std::move(loader));
}

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run() {
  if (mq_ == nullptr)
    this->run_(this->conf_->output_fpath);
  else
    this->run_(mq_);
}

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run_(std::shared_ptr<mapreduce::data::MessageQueue> mq) {
  /// Grouping data by the keys from mapped data
  auto sorter = this->get_sorter(mq);
  std::map<IK, std::vector<IV>> container = sorter->run();

  auto context = this->get_context(mq);

  for (const auto [key, values] : container)
    reduce(key, values, *(context));
  mq->end();
}

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run_(const std::filesystem::path& outpath) {
  /// Grouping data by the keys from shuffled data
  auto sorter = this->get_sorter();
  std::map<IK, std::vector<IV>> container = sorter->run();

  auto context = this->get_context(outpath);

  for (const auto [key, values] : container)
    reduce(key, values, *(context));
}

}  // namespace mapreduce