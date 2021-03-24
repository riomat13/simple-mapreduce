namespace mapreduce {

template <typename IK, typename IV, typename OK, typename OV>
std::filesystem::path Reducer<IK, IV, OK, OV>::get_output_filepath() {
  /// Set file path named by current worker rank and join with the output directory
  std::ostringstream oss;
  oss << std::setw(5) << std::setfill('0') << conf_->worker_rank;
  std::filesystem::path fname = oss.str();

  return conf_->output_dirpath / fname;
}

template <typename IK, typename IV, typename OK, typename OV>
std::unique_ptr<mapreduce::Context<OK, OV>> Reducer<IK, IV, OK, OV>::get_context(const std::string& path) {
  std::unique_ptr<mapreduce::proc::OutputWriter<OK, OV>> writer = std::make_unique<mapreduce::proc::OutputWriter<OK, OV>>(path);
  return std::make_unique<mapreduce::Context<OK, OV>>(std::move(writer));
}

template <typename IK, typename IV, typename OK, typename OV>
std::unique_ptr<mapreduce::Context<OK, OV>> Reducer<IK, IV, OK, OV>::get_context(std::shared_ptr<mapreduce::data::MessageQueue> mq) {
  std::unique_ptr<mapreduce::proc::MQWriter> writer = std::make_unique<mapreduce::proc::MQWriter>(mq);
  return std::make_unique<mapreduce::Context<OK, OV>>(std::move(writer));
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
void Reducer<IK, IV, OK, OV>::set_shuffle(std::unique_ptr<mapreduce::proc::ShuffleTask> shuffle) {
  shuffle_ = std::unique_ptr<mapreduce::proc::Shuffle<IK, IV>>(static_cast<mapreduce::proc::Shuffle<IK, IV>*>(shuffle.get()));
}

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run() {
  if (is_combiner_)
    this->run_();
  else
    this->run_(get_output_filepath());
}

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run_() {
  /// Grouping data by the keys from mapped data
  auto sorter = this->get_sorter(mq_);
  auto container = sorter->run();

  auto context = this->get_context(mq_);

  for (const auto& [key, values] : *container)
    reduce(key, values, *context);
  mq_->end();
}

template <typename IK, typename IV, typename OK, typename OV>
void Reducer<IK, IV, OK, OV>::run_(const std::filesystem::path& outpath) {
  /// Grouping data by the keys from shuffled data
  auto sorter = this->get_sorter();

  std::map<IK, std::vector<IV>> container_;
  auto data = mq_->receive();
  while (!data.first.empty()) {
    container_[data.first.get_data<IK>()].push_back(data.second.get_data<IV>());
    data = mq_->receive();
  }
  sorter->set_container(std::make_unique<std::map<IK, std::vector<IV>>>(container_));
  auto container = sorter->run();

  auto context = this->get_context(outpath);

  for (const auto& [key, values] : *container)
    reduce(key, values, *context);
}

}  // namespace mapreduce