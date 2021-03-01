namespace mapreduce {

template <typename IK, typename IV, typename OK, typename OV>
std::unique_ptr<mapreduce::Context<OK, OV>> Mapper<IK, IV, OK, OV>::get_context() {
  std::unique_ptr<mapreduce::proc::MQWriter> writer = std::make_unique<mapreduce::proc::MQWriter>(get_mq());
  return std::make_unique<mapreduce::Context<OK, OV>>(std::move(writer));
}

template <typename IK, typename IV, typename OK, typename OV>
std::unique_ptr<mapreduce::proc::ShuffleTask> Mapper<IK, IV, OK, OV>::get_shuffle() {
  std::unique_ptr<mapreduce::proc::ShuffleTask> shuffle = std::make_unique<mapreduce::proc::Shuffle<OK, OV>>(get_mq(), conf_);
  return shuffle;
};

template <typename IK, typename IV, typename OK, typename OV>
void Mapper<IK, IV, OK, OV>::run(mapreduce::data::ByteData& key, mapreduce::data::ByteData& value) {
  this->map(key.get_data<IK>(), value.get_data<IV>(), *(this->get_context()));
}

} // namespace mapreduce