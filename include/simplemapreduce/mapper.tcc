namespace mapreduce {

template <typename IK, typename IV, typename OK, typename OV>
void Mapper<IK, IV, OK, OV>::run(ByteData &key, ByteData &value)
{
  this->map(key.get_data<IK>(), value.get_data<IV>(), *(this->get_context()));
}

} // namespace mapreduce