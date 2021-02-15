#include "simplemapreduce/proc/writer.h"

#include <iomanip>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

#include "simplemapreduce/data/bytes.h"

using lim_float = std::numeric_limits<float>;
using lim_double = std::numeric_limits<double>;

using namespace mapreduce::data;

namespace mapreduce {
namespace proc {

template<>
void write_binary(std::ofstream& fout, ByteData&& bdata)
{
  if (bdata.size() > 0)
  {
    size_t data_size = bdata.size();
    fout.write(reinterpret_cast<char*>(&data_size), sizeof(size_t));
  }
  fout.write(bdata.get_byte(), bdata.bsize());
}

template<>
void write_binary(std::ofstream& fout, const std::string& data)
{
  /// Store string size and the chars
  size_t data_size = data.size();
  fout.write(reinterpret_cast<char*>(&data_size), sizeof(size_t));
  fout.write(data.c_str(), data_size);
}

template<>
void write_binary(std::ofstream& fout, int&& data)
{
  fout.write(reinterpret_cast<char*>(const_cast<int*>(&data)), sizeof(int));
}

template<>
void write_binary(std::ofstream& fout, long&& data)
{
  fout.write(reinterpret_cast<char*>(const_cast<long*>(&data)), sizeof(long));
}

template<>
void write_binary(std::ofstream& fout, float&& data)
{
  fout.write(reinterpret_cast<char*>(const_cast<float*>(&data)), sizeof(float));
}

template<>
void write_binary(std::ofstream& fout, double&& data)
{
  fout.write(reinterpret_cast<char*>(const_cast<double*>(&data)), sizeof(double));
}

template<>
void write_output(std::ofstream& fout, const std::string& data)
{
  fout << std::left << std::setw(10) << data;
}

template<>
void write_output(std::ofstream& fout, const int& data)
{
  fout << std::right << std::setw(6) << data;
}

template<>
void write_output(std::ofstream& fout, const long& data)
{
  fout << std::right << std::setw(10) << data;
}

template<>
void write_output(std::ofstream& fout, const float& data)
{
  fout << std::right << std::fixed << std::setprecision(lim_float::max_digits10) << data;
}

template<>
void write_output(std::ofstream& fout, const double& data)
{
  fout << std::right << std::fixed << std::setprecision(lim_double::max_digits10) << data;
}

void MQWriter::write(ByteData&& key, ByteData&& value)
{
  mq_->send(std::make_pair(std::move(key), std::move(value)));
}

}  // namespace proc
}  // namespace mapreduce