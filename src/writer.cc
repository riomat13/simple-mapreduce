#include "simplemapreduce/proc/writer.h"

#include <iomanip>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

#include "simplemapreduce/commons.h"
#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/type.h"

using namespace mapreduce::data;
using namespace mapreduce::type;

using lim_float = std::numeric_limits<Float>;
using lim_double = std::numeric_limits<Double>;

namespace mapreduce {
namespace proc {

template<>
void write_binary(std::ofstream& fout, ByteData&& bdata) {
  /// Write size for array type (e.g. vector, string)
  /// TODO: find better way to detect string(char array)
  if (bdata.size() > 1 || bdata.bsize() == 1) {
    Size_t data_size = bdata.bsize();
    fout.write(reinterpret_cast<char*>(&data_size), sizeof(Size_t));
  }
  fout.write(bdata.get_byte(), bdata.bsize());
}

template<>
void write_binary(std::ofstream& fout, const String& data) {
  /// Store string size and the chars
  Size_t data_size = data.size();
  fout.write(reinterpret_cast<char*>(&data_size), sizeof(Size_t));
  fout.write(data.c_str(), data_size);
}

template<>
void write_binary(std::ofstream& fout, Int16&& data) {
  fout.write(reinterpret_cast<char*>(const_cast<Int16*>(&data)), sizeof(Int16));
}

template<>
void write_binary(std::ofstream& fout, Int&& data) {
  fout.write(reinterpret_cast<char*>(const_cast<Int*>(&data)), sizeof(Int));
}

template<>
void write_binary(std::ofstream& fout, Long&& data) {
  fout.write(reinterpret_cast<char*>(const_cast<Long*>(&data)), sizeof(Long));
}

template<>
void write_binary(std::ofstream& fout, Float&& data) {
  fout.write(reinterpret_cast<char*>(const_cast<Float*>(&data)), sizeof(Float));
}

template<>
void write_binary(std::ofstream& fout, Double&& data) {
  fout.write(reinterpret_cast<char*>(const_cast<Double*>(&data)), sizeof(Double));
}

template<>
void write_output(std::ofstream& fout, const String& data) {
  fout << std::left << std::setw(10) << data;
}

template<>
void write_output(std::ofstream& fout, const Int16& data) {
  fout << std::right << std::setw(6) << data;
}

template<>
void write_output(std::ofstream& fout, const Int& data) {
  fout << std::right << std::setw(6) << data;
}

template<>
void write_output(std::ofstream& fout, const Long& data) {
  fout << std::right << std::setw(10) << data;
}

template<>
void write_output(std::ofstream& fout, const Float& data) {
  fout << std::right << std::fixed << std::setprecision(lim_float::max_digits10) << data;
}

template<>
void write_output(std::ofstream& fout, const Double& data) {
  fout << std::right << std::fixed << std::setprecision(lim_double::max_digits10) << data;
}

void MQWriter::write(ByteData&& key, ByteData&& value) {
  mq_->send(std::make_pair(std::move(key), std::move(value)));
}

}  // namespace proc
}  // namespace mapreduce