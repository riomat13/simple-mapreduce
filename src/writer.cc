#include "simplemapreduce/proc/writer.h"

#include <iomanip>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

typedef std::numeric_limits<float> lim_float;
typedef std::numeric_limits<double> lim_double;
namespace mapreduce {
namespace proc {

  template <>
  void write_binary(std::ofstream &fout, std::string &data)
  {
    /// Store string size and the chars
    size_t data_size = data.size();
    fout.write(reinterpret_cast<char *>(&data_size), sizeof(size_t));
    fout.write(data.c_str(), data.size());
  }

  template <>
  void write_binary(std::ofstream &fout, int &data)
  {
    fout.write(reinterpret_cast<char *>(&data), sizeof(int));
  }

  template <>
  void write_binary(std::ofstream &fout, long &data)
  {
    fout.write(reinterpret_cast<char *>(&data), sizeof(long));
  }

  template <>
  void write_binary(std::ofstream &fout, float &data)
  {
    fout.write(reinterpret_cast<char *>(&data), sizeof(float));
  }

  template <>
  void write_binary(std::ofstream &fout, double &data)
  {
    fout.write(reinterpret_cast<char *>(&data), sizeof(double));
  }

  template <>
  void write_output(std::ofstream &fout, std::string &data)
  {
    fout << std::left << std::setw(10) << data;
  }

  template <>
  void write_output(std::ofstream &fout, int &data)
  {
    fout << std::right << std::setw(6) << data;
  }

  template <>
  void write_output(std::ofstream &fout, long &data)
  {
    fout << std::right << std::setw(10) << data;
  }

  template <>
  void write_output(std::ofstream &fout, float &data)
  {
    fout << std::right << std::fixed << std::setprecision(lim_float::max_digits10) << data;
  }

  template <>
  void write_output(std::ofstream &fout, double &data)
  {
    fout << std::right << std::fixed << std::setprecision(lim_double::max_digits10) << data;
  }

} // namespace proc
} // namespace mapreduce