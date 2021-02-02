#include "simplemapreduce/proc/writer.h"

#include <iomanip>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>

#include "simplemapreduce/commons.h"


typedef std::numeric_limits<float> lim_float;
typedef std::numeric_limits<double> lim_double;
namespace mapreduce {
namespace proc {

  BinaryFileWriter::BinaryFileWriter(const fs::path &path) : path_(std::move(path)) {
    fout_.open(path_, std::ios::binary | std::ios::trunc);
  };

  BinaryFileWriter::BinaryFileWriter(const std::string &path) : path_(std::move(path)) {
    fout_.open(path_, std::ios::binary | std::ios::trunc);
  };

  BinaryFileWriter::~BinaryFileWriter()
  {
    fout_.close();
  }

  void BinaryFileWriter::write(std::string &key, int &value)
  {
    std::lock_guard<std::mutex> lock(mr_mutex_);

    /// Store string size and the chars
    size_t data_size = key.size();
    fout_.write(reinterpret_cast<char *>(&data_size), sizeof(size_t));
    fout_.write(key.c_str(), key.size());
    /// Store provided value
    fout_.write(reinterpret_cast<char *>(&value), sizeof(int));
  }

  void BinaryFileWriter::write(std::string &key, long &value)
  {
    std::lock_guard<std::mutex> lock(mr_mutex_);

    /// Store string size and the chars
    size_t data_size = key.size();
    fout_.write(reinterpret_cast<char *>(&data_size), sizeof(size_t));
    fout_.write(key.c_str(), key.size());
    /// Store provided value
    fout_.write(reinterpret_cast<char *>(&value), sizeof(long));
  }

  void BinaryFileWriter::write(std::string &key, float &value)
  {
    std::lock_guard<std::mutex> lock(mr_mutex_);

    /// Store string size and the chars
    size_t data_size = key.size();
    fout_.write(reinterpret_cast<char *>(&data_size), sizeof(size_t));
    fout_.write(key.c_str(), key.size());
    /// Store provided value
    fout_.write(reinterpret_cast<char *>(&value), sizeof(float));
  }

  void BinaryFileWriter::write(std::string &key, double &value)
  {
    std::lock_guard<std::mutex> lock(mr_mutex_);

    /// Store string size and the chars
    size_t data_size = key.size();
    fout_.write(reinterpret_cast<char *>(&data_size), sizeof(size_t));
    fout_.write(key.c_str(), key.size());
    /// Store provided value
    fout_.write(reinterpret_cast<char *>(&value), sizeof(double));
  }

  template <>
  void MQWriter<std::string, int>::write(std::string &key, int &value)
  {
    mq_->send(std::make_pair<std::string, int>(std::move(key), std::move(value)));
  }

  template <>
  void MQWriter<std::string, long>::write(std::string &key, long &value)
  {
    mq_->send(std::make_pair<std::string, long>(std::move(key), std::move(value)));
  }

  template <>
  void MQWriter<std::string, float>::write(std::string &key, float &value)
  {
    mq_->send(std::make_pair<std::string, float>(std::move(key), std::move(value)));
  }

  template <>
  void MQWriter<std::string, double>::write(std::string &key, double &value)
  {
    mq_->send(std::make_pair<std::string, double>(std::move(key), std::move(value)));
  }

  OutputWriter::OutputWriter(const fs::path &path) {
    fout_.open(path, std::ios::out | std::ios::ate);
  };

  OutputWriter::OutputWriter(const std::string &path) {
    fout_.open(path, std::ios::out | std::ios::ate);
  };

  OutputWriter::~OutputWriter()
  {
    fout_.close();
  }

  void OutputWriter::write(std::string &key, int &value)
  {
    fout_ << std::left << std::setw(10) << key << " "
      << std::right << std::setw(6) << value << "\n";
  }

  void OutputWriter::write(std::string &key, long &value)
  {
    fout_ << std::left << std::setw(10) << key << " "
      << std::right << std::setw(10) << value << "\n";
  }

  void OutputWriter::write(std::string &key, float &value)
  {
    fout_ << std::left << std::setw(10) << key << "\t"
      << std::fixed << std::setprecision(lim_float::max_digits10) << value << "\n";
  }

  void OutputWriter::write(std::string &key, double &value)
  {
    fout_ << std::left << std::setw(10) << key << "\t"
      << std::fixed << std::setprecision(lim_double::max_digits10) << value << "\n";
  }

} // namespace proc
} // namespace mapreduce