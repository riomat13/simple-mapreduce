#include "proc/writer.h"

#include <iomanip>
#include <mutex>
#include <thread>

#include "commons.h"

namespace mapreduce {
namespace proc {

  BinaryFileWriter::BinaryFileWriter(const std::string &path) : path_(std::move(path)) {
    fout_.open(path_, std::ios::binary);
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

  OutputWriter::OutputWriter(const std::string &path) {
    fout_.open(path);
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
      << std::right << std::setw(6) << value << "\n";
  }

} // namespace proc
} // namespace mapreduce