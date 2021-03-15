#ifndef SIMPLEMAPREDUCE_PROC_WRITER_H_
#define SIMPLEMAPREDUCE_PROC_WRITER_H_

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "simplemapreduce/data/bytes.h"
#include "simplemapreduce/data/queue.h"

namespace mapreduce {
namespace proc {

/**
 * Write data as binary to file stream.
 *
 *  @param filestream  target file stream to write data
 *  @param data        data to be stored the read data
 */
template <typename T>
void write_binary(std::ofstream&, T&&);

/**
 * Format value to output.
 *
 *  @param filestream  target file stream to write data
 *  @param data        data to be stored the read data
 */
template <typename T>
void write_output(std::ofstream&, const T&);

/** Base class to write data used by context. */
class Writer {
 public:
  virtual ~Writer() = default;

  virtual void write(mapreduce::data::ByteData&&, mapreduce::data::ByteData&&) = 0;
};

/**
 * Wrapper class to write intermediate key/value data to a file.
 * This is used to write intermediate state after map process is applied.
 * The input data must be formatted in mapreduce::data::ByteData.
 * 
 * This is for RAII to handle long opened file descriptor.
 */
template <typename K, typename V>
class BinaryFileWriter : public Writer {
 public:
  /**
   * Constructor Binary data writing class.
   *
   *  @param path  file path to write the data
   */
  BinaryFileWriter(const std::filesystem::path &path);
  BinaryFileWriter(const std::string &path);
  ~BinaryFileWriter();

  /* Write data to file */
  void write(mapreduce::data::ByteData&&, mapreduce::data::ByteData&&);

  /* Return current set path */
  const std::string &get_path() { return path_; }

 private:
  /// Target file path to write data
  std::string path_;
  /// File stream to write the binary data
  std::ofstream fout_;
};

/**
 * Write key/value data to MessageQueue.
 * The input data must be formatted in mapreduce::data::ByteData.
 */
class MQWriter : public Writer {
 public:

  MQWriter(std::shared_ptr<mapreduce::data::MessageQueue> mq) : mq_(mq) {};
  ~MQWriter() {};

  /* Save data to Message Queue */
  void write(mapreduce::data::ByteData&&, mapreduce::data::ByteData&&);

 private:
  std::shared_ptr<mapreduce::data::MessageQueue> mq_ = nullptr;
};

/**
 * Wrapper class to write output key/value result to a file.
 * The input data must be formatted in mapreduce::data::ByteData.
 * This is for RAII to handle long opened file descriptor.
 */
template <typename K, typename V>
class OutputWriter : public Writer {
 public:
  /**
   * Constructor
   *
   *  @param path  target file path to write data
   */
  OutputWriter(const std::filesystem::path&);
  OutputWriter(const std::string&);
  ~OutputWriter();

  /* Write data to output file */
  void write(mapreduce::data::ByteData&&, mapreduce::data::ByteData&&);

 private:
  std::ofstream fout_;
};

}  // namespace proc
}  // namespace mapreduce

#include "simplemapreduce/proc/writer-inl.h"

#endif  // SIMPLEMAPREDUCE_PROC_WRITER_H_