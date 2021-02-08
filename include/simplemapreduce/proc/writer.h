#ifndef SIMPLEMAPREDUCE_PROC_WRITER_H_
#define SIMPLEMAPREDUCE_PROC_WRITER_H_

#include <iostream>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "simplemapreduce/data/queue.h"

namespace fs = std::filesystem;

using namespace mapreduce::data;

namespace mapreduce {
namespace proc {

  /**
   * Read data from binary.
   *
   *  @param filestream&  target file stream to write data
   *  @param data&        data to be stored the read data
   */
  template <typename T>
  void write_binary(std::ofstream&, T&);

  /**
   * Format value to output.
   *
   *  @param filestream&  target file stream to write data
   *  @param data& data to be stored the read data
   */
  template <typename T>
  void write_output(std::ofstream&, T&);

  /** Base class to write data used by context. */
  template <typename K, typename V>
  class Writer
  {
   public:
    virtual ~Writer() = default;

    virtual void write(K&, V&) = 0;
  };

  /**
   * Wrapper class to write intermediate result to a file.
   * This is used to write intermediate state after map process is applied.
   * 
   * This is for RAII to handle long opened file descriptor.
   */
  template <typename K, typename V>
  class BinaryFileWriter : public Writer<K, V>
  {
   public:
    BinaryFileWriter(const fs::path &path);
    BinaryFileWriter(const std::string &path);
    ~BinaryFileWriter();

    /* Write data to file */
    void write(K&, V&);

    /* Return current set path */
    const std::string &get_path() { return path_; }

   private:
    std::string path_;
    std::ofstream fout_;
  };

  template <typename K, typename V>
  class MQWriter : public Writer<K, V>
  {
   public:
    typedef MessageQueue<K, V> Queue;

    MQWriter(std::shared_ptr<Queue> mq) : mq_(mq) {};
    ~MQWriter() {};

    /* Save data to Message Queue */
    void write(K&, V&);

   private:
    std::shared_ptr<Queue> mq_ = nullptr;
  };

  /**
   * Wrapper class to write output result to a file.
   * This is for RAII to handle long opened file descriptor.
   */
  template <typename K, typename V>
  class OutputWriter : public Writer<K, V>
  {
   public:
    /**
     * Constructor
     *
     *  @param path& target file path to write data
     */
    OutputWriter(const fs::path&);
    OutputWriter(const std::string&);
    ~OutputWriter();

    /* Write data to output file */
    void write(K&, V&);
  
   private:
    std::ofstream fout_;
  };

} // namespace proc
} // namespace mapreduce

#include "simplemapreduce/proc/writer.tcc"

#endif