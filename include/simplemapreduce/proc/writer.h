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

  /** Base class to write data used by context. */
  class Writer
  {
   public:
    virtual ~Writer() = default;

    virtual void write(std::string &, int&) = 0;
    virtual void write(std::string &, long&) = 0;
    virtual void write(std::string &, float&) = delete;
    virtual void write(std::string &, double&) = delete;
  };

  /**
   * Wrapper class to write intermediate result to a file.
   * This is used to write intermediate state after map process is applied.
   * 
   * This is for RAII to handle long opened file descriptor.
   */
  class BinaryFileWriter : public Writer
  {
   public:
    BinaryFileWriter(const std::string &path);
    ~BinaryFileWriter();

    /* Write data to file */
    void write(std::string &, int&);
    void write(std::string &, long&);

    // TODO
    // void write(const std::string &, const float&);
    // void write(const std::string &, const double&);

    /* Return current set path */
    const std::string &get_path() { return path_; }

   private:
    std::string path_;
    std::ofstream fout_;
  };

  template <typename K, typename V>
  class MQWriter : public Writer
  {
   public:
    typedef MessageQueue<K, V> Queue;

    MQWriter(std::shared_ptr<Queue> mq) : mq_(mq) {};
    ~MQWriter() {};

    /* Save data to Message Queue */
    void write(std::string &, int&);
    void write(std::string &, long&);

    // TODO
    // void write(std::string &, float&);
    // void write(std::string &, double&);

   private:
    std::shared_ptr<Queue> mq_ = nullptr;
  };

  /**
   * Wrapper class to write output result to a file.
   * This is for RAII to handle long opened file descriptor.
   */
  class OutputWriter : public Writer
  {
   public:
    OutputWriter(const fs::path &);
    OutputWriter(const std::string &);
    ~OutputWriter();

    /* Write data to output file */
    void write(std::string &, int&);
    void write(std::string &, long&);
  
   private:
    std::ofstream fout_;
  };

} // namespace proc
} // namespace mapreduce

#include "simplemapreduce/proc/writer.tcc"

#endif