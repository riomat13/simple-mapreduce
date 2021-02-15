#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "simplemapreduce.h"

// This is an example app using mapreduce to count each word
// appeared in texts.
//
// The map operation consts of:
//    - Remove puctuations
//    - Tokenize by spaces/tabs
//
// and in reduce operation, aggregate the count associated with the each
// key words.
//

class WordCountMapper : public mapreduce::Mapper<std::string, long, std::string, long>
{
 public:
  void map(const std::string&, const long&, const mapreduce::Context<std::string, long>&);
};

class WordCountReducer
  : public mapreduce::Reducer<std::string, long, std::string, long>
{
 public:
    void reduce(const std::string &,
                const std::vector<long> &,
                const mapreduce::Context<std::string, long> &);
};

int main(int argc, char *argv[])
{
  /// Set directory paths to handle data
  mapreduce::FileFormat fmt{};
  fmt.add_input_path("./inputs");
  fmt.set_output_path("./outputs");

  mapreduce::Job job{argc, argv};

  job.set_file_format(fmt);        // Notify target directories
  job.set_config("n_groups", -1);  // Number of workers to run reduced task
                                   // This will be a number of output files
                                   // (Note: even if this is set to small value,
                                   //  all workers will be used for other processes)
                                   // -1 for using all workers
  job.set_config("log_level", mapreduce::util::LogLevel::INFO);
                                   // DEBUG, INFO, WARNING, ERROR, CRITICAL, DISABLE
                                   // Disable logs which levels are less than it

  job.set_mapper<WordCountMapper>();
  job.set_reducer<WordCountReducer>();

  /// Start mapreduce task
  job.run();

  return 0;
}


/* --------------------------------------------------
 *   Implementation
 * -------------------------------------------------- */
void WordCountMapper::map(const std::string &input, const long &, const mapreduce::Context<std::string, long> &context)
{
  std::string line;
  std::istringstream iss(input);

  while (std::getline(iss, line))
  {
    /// Remove all punctuations
    std::replace_if(line.begin(), line.end(),
                    [](unsigned char c){ return std::ispunct(c); }, ' ');

    long count{1};
    std::string word;
    std::istringstream linestream(line);

    /// Tokenize only by spliting by space/tab
    /// No lower cased nor any stemming, lemmatizing
    while (linestream >> word)
    {
      context.write(word, count);
    }
  }
}

void WordCountReducer::reduce(const std::string &key,
                              const std::vector<long> &values,
                              const mapreduce::Context<std::string, long> &context)
{
  std::string keyitem(key);

  /// Aggregate word count
  /// values.size() should be faster since it is O(1),
  /// but for reduce operation, use summation instead.
  long count = REDUCE_SUM(values);

  context.write(keyitem, count);
}