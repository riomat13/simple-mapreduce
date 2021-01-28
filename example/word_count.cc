#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "mapreduce.h"

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

class WordCountMapper : public mapreduce::Mapper<std::string, long>
{
 public:
  void map(const std::string &, const mapreduce::Context &);
};

class WordCountReducer
  : public mapreduce::Reducer<std::string, long, std::string, long>
{
 public:
    void reduce(const std::string &,
                const std::vector<long> &,
                const mapreduce::Context &);
};

int main(int argc, char *argv[])
{
  /// Set directory paths to handle data
  mapreduce::FileFormat fmt{};
  fmt.add_input_path("./inputs");
  fmt.set_output_path("./outputs");

  mapreduce::Job<WordCountMapper, WordCountReducer> job{argc, argv};

  job.set_file_format(fmt);        // Notify target directories
  job.set_config("n_groups", -1);  // Number of workers to run reduced task
                                   // This will be a number of output files
                                   // (Note: even if this is set to small value,
                                   //  all workers will be used for other processes)
                                   // -1 for using all workers
  job.set_config("log_level", 2);  // 1->debug, 2->info, 3->warning,...
                                   // Disable logs which levels are less than it

  /// Start mapreduce task
  job.run();

  return 0;
}


/* --------------------------------------------------
 *   Implementation
 * -------------------------------------------------- */
void WordCountMapper::map(const std::string &input, const mapreduce::Context &context)
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
                              const mapreduce::Context &context)
{
  long count = 0;

  /// Aggregate word count
  /// (for shortcut, `count = values.size()` is suffice and faster
  ///  since just `1` is assigned as value,
  ///  but to show as an example of aggregation, leave the for-loop)
  for (auto &val: values)
    count += val;

  std::string keyitem(key);
  context.write(keyitem, count);
}