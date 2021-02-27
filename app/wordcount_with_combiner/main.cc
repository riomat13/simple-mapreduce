#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "simplemapreduce.h"

using namespace mapreduce;
using namespace mapreduce::type;

// This is an example app using mapreduce to count each word appeared in texts.
//
// The map operation consts of:
//    - Remove puctuations
//    - Tokenize by spaces/tabs
//
// and in reduce operation, aggregate the count associated with the each key words.
//
// In addition to this, added Combiner layer before shuffle process to optimize.

class WordCountMapper : public Mapper<String, Long, String, Long> {
 public:
  void map(const String&, const Long&, const Context<String, Long>&);
};

class WordCountReducer : public Reducer<String, Long, String, Long> {
 public:
    void reduce(const String&, const std::vector<Long>&,
                const Context<String, Long>&);
};

int main(int argc, char *argv[]) {
  /// Set directory paths to handle data

  Job job{argc, argv};

  job.set_config("n_groups", -1);
  job.set_config("log_level", mapreduce::util::LogLevel::INFO);

  job.set_mapper<WordCountMapper>();
  job.set_reducer<WordCountReducer>();

  // Set Combiner as the same process as reduce
  job.set_combiner<WordCountReducer>();

  job.run();

  return 0;
}

/* --------------------------------------------------
 *   Implementation
 * -------------------------------------------------- */
void WordCountMapper::map(const String& input, const Long&, const Context<String, Long>& context) {
  std::string line;
  std::istringstream iss(input);

  while (std::getline(iss, line)) {
    /// Remove all punctuations
    std::replace_if(line.begin(), line.end(),
                    [](unsigned char c){ return std::ispunct(c); }, ' ');

    Long count{1};
    String word;
    std::istringstream linestream(line);

    /// Tokenize only by spliting by space/tab
    /// No lower cased nor any stemming, lemmatizing
    while (linestream >> word) {
      context.write(word, count);
    }
  }
}

void WordCountReducer::reduce(const String& key, const std::vector<Long>& values,
                              const Context<String, Long>& context) {
  String keyitem(key);

  /// Aggregate word count
  Long count = REDUCE_SUM(values);

  context.write(keyitem, count);
}