#include <vector>

#include "simplemapreduce.h"

using namespace mapreduce;
using namespace mapreduce::type;

// This is an example app using mapreduce to calculate average value from csv files.
//
//  Dataset:
//    MovieLens 20M dataset
//    Link: https://www.kaggle.com/grouplens/movielens-20m-dataset
//
//    Only used rating.csv (690.4MB uncompressed) with preprocessing.
//    The data is split by year, e.g. 2000.csv, 2001.csv, ...
//
//  The objective is calculating rating mean per movie,
//  thus, key is movieId (here, use the id as long value to save memory consumption instead of string)

class RatingMeanMapper : public Mapper<String, Long, Long, Double> {
 public:
  void map(const String&, const Long&, const Context<Long, Double>&);
};

class RatingMeanReducer : public Reducer<Long, Double, Long, Double> {
 public:
    void reduce(const Long&, const std::vector<Double>&,
                const Context<Long, Double>&);
};

int main(int argc, char *argv[]) {
  Job job{argc, argv};
  job.set_config(Config::log_level, mapreduce::util::LogLevel::INFO);
  job.set_mapper<RatingMeanMapper>();
  job.set_reducer<RatingMeanReducer>();

  job.run();

  return 0;
}

/* --------------------------------------------------
 *   Implementation
 * -------------------------------------------------- */
void RatingMeanMapper::map(const String& input, const Long&, const Context<Long, Double>& context) {
  Long tmp, movie_id;
  Double rating;
  std::string line;
  std::istringstream iss(input);

  /// Skip title row
  std::getline(iss, line);

  while (std::getline(iss, line)) {
    std::replace(line.begin(), line.end(), ',', ' ');
    std::istringstream linestream(line);
    /// Format: index, user_id, movie_id, rating, timestamp
    linestream >> tmp >> tmp >> movie_id >> rating;
    context.write(movie_id, rating);
  }
}

void RatingMeanReducer::reduce(const Long& key,
                               const std::vector<Double>& values,
                               const Context<Long, Double>& context) {
  Long key_(key);
  Double value = REDUCE_MEAN(values);
  context.write(key_, value);
}