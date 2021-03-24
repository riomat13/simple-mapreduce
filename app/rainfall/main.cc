#include <functional>
#include <sstream>

#include <simplemapreduce.h>

// This is an example app using mapreduce to secondary sort by each month and each city.
// in each month for each city in Australia.
// The original dataset is from https://www.kaggle.com/jsphyg/weather-dataset-rattle-package
//

using namespace mapreduce;
using namespace mapreduce::type;

class RainfallMapper : public Mapper<String, Long, CompositeKey<String, String>, Double> {
 public:
  void map(const String&, const Long&,
           const Context<CompositeKey<String, String>, Double>&) override;
};

class RainfallReducer: public Reducer<CompositeKey<String, String>, Double, String, String> {
 public:
  void reduce(const CompositeKey<String, String>&, const std::vector<Double>&,
              const Context<String, String>&) override;
};

int main(int argc, char* argv[]) {
  Job job{argc, argv};

  job.set_config("log_level", mapreduce::util::LogLevel::INFO);
  job.set_mapper<RainfallMapper>();
  job.set_reducer<RainfallReducer>();

  job.run();

  return 0;
}

/* --------------------------------------------------
 *   Implementation
 * -------------------------------------------------- */
void RainfallMapper::map(const String& input, const Long&,
                         const Context<CompositeKey<String, String>, Double>& context) {
  std::istringstream iss(input);
  String line;

  // Columns:
  //    Date,Location,MinTemp,MaxTemp,Rainfall,...
  while (std::getline(iss, line)) {
    std::replace(line.begin(), line.end(), ',', ' ');
    std::istringstream linestream(line);
    String date, city, tmp;
    Double value;

    if ((linestream >> date >> city >> tmp >> tmp >> value).fail()) {
      continue;
    }

    /// Composite key will be shuffled with primary key (first value)
    /// As secondary key, use `year-month` and grouping all date data
    CompositeKey<String, String> key(city, date.substr(0, date.find_last_of('-')));
    context.write(key, value);
  }
}

void RainfallReducer::reduce(const CompositeKey<String, String>& key,
                             const std::vector<Double>& values,
                             const Context<String, String>& context) {

  /// Sort rainfall from larger -> smaller
  std::vector<Double> values_;
  values_.insert(values_.end(), values.begin(), values.end());
  std::sort(values_.begin(), values_.end(), std::greater<Double>());

  /// Output is "cityname, year-month   rainfalls,..."
  String outkey = key.first + ", " + key.second;
  std::ostringstream oss;

  for (auto& val: values_) {
    oss << val << ",";
  }

  String outvalues = oss.str();
  /// pop out the last ','
  outvalues.pop_back();

  context.write(outkey, outvalues);
}