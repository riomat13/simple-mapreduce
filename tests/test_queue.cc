#include "simplemapreduce/data/queue.h"

#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "catch.hpp"

#include "simplemapreduce/data/bytes.h"

using namespace mapreduce::data;

TEST_CASE("MessageQueue", "[mq]") {

  SECTION("string/int") {
    size_t count = 3;
    MessageQueue mq;

    std::string target_key{"test"};
    ByteData key{std::string(target_key)};
    int target_value{1};
    ByteData value(target_value);

    for (size_t i = 0; i < count; ++i)
      mq.send(BytePair(key, value));

    mq.end();

    for (size_t i = 0; i < count; ++i) {
      BytePair pair = mq.receive();
      REQUIRE(pair.first.get_data<std::string>() == target_key);
      REQUIRE(pair.second.get_data<int>() == target_value);
    }

    BytePair pair = mq.receive();
    REQUIRE(pair.first.empty());
  }

  SECTION("string/double") {
    size_t count = 3;
    MessageQueue mq;

    std::string target_key{"test"};
    ByteData key{std::string(target_key)};
    double target_value{0.123456789};
    ByteData value(target_value);

    for (size_t i = 0; i < count; ++i)
      mq.send(BytePair(key, value));

    mq.end();

    for (size_t i = 0; i < count; ++i) {
      BytePair pair = mq.receive();
      REQUIRE(pair.first.get_data<std::string>() == target_key);
      REQUIRE(pair.second.get_data<double>() == target_value);
    }

    BytePair pair = mq.receive();
    REQUIRE(pair.first.empty());
  }
}

TEST_CASE("MessageQueue with threads", "[mq][threads]") {
  std::vector<std::thread> threads;
  MessageQueue mq;

  unsigned int sum = 0;

  for (unsigned int i = 0; i < 2; ++i) {
    threads.emplace_back([&mq](int num){
      /// Each thread pushes a pair 10 times
      for (unsigned int i = 0; i < 10; ++i) {
        ByteData key("test");
        ByteData value(long(num+1));
        mq.send(std::make_pair(key, value));
      }
    }, i);

    /// Target values to be pushed in total
    sum += (i + 1) * 10;
  }

  for (auto& thread: threads)
    thread.join();

  mq.end();

  unsigned int res = 0;
  while (true) {
    BytePair pair = mq.receive();
    if (pair.first.empty())
      break;

    /// Accumulate all values in message queue
    res += pair.second.get_data<long>();
  }

  REQUIRE(res == sum);
}