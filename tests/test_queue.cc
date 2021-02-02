#include "simplemapreduce/data/queue.h"

#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "catch.hpp"

using namespace mapreduce::data;

TEST_CASE("MessageQueue", "[mq]")
{
  SECTION("string_int")
  {
    typedef std::pair<std::string, int> KV;

    size_t count = 3;
    MessageQueue<std::string, int> mq;

    std::string key{"test"};
    int value = 1;

    for (size_t i = 0; i < count; ++i)
      mq.send(KV(std::string(key), value));

    mq.end();

    for (size_t i = 0; i < count; ++i)
    {
      KV pair = mq.receive();
      REQUIRE(pair.first == key);
      REQUIRE(pair.second == value);
    }

    KV pair = mq.receive();
    REQUIRE(pair.first.empty());
  }

  SECTION("string_long")
  {
    typedef std::pair<std::string, long> KV;

    size_t count = 4;
    MessageQueue<std::string, long> mq;

    std::string key{"test"};
    long value = 1234567890;

    for (size_t i = 0; i < count; ++i)
      mq.send(KV(std::string(key), value));

    mq.end();

    for (size_t i = 0; i < count; ++i)
    {
      KV pair = mq.receive();
      REQUIRE(pair.first == key);
      REQUIRE(pair.second == value);
    }

    KV pair = mq.receive();
    REQUIRE(pair.first.empty());
  }

  SECTION("string_float")
  {
    typedef std::pair<std::string, float> KV;

    size_t count = 5;
    MessageQueue<std::string, float> mq;

    std::string key{"test"};
    float value = 1.23;

    for (size_t i = 0; i < count; ++i)
      mq.send(KV(std::string(key), value));

    mq.end();

    for (size_t i = 0; i < count; ++i)
    {
      KV pair = mq.receive();
      REQUIRE(pair.first == key);
      REQUIRE(pair.second == value);
    }

    KV pair = mq.receive();
    REQUIRE(pair.first.empty());
  }

  SECTION("string_double")
  {
    typedef std::pair<std::string, double> KV;

    size_t count = 3;
    MessageQueue<std::string, double> mq;

    std::string key{"test"};
    double value = 0.123456789;

    for (size_t i = 0; i < count; ++i)
      mq.send(KV(std::string(key), value));

    mq.end();

    for (size_t i = 0; i < count; ++i)
    {
      KV pair = mq.receive();
      REQUIRE(pair.first == key);
      REQUIRE(pair.second == value);
    }

    KV pair = mq.receive();
    REQUIRE(pair.first.empty());
  }
}

TEST_CASE("MessageQueue with threads", "[mq][threads]")
{
  typedef std::pair<std::string, long> KV;
  std::vector<std::thread> threads;
  MessageQueue<std::string, long> mq;

  unsigned int sum = 0;

  for (unsigned int i = 0; i < 10; ++i)
  {
    threads.emplace_back([&mq](int num){
      /// Each thread pushes a pair 10 times
      for (unsigned int i = 0; i < 10; ++i)
        mq.send(KV("test", num + 1));
    }, i);

    /// Target values to be pushed in total
    sum += (i + 1) * 10;
  }

  for (auto &thread: threads)
    thread.join();

  mq.end();

  unsigned int res = 0;
  while (true)
  {
    KV pair = mq.receive();
    if (pair.first.empty())
      break;

    /// Accumulate all values in message queue
    res += pair.second;
  }

  REQUIRE(res == sum);
}