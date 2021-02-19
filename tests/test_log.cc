#include "simplemapreduce/util/log.h"

#include <sstream>
#include <string>

#include "catch.hpp"

using namespace mapreduce::util;

TEST_CASE("LogLevel", "[log][loglevel]") {
  /// Check log level order
  REQUIRE(LogLevel::DEBUG < LogLevel::INFO);
  REQUIRE(LogLevel::INFO < LogLevel::WARNING);
  REQUIRE(LogLevel::WARNING < LogLevel::ERROR);
  REQUIRE(LogLevel::ERROR < LogLevel::CRITICAL);
  REQUIRE(LogLevel::CRITICAL < LogLevel::DISABLE);
}

TEST_CASE("LogBuffer", "[log][buffer]") {
  LogBuffer buffer;
  buffer << 10;
  buffer << "test";
  buffer << 42.1424 << ' ' << "sample";

  REQUIRE(buffer.to_string() == "10test42.1424 sample");
}

TEST_CASE("Logger", "[log][logger]") {

  SECTION("general")
  {
    Logger logger{};
    auto buff = logger.log(LogLevel::INFO, "test", ' ', "log", ' ', 100);
    auto log = buff->to_string();
    REQUIRE(log.find("[INFO]") != std::string::npos);
    REQUIRE(log.find("test log 100") != std::string::npos);
  }

  SECTION("DEBUG") {
    Logger logger{};
    logger.set_log_level(LogLevel::DEBUG);
    logger.debug(10, " debug ", 20, " example");

    auto buff = logger.debug(10, " debug ", 20, " example");
    auto log = buff->to_string();
    REQUIRE(log.find("[DEBUG]") != std::string::npos);
    REQUIRE(log.find("10 debug 20 example") != std::string::npos);
  }

  SECTION("INFO") {
    Logger logger{};
    logger.set_log_level(LogLevel::INFO);
    REQUIRE(logger.debug(10, " debug ", 20, " example") == nullptr);

    auto buff = logger.info("test", ' ', 10, 20, " example");
    auto log = buff->to_string();
    REQUIRE(log.find("[INFO]") != std::string::npos);
    REQUIRE(log.find("test 1020 example") != std::string::npos);
  }

  SECTION("WARNING") {
    Logger logger{};
    logger.set_log_level(LogLevel::WARNING);
    REQUIRE(logger.info("info") == nullptr);

    std::string word{"example"};
    long value{123456789l};
    auto buff = logger.warning("warning ", value, ' ', word);
    auto log = buff->to_string();
    REQUIRE(log.find("[WARNING]") != std::string::npos);
    REQUIRE(log.find("warning 123456789 example") != std::string::npos);
  }

  SECTION("ERROR") {
    Logger logger{};
    logger.set_log_level(LogLevel::ERROR);
    REQUIRE(logger.warning("warning") == nullptr);

    std::string word{"test"};
    long value{123456789l};
    auto buff = logger.error("error ", value, ' ', word);
    auto log = buff->to_string();
    REQUIRE(log.find("[ERROR]") != std::string::npos);
    REQUIRE(log.find("error 123456789 test") != std::string::npos);
  }

  SECTION("CRITICAL") {
    Logger logger{};
    logger.set_log_level(LogLevel::CRITICAL);
    REQUIRE(logger.error("error") == nullptr);

    std::string word{"sample"};
    int value{12345};
    auto buff = logger.critical("critical ", value, ' ', word);
    auto log = buff->to_string();
    REQUIRE(log.find("[URGENT]") != std::string::npos);
    REQUIRE(log.find("critical 12345 sample") != std::string::npos);
  }

  SECTION("CRITICAL") {
    Logger logger{};
    logger.set_log_level(LogLevel::DISABLE);
    REQUIRE(logger.critical("test") == nullptr);
  }
}