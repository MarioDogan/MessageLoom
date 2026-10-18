#include "mloom/node/testing/third_party/acme_sensors.hpp"

#include <cmath>
#include <cstdlib>
#include <numeric>
#include <utility>

namespace acme {

double CelsiusToFahrenheit(double celsius) { return celsius * 9.0 / 5.0 + 32.0; }

bool ParseReading(const std::string& line, Reading* out) {
  const std::size_t equals = line.find('=');
  if (equals == std::string::npos || equals == 0 || equals + 1 == line.size()) return false;
  const std::string number = line.substr(equals + 1);
  char* end = nullptr;
  const double celsius = std::strtod(number.c_str(), &end);
  if (end != number.c_str() + number.size()) return false;
  out->sensor = line.substr(0, equals);
  out->celsius = celsius;
  return true;
}

std::optional<Alarm> CheckLimits(const Reading& reading, double warning, double critical) {
  if (reading.celsius >= critical) return Alarm{reading.sensor, reading.celsius, Level::kCritical};
  if (reading.celsius >= warning) return Alarm{reading.sensor, reading.celsius, Level::kWarning};
  return std::nullopt;
}

std::vector<std::uint8_t> DecodeFrame(const Frame& frame) {
  if (frame.bytes.empty()) throw DecodeError("frame " + std::to_string(frame.id) + " is empty");
  std::uint8_t sum = 0;
  for (std::size_t i = 0; i + 1 < frame.bytes.size(); ++i) {
    sum = static_cast<std::uint8_t>(sum + frame.bytes[i]);
  }
  if (sum != frame.bytes.back()) {
    throw DecodeError("frame " + std::to_string(frame.id) + " has a bad checksum");
  }
  return {frame.bytes.begin(), frame.bytes.end() - 1};
}

Frame InvertFrame(Frame&& frame) {
  Frame inverted(std::move(frame));
  for (std::uint8_t& byte : inverted.bytes) byte = static_cast<std::uint8_t>(~byte);
  return inverted;
}

MovingAverage::MovingAverage(std::size_t window) : window_(window == 0 ? 1 : window) {}

void MovingAverage::Add(double value) {
  values_.push_back(value);
  if (values_.size() > window_) values_.pop_front();
  ++count_;
}

bool MovingAverage::full() const { return values_.size() == window_; }

double MovingAverage::average() const {
  if (values_.empty()) return 0.0;
  return std::accumulate(values_.begin(), values_.end(), 0.0) / static_cast<double>(values_.size());
}

std::size_t MovingAverage::count() const { return count_; }

void LineSplitter::Feed(const std::string& chunk) {
  for (const char c : chunk) {
    if (c == '\n') {
      lines_.push_back(std::move(partial_));
      partial_.clear();
    } else {
      partial_ += c;
    }
  }
}

std::vector<std::string> LineSplitter::TakeLines() {
  std::vector<std::string> lines;
  lines.swap(lines_);
  return lines;
}

std::size_t LineSplitter::buffered() const { return partial_.size(); }

void Calibrator::Configure(double gain, double offset) {
  gain_ = gain;
  offset_ = offset;
}

double Calibrator::Apply(double raw) const { return raw * gain_ + offset_; }

Recorder::Recorder(std::size_t capacity) : capacity_(capacity) {}

bool Recorder::Write(Reading&& reading) {
  if (records_.size() >= capacity_) return false;
  records_.push_back(std::move(reading));
  return true;
}

const std::vector<Reading>& Recorder::records() const { return records_; }

JumpDetector::JumpDetector(double threshold) : threshold_(threshold) {}

void JumpDetector::OnJump(Callback callback) { callback_ = std::move(callback); }

void JumpDetector::Process(const Reading& reading) {
  const auto last = last_.find(reading.sensor);
  if (last != last_.end() && callback_ && std::abs(reading.celsius - last->second) >= threshold_) {
    callback_(Jump{reading.sensor, last->second, reading.celsius});
  }
  last_[reading.sensor] = reading.celsius;
}

ReadingGenerator::ReadingGenerator(std::string sensor, double start, double step, std::size_t count)
    : sensor_(std::move(sensor)), start_(start), step_(step), count_(count) {}

bool ReadingGenerator::HasNext() const { return produced_ < count_; }

Reading ReadingGenerator::Next() {
  const double celsius = start_ + step_ * static_cast<double>(produced_++);
  return Reading{sensor_, celsius};
}

PollingSensor::PollingSensor(std::string name, int ready_every)
    : name_(std::move(name)), ready_every_(ready_every < 1 ? 1 : ready_every) {}

std::optional<Reading> PollingSensor::Poll() {
  if (disconnect_after_ >= 0 && polls_ >= disconnect_after_) connected_ = false;
  if (!connected_) throw std::runtime_error("sensor '" + name_ + "' disconnected");
  if (++polls_ % ready_every_ != 0) return std::nullopt;
  return Reading{name_, 20.0 + readings_++};
}

void PollingSensor::Disconnect() { connected_ = false; }

void PollingSensor::DisconnectAfter(int polls) { disconnect_after_ = polls; }

}  // namespace acme
