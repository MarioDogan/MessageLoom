#pragma once

// Stand-in for a third-party sensor SDK, used to test how existing business logic is integrated
// into logic handlers. It knows nothing about MessageLoom: it has its own types, its own error
// conventions (bool results with out parameters, std::optional, exceptions, callbacks) and
// stateful classes, one of them not copyable.

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace acme {

struct Reading {
  std::string sensor;
  double celsius = 0.0;
};

enum class Level { kWarning, kCritical };

struct Alarm {
  std::string sensor;
  double celsius = 0.0;
  Level level = Level::kWarning;
};

struct Jump {
  std::string sensor;
  double from = 0.0;
  double to = 0.0;
};

/// Raw telegram: payload bytes followed by one checksum byte (low byte of the payload sum).
struct Frame {
  std::uint32_t id = 0;
  std::vector<std::uint8_t> bytes;
};

class DecodeError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// --- Functions ----------------------------------------------------------------------------------

double CelsiusToFahrenheit(double celsius);

/// Parses "<sensor>=<celsius>", e.g. "boiler=71.5". Returns false and leaves `out` untouched on
/// malformed input.
bool ParseReading(const std::string& line, Reading* out);

/// An alarm when `reading` reaches the `warning` or `critical` limit, nothing otherwise.
std::optional<Alarm> CheckLimits(const Reading& reading, double warning, double critical);

/// Verifies the checksum and returns the payload. Throws DecodeError for empty or corrupt frames.
std::vector<std::uint8_t> DecodeFrame(const Frame& frame);

/// Inverts every byte. Takes the frame over so that large frames are never copied.
Frame InvertFrame(Frame&& frame);

// --- Stateful classes ---------------------------------------------------------------------------

/// Average of the last `window` values.
class MovingAverage {
 public:
  explicit MovingAverage(std::size_t window);

  void Add(double value);
  bool full() const;
  double average() const;
  /// Values added so far.
  std::size_t count() const;

 private:
  std::size_t window_;
  std::deque<double> values_;
  std::size_t count_ = 0;
};

/// Reassembles '\n'-terminated lines from arbitrary chunks of text.
class LineSplitter {
 public:
  void Feed(const std::string& chunk);
  /// Complete lines fed so far, in order; they are handed out only once.
  std::vector<std::string> TakeLines();
  /// Characters of the unfinished line.
  std::size_t buffered() const;

 private:
  std::string partial_;
  std::vector<std::string> lines_;
};

/// Linear correction raw * gain + offset. The parameters may change at any time.
class Calibrator {
 public:
  void Configure(double gain, double offset);
  double Apply(double raw) const;

 private:
  double gain_ = 1.0;
  double offset_ = 0.0;
};

/// Stores readings up to a capacity. Not copyable, like a class owning a file or device.
class Recorder {
 public:
  explicit Recorder(std::size_t capacity);
  Recorder(const Recorder&) = delete;
  Recorder& operator=(const Recorder&) = delete;

  /// Takes the reading over; returns false when the recorder is full.
  bool Write(Reading&& reading);
  const std::vector<Reading>& records() const;

 private:
  std::size_t capacity_;
  std::vector<Reading> records_;
};

/// Reports, through a callback, when a sensor's value changes by at least `threshold` between
/// two consecutive readings.
class JumpDetector {
 public:
  using Callback = std::function<void(const Jump& jump)>;

  explicit JumpDetector(double threshold);

  void OnJump(Callback callback);
  void Process(const Reading& reading);

 private:
  double threshold_;
  Callback callback_;
  std::map<std::string, double> last_;
};

/// Produces `count` readings of one sensor, starting at `start` and rising by `step`.
class ReadingGenerator {
 public:
  ReadingGenerator(std::string sensor, double start, double step, std::size_t count);

  bool HasNext() const;
  Reading Next();

 private:
  std::string sensor_;
  double start_;
  double step_;
  std::size_t count_;
  std::size_t produced_ = 0;
};

/// A device that has a new reading on every `ready_every`-th poll.
class PollingSensor {
 public:
  PollingSensor(std::string name, int ready_every);

  /// The next reading, or nothing when the device is not ready yet. Throws std::runtime_error
  /// once the device is disconnected.
  std::optional<Reading> Poll();
  void Disconnect();
  /// Simulates a device that drops out: every poll after the first `polls` throws.
  void DisconnectAfter(int polls);

 private:
  std::string name_;
  int ready_every_;
  int polls_ = 0;
  int readings_ = 0;
  int disconnect_after_ = -1;
  bool connected_ = true;
};

}  // namespace acme
