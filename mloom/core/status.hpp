#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

namespace mloom {

/// Category of an operation's outcome. Business logic uses the first three codes to steer the
/// node loop; the remaining codes describe failures.
enum class StatusCode : std::uint8_t {
  kOk = 0,           ///< Success; an accompanying result is valid.
  kNeedMoreData,     ///< Input was accepted but no output is available yet.
  kEndOfStream,      ///< The producer is exhausted; the node loop finishes.
  kTimeout,          ///< Nothing happened within the allotted time.
  kTypeMismatch,     ///< A message does not carry the expected payload type.
  kInvalidArgument,  ///< Malformed input, configuration or wire data.
  kUnsupported,      ///< The operation is not available, e.g. no codec for a type.
  kClosed,           ///< The resource is closed or was never opened.
  kError,            ///< Any other failure.
};

std::string_view ToString(StatusCode code) noexcept;
std::ostream& operator<<(std::ostream& os, StatusCode code);

/// Result of an operation: a code plus an optional human-readable detail.
/// A default-constructed Status is Ok.
class [[nodiscard]] Status {
 public:
  Status() noexcept = default;
  explicit Status(StatusCode code, std::string message = {})
      : code_(code), message_(std::move(message)) {}

  static Status Ok() noexcept { return Status(); }
  static Status NeedMoreData(std::string message = {}) {
    return Status(StatusCode::kNeedMoreData, std::move(message));
  }
  static Status EndOfStream(std::string message = {}) {
    return Status(StatusCode::kEndOfStream, std::move(message));
  }
  static Status Timeout(std::string message = {}) {
    return Status(StatusCode::kTimeout, std::move(message));
  }
  static Status TypeMismatch(std::string message = {}) {
    return Status(StatusCode::kTypeMismatch, std::move(message));
  }
  static Status InvalidArgument(std::string message = {}) {
    return Status(StatusCode::kInvalidArgument, std::move(message));
  }
  static Status Unsupported(std::string message = {}) {
    return Status(StatusCode::kUnsupported, std::move(message));
  }
  static Status Closed(std::string message = {}) {
    return Status(StatusCode::kClosed, std::move(message));
  }
  static Status Error(std::string message = {}) {
    return Status(StatusCode::kError, std::move(message));
  }

  bool ok() const noexcept { return code_ == StatusCode::kOk; }
  StatusCode code() const noexcept { return code_; }
  const std::string& message() const noexcept { return message_; }

  /// "Code" or "Code: message".
  std::string ToString() const;

  friend bool operator==(const Status&, const Status&) = default;

 private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

std::ostream& operator<<(std::ostream& os, const Status& status);

}  // namespace mloom
