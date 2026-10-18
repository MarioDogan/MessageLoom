#include "mloom/core/status.hpp"

namespace mloom {

std::string_view ToString(StatusCode code) noexcept {
  switch (code) {
    case StatusCode::kOk:
      return "Ok";
    case StatusCode::kNeedMoreData:
      return "NeedMoreData";
    case StatusCode::kEndOfStream:
      return "EndOfStream";
    case StatusCode::kTimeout:
      return "Timeout";
    case StatusCode::kTypeMismatch:
      return "TypeMismatch";
    case StatusCode::kInvalidArgument:
      return "InvalidArgument";
    case StatusCode::kUnsupported:
      return "Unsupported";
    case StatusCode::kClosed:
      return "Closed";
    case StatusCode::kError:
      return "Error";
  }
  return "Unknown";
}

std::ostream& operator<<(std::ostream& os, StatusCode code) { return os << ToString(code); }

std::string Status::ToString() const {
  std::string text(mloom::ToString(code_));
  if (!message_.empty()) {
    text += ": ";
    text += message_;
  }
  return text;
}

std::ostream& operator<<(std::ostream& os, const Status& status) { return os << status.ToString(); }

}  // namespace mloom
