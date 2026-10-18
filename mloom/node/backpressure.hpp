#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string_view>

#include "mloom/core/message.hpp"
#include "mloom/core/timeout.hpp"

namespace mloom {

/// What a node does with a message that a consumer had no room for within the send timeout.
enum class BackpressureAction : std::uint8_t {
  kReportError,  ///< Drop the message and report an Error (code Timeout) on the status socket.
  kSkip,         ///< Drop the message silently.
  kRetry,        ///< Keep waiting for room, for another send timeout.
};

constexpr std::string_view ToString(BackpressureAction action) noexcept {
  switch (action) {
    case BackpressureAction::kReportError:
      return "ReportError";
    case BackpressureAction::kSkip:
      return "Skip";
    case BackpressureAction::kRetry:
      return "Retry";
  }
  return "Unknown";
}

inline std::ostream& operator<<(std::ostream& os, BackpressureAction action) {
  return os << ToString(action);
}

/// A message that waited a whole send timeout for room on one of the node's outputs. `node` is
/// valid during the handler call only.
struct BackpressureEvent {
  std::string_view node{};
  std::size_t output = 0;
  MessagePtr message{};
  /// How long this attempt waited.
  Timeout waited{};
  /// 1 for the first timeout of the message, 2 after the first kRetry, and so on.
  std::uint32_t attempt = 1;
};

/// Decides about a message that a consumer had no room for. Injected through NodeCoreOptions;
/// runs on the node's thread, which does nothing else meanwhile.
using BackpressureHandler = std::function<BackpressureAction(const BackpressureEvent& event)>;

}  // namespace mloom
