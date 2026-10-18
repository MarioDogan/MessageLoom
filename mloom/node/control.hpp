#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>

#include "mloom/core/codec.hpp"
#include "mloom/core/status.hpp"

namespace mloom {

/// Lifecycle state of a node.
enum class NodeState : std::uint8_t {
  kCreated,  ///< Not running yet.
  kRunning,  ///< Business logic runs (free-running) or runs on Trigger (triggered).
  kPaused,   ///< Only control commands are serviced.
  kStopped,  ///< The loop ended normally (stop request or end of stream).
  kFailed,   ///< Startup or the transport failed.
};

/// Commands a node accepts on its control socket.
enum class ControlType : std::uint8_t {
  kStart,        ///< Paused -> Running.
  kPause,        ///< Running -> Paused.
  kStop,         ///< End the node loop.
  kTrigger,      ///< Run one step of a triggered loop.
  kQueryStatus,  ///< Reply with a NodeStatus on the status socket.
  kFinish,       ///< Process the input that is still queued, then end the node loop.
};

/// Why a node published a NodeStatus.
enum class StatusEvent : std::uint8_t {
  kStateChanged,   ///< `state` is the new state.
  kStepCompleted,  ///< A triggered step finished; `code` is its outcome.
  kError,          ///< Something failed; `code` and `detail` describe it.
  kStatusReply,    ///< Answer to QueryStatus, or acknowledgement of a no-op command.
  kEndOfStream,    ///< The business logic ended the stream; the node stopped.
};

std::string_view ToString(NodeState state) noexcept;
std::string_view ToString(ControlType type) noexcept;
std::string_view ToString(StatusEvent event) noexcept;
std::ostream& operator<<(std::ostream& os, NodeState state);
std::ostream& operator<<(std::ostream& os, ControlType type);
std::ostream& operator<<(std::ostream& os, StatusEvent event);

/// Message sent to a node's control socket.
struct ControlCommand {
  ControlType type = ControlType::kQueryStatus;
  /// Node name the command is meant for; empty addresses every node on the control socket.
  std::string target{};
  /// Echoed in the NodeStatus that answers this command, for correlation.
  std::uint64_t sequence = 0;
};

/// Message a node publishes on its status socket.
struct NodeStatus {
  std::string node{};
  NodeState state = NodeState::kCreated;
  StatusEvent event = StatusEvent::kStatusReply;
  StatusCode code = StatusCode::kOk;
  std::string detail{};
  /// Sequence of the command that caused this report; 0 if none did.
  std::uint64_t sequence = 0;
  /// Business logic invocations so far.
  std::uint64_t invocations = 0;
  /// Errors reported so far.
  std::uint64_t errors = 0;
};

/// Registers codecs for ControlCommand and NodeStatus, so control and status sockets work over
/// ipc and tcp. Idempotent.
Status RegisterControlCodecs(CodecRegistry& registry);

}  // namespace mloom
