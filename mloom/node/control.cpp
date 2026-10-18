#include "mloom/node/control.hpp"

#include <utility>

namespace mloom {
namespace {

template <typename Enum>
bool InRange(Enum value, Enum last) {
  return static_cast<std::uint8_t>(value) <= static_cast<std::uint8_t>(last);
}

}  // namespace

std::string_view ToString(NodeState state) noexcept {
  switch (state) {
    case NodeState::kCreated:
      return "Created";
    case NodeState::kRunning:
      return "Running";
    case NodeState::kPaused:
      return "Paused";
    case NodeState::kStopped:
      return "Stopped";
    case NodeState::kFailed:
      return "Failed";
  }
  return "Unknown";
}

std::string_view ToString(ControlType type) noexcept {
  switch (type) {
    case ControlType::kStart:
      return "Start";
    case ControlType::kPause:
      return "Pause";
    case ControlType::kStop:
      return "Stop";
    case ControlType::kTrigger:
      return "Trigger";
    case ControlType::kQueryStatus:
      return "QueryStatus";
    case ControlType::kFinish:
      return "Finish";
  }
  return "Unknown";
}

std::string_view ToString(StatusEvent event) noexcept {
  switch (event) {
    case StatusEvent::kStateChanged:
      return "StateChanged";
    case StatusEvent::kStepCompleted:
      return "StepCompleted";
    case StatusEvent::kError:
      return "Error";
    case StatusEvent::kStatusReply:
      return "StatusReply";
    case StatusEvent::kEndOfStream:
      return "EndOfStream";
  }
  return "Unknown";
}

std::ostream& operator<<(std::ostream& os, NodeState state) { return os << ToString(state); }
std::ostream& operator<<(std::ostream& os, ControlType type) { return os << ToString(type); }
std::ostream& operator<<(std::ostream& os, StatusEvent event) { return os << ToString(event); }

Status RegisterControlCodecs(CodecRegistry& registry) {
  Status status = registry.Register<ControlCommand>(
      [](const ControlCommand& command, ByteWriter& writer) {
        writer.Write(command.type);
        writer.WriteString(command.target);
        writer.Write(command.sequence);
      },
      [](ByteReader& reader) {
        ControlCommand command;
        reader.Read(command.type);
        reader.ReadString(command.target);
        reader.Read(command.sequence);
        Status valid = InRange(command.type, ControlType::kFinish)
                           ? Status::Ok()
                           : Status::InvalidArgument("unknown control command type");
        return std::pair{std::move(command), std::move(valid)};
      });
  if (!status.ok()) return status;
  return registry.Register<NodeStatus>(
      [](const NodeStatus& report, ByteWriter& writer) {
        writer.WriteString(report.node);
        writer.Write(report.state);
        writer.Write(report.event);
        writer.Write(report.code);
        writer.WriteString(report.detail);
        writer.Write(report.sequence);
        writer.Write(report.invocations);
        writer.Write(report.errors);
      },
      [](ByteReader& reader) {
        NodeStatus report;
        reader.ReadString(report.node);
        reader.Read(report.state);
        reader.Read(report.event);
        reader.Read(report.code);
        reader.ReadString(report.detail);
        reader.Read(report.sequence);
        reader.Read(report.invocations);
        reader.Read(report.errors);
        const bool valid = InRange(report.state, NodeState::kFailed) &&
                           InRange(report.event, StatusEvent::kEndOfStream) &&
                           InRange(report.code, StatusCode::kError);
        return std::pair{std::move(report),
                         valid ? Status::Ok() : Status::InvalidArgument("malformed node status")};
      });
}

}  // namespace mloom
