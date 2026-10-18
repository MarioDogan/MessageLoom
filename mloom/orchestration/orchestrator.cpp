#include "mloom/orchestration/orchestrator.hpp"

#include <algorithm>
#include <chrono>

namespace mloom {
namespace {

/// Converts a Timeout (negative = forever) into a fixed point in time.
class Deadline {
 public:
  using Clock = std::chrono::steady_clock;

  explicit Deadline(Timeout timeout)
      : forever_(timeout < Timeout::zero()),
        at_(Clock::now() + std::max(timeout, Timeout::zero())) {}

  Timeout remaining() const {
    if (forever_) return kWaitForever;
    return std::max(Timeout::zero(), std::chrono::ceil<Timeout>(at_ - Clock::now()));
  }
  bool expired() const { return !forever_ && Clock::now() >= at_; }

 private:
  bool forever_;
  Clock::time_point at_;
};

bool IsFinal(NodeState state) {
  return state == NodeState::kStopped || state == NodeState::kFailed;
}

/// Commands after which a node that is no longer running counts as done.
bool EndsTheNode(ControlType type) {
  return type == ControlType::kStop || type == ControlType::kFinish;
}

}  // namespace

std::pair<std::unique_ptr<Orchestrator>, Status> Orchestrator::Create(
    const std::shared_ptr<MessageQueue>& queue, const Stage& stage) {
  if (!queue) return {nullptr, Status::InvalidArgument("orchestrator needs a message queue")};
  if (!stage.options().orchestrated) {
    return {nullptr, Status::InvalidArgument("stage '" + stage.name() + "' is not orchestrated")};
  }
  if (Status status = stage.Validate(); !status.ok()) return {nullptr, status};
  if (Status status = RegisterControlCodecs(queue->codecs()); !status.ok()) {
    return {nullptr, status};
  }
  auto [control, control_status] = queue->CreatePublisher(stage.ControlEndpoint());
  if (!control_status.ok()) return {nullptr, control_status};
  std::string status_endpoints;
  for (const std::string& node : stage.node_names()) {
    if (!status_endpoints.empty()) status_endpoints += ",";
    status_endpoints += stage.StatusEndpoint(node);
  }
  auto [status, status_status] = queue->CreateSubscriber(status_endpoints);
  if (!status_status.ok()) return {nullptr, status_status};
  return {std::unique_ptr<Orchestrator>(
              new Orchestrator(stage.node_names(), std::move(control), std::move(status))),
          Status::Ok()};
}

Orchestrator::Orchestrator(std::vector<std::string> nodes, std::unique_ptr<PubSocket> control,
                           std::unique_ptr<SubSocket> status)
    : nodes_(std::move(nodes)), control_(std::move(control)), status_(std::move(status)) {}

void Orchestrator::Record(const NodeStatus& report) {
  latest_[report.node] = report;
  if (report.event == StatusEvent::kError) errors_.push_back(report);
}

std::optional<NodeStatus> Orchestrator::AwaitReport(
    const std::function<bool(const NodeStatus&)>& match, Timeout timeout) {
  const Deadline deadline(timeout);
  while (true) {
    auto [message, status] = status_->Receive(deadline.remaining());
    if (status.ok()) {
      if (const NodeStatus* report = message->As<NodeStatus>()) {
        Record(*report);
        if (match(*report)) return *report;
      }
    } else if (status.code() == StatusCode::kTimeout || deadline.expired()) {
      return std::nullopt;
    }
  }
}

const std::map<std::string, NodeStatus>& Orchestrator::Snapshot() {
  (void)AwaitReport([](const NodeStatus&) { return false; }, kNoWait);
  return latest_;
}

bool Orchestrator::InState(std::string_view node, NodeState state) const {
  const auto it = latest_.find(std::string(node));
  return it != latest_.end() && it->second.state == state;
}

Status Orchestrator::AwaitReady(Timeout timeout) {
  const Deadline deadline(timeout);
  const auto announced = [&] {
    return std::all_of(nodes_.begin(), nodes_.end(), [&](const std::string& node) {
      const auto it = latest_.find(node);
      return it != latest_.end() && it->second.state != NodeState::kCreated;
    });
  };
  while (!announced()) {
    if (!AwaitReport([](const NodeStatus&) { return true; }, deadline.remaining())) {
      return Status::Timeout("not every node announced its state");
    }
  }

  // Every node that still runs must listen for commands. A node may already have finished
  // (e.g. a source at EndOfStream) and withdrawn its subscription, so the count is re-derived
  // from fresh states between short waits.
  constexpr Timeout kSlice{10};
  while (true) {
    (void)Snapshot();
    const auto live = static_cast<std::size_t>(
        std::count_if(latest_.begin(), latest_.end(),
                      [](const auto& entry) { return !IsFinal(entry.second.state); }));
    const Timeout remaining = deadline.remaining();
    Status status = control_->WaitForSubscribers(
        live, remaining < Timeout::zero() ? kSlice : std::min(kSlice, remaining));
    if (status.ok()) break;
    if (status.code() != StatusCode::kTimeout) return status;
    if (deadline.expired()) {
      return Status(status.code(), "nodes listening for commands: " + status.message());
    }
  }

  for (const auto& [node, report] : latest_) {
    if (report.state == NodeState::kFailed) {
      return Status(report.code, "node '" + node + "' failed: " + report.detail);
    }
  }
  return Status::Ok();
}

std::pair<std::uint64_t, Status> Orchestrator::Send(ControlType type, std::string_view node) {
  const std::uint64_t sequence = next_sequence_++;
  Status status = control_->Send(MakeMessage(ControlCommand{type, std::string(node), sequence}));
  return {sequence, std::move(status)};
}

Status Orchestrator::SendAndAwait(ControlType type, std::string_view node, NodeState state,
                                  Timeout timeout) {
  const Deadline deadline(timeout);
  (void)Snapshot();  // act on fresh states
  auto [sequence, status] = Send(type, node);
  if (!status.ok()) return status;
  const std::vector<std::string> targets =
      node.empty() ? nodes_ : std::vector<std::string>{std::string(node)};
  for (const std::string& target : targets) {
    if (InState(target, state)) continue;
    const auto it = latest_.find(target);
    if (it != latest_.end() && IsFinal(it->second.state)) {
      if (EndsTheNode(type)) continue;  // not running anymore anyway
      return Status::Closed("node '" + target + "' is " + std::string(ToString(it->second.state)));
    }
    // The answer to this command carries its sequence; a final state ends the wait as well.
    const auto report = AwaitReport(
        [&](const NodeStatus& r) {
          return r.node == target &&
                 (r.sequence == sequence || r.state == state || IsFinal(r.state));
        },
        deadline.remaining());
    if (!report) {
      return Status::Timeout("node '" + target + "' did not reach " + std::string(ToString(state)));
    }
    if (type == ControlType::kFinish && report->state == NodeState::kFailed) {
      return Status(report->code,
                    "node '" + target + "' failed while finishing: " + report->detail);
    }
    if (report->state != state && !(EndsTheNode(type) && IsFinal(report->state))) {
      return Status::Error("node '" + target + "' is " + std::string(ToString(report->state)) +
                           " instead of " + std::string(ToString(state)));
    }
  }
  return Status::Ok();
}

Status Orchestrator::Start(Timeout timeout, std::string_view node) {
  return SendAndAwait(ControlType::kStart, node, NodeState::kRunning, timeout);
}

Status Orchestrator::Pause(Timeout timeout, std::string_view node) {
  return SendAndAwait(ControlType::kPause, node, NodeState::kPaused, timeout);
}

Status Orchestrator::Stop(Timeout timeout, std::string_view node) {
  return SendAndAwait(ControlType::kStop, node, NodeState::kStopped, timeout);
}

Status Orchestrator::Finish(const std::vector<std::string>& nodes, Timeout timeout) {
  for (const std::string& node : nodes) {
    if (node.empty()) return Status::InvalidArgument("Finish needs node names");
    if (Status status = SendAndAwait(ControlType::kFinish, node, NodeState::kStopped, timeout);
        !status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

Status Orchestrator::Step(std::string_view node, Timeout timeout) {
  if (node.empty()) return Status::InvalidArgument("Step needs a node name");
  auto [sequence, status] = Send(ControlType::kTrigger, node);
  if (!status.ok()) return status;
  const auto report = AwaitReport(
      [&](const NodeStatus& r) {
        return r.node == node && r.sequence == sequence &&
               (r.event == StatusEvent::kStepCompleted || r.event == StatusEvent::kError);
      },
      timeout);
  if (!report) {
    return Status::Timeout("node '" + std::string(node) + "' did not finish its step");
  }
  return Status(report->code, report->detail);
}

Status Orchestrator::StepAll(const std::vector<std::string>& nodes, Timeout timeout) {
  for (const std::string& node : nodes) {
    Status status = Step(node, timeout);
    if (!status.ok() && status.code() != StatusCode::kNeedMoreData) return status;
  }
  return Status::Ok();
}

Status Orchestrator::AwaitState(std::string_view node, NodeState state, Timeout timeout) {
  (void)Snapshot();
  if (InState(node, state)) return Status::Ok();
  const auto report =
      AwaitReport([&](const NodeStatus& r) { return r.node == node && r.state == state; }, timeout);
  return report ? Status::Ok()
                : Status::Timeout("node '" + std::string(node) + "' did not reach " +
                                  std::string(ToString(state)));
}

std::vector<NodeStatus> Orchestrator::TakeErrors() {
  (void)Snapshot();
  return std::exchange(errors_, {});
}

}  // namespace mloom
