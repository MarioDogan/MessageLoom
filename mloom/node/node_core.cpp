#include "mloom/node/node_core.hpp"

#include <algorithm>
#include <chrono>
#include <functional>
#include <string>

namespace mloom {

class NodeCore::Context final : public LoopContext {
 public:
  Context(NodeCore& core, std::string_view name, std::stop_token stop_token)
      : core_(core), name_(name), stop_token_(std::move(stop_token)) {}

  SocketManager& sockets() noexcept override { return *core_.sockets_; }
  NodeState state() const noexcept override { return core_.state(); }
  bool stop_requested() const noexcept override {
    return stop_command_ || finish_command_ || stop_token_.stop_requested();
  }
  bool finishing() const noexcept override {
    return finish_command_ && !stop_command_ && !stop_token_.stop_requested();
  }
  Timeout poll_interval() const noexcept override { return core_.options_.poll_interval; }
  Timeout send_timeout() const noexcept override { return core_.options_.send_timeout; }

  BackpressureAction OnBackpressure(BackpressureEvent event) override {
    if (!core_.options_.on_backpressure) return BackpressureAction::kReportError;
    event.node = name_;
    return core_.options_.on_backpressure(event);
  }

  bool ApplyControl(const ControlCommand& command) override {
    if (!command.target.empty() && command.target != name_) return false;
    switch (command.type) {
      case ControlType::kStart:
        SwitchState(NodeState::kPaused, NodeState::kRunning, command.sequence);
        return false;
      case ControlType::kPause:
        SwitchState(NodeState::kRunning, NodeState::kPaused, command.sequence);
        return false;
      case ControlType::kStop:
        stop_command_ = true;
        stop_sequence_ = command.sequence;
        return false;
      case ControlType::kTrigger:
        if (!core_.loop_->triggered()) {
          ReportError(Status::Unsupported(std::string(core_.loop_->name()) +
                                          " is not triggered; Trigger ignored"),
                      command.sequence);
          return false;
        }
        if (state() != NodeState::kRunning) {
          ReportError(
              Status::InvalidArgument("Trigger ignored while " + std::string(ToString(state()))),
              command.sequence);
          return false;
        }
        return true;
      case ControlType::kQueryStatus:
        Report(StatusEvent::kStatusReply, Status::Ok(), command.sequence);
        return false;
      case ControlType::kFinish:
        finish_command_ = true;
        stop_sequence_ = command.sequence;
        return false;
    }
    ReportError(Status::InvalidArgument("unknown control command"), command.sequence);
    return false;
  }

  void CountInvocation() noexcept override { core_.invocations_.fetch_add(1); }

  void ReportError(const Status& status) override { ReportError(status, 0); }

  void ReportStepCompleted(std::uint64_t sequence, const Status& status) override {
    Report(StatusEvent::kStepCompleted, status, sequence);
  }

  void ReportError(const Status& status, std::uint64_t sequence) {
    core_.errors_.fetch_add(1);
    Report(StatusEvent::kError, status, sequence);
  }

  void SetState(NodeState state, StatusEvent event, const Status& status, std::uint64_t sequence) {
    core_.state_.store(state, std::memory_order_release);
    Report(event, status, sequence);
  }

  void Report(StatusEvent event, const Status& status, std::uint64_t sequence) {
    SocketManager& manager = sockets();
    if (!manager.is_open() || !manager.has_status()) return;
    NodeStatus report{name_,
                      state(),
                      event,
                      status.code(),
                      status.message(),
                      sequence,
                      core_.invocations(),
                      core_.errors()};
    // A failing status socket cannot report its own failure; the next report retries.
    (void)manager.PublishStatus(report);
  }

  const std::string& name() const noexcept { return name_; }
  std::uint64_t stop_sequence() const noexcept { return stop_sequence_; }

 private:
  /// Moves `from` -> `to`; any other state only acknowledges the command.
  void SwitchState(NodeState from, NodeState to, std::uint64_t sequence) {
    if (state() == from) {
      SetState(to, StatusEvent::kStateChanged, Status::Ok(), sequence);
    } else {
      Report(StatusEvent::kStatusReply, Status::Ok(), sequence);
    }
  }

  NodeCore& core_;
  std::string name_;
  std::stop_token stop_token_;
  bool stop_command_ = false;
  bool finish_command_ = false;
  std::uint64_t stop_sequence_ = 0;
};

NodeCore::NodeCore(std::unique_ptr<SocketManager> sockets, std::unique_ptr<NodeLoop> loop,
                   NodeCoreOptions options)
    : sockets_(std::move(sockets)), loop_(std::move(loop)), options_(std::move(options)) {}

NodeCore::~NodeCore() = default;

Status NodeCore::Run(std::string_view node_name, std::stop_token stop_token) {
  if (!sockets_ || !loop_) {
    return Status::InvalidArgument("a node core needs a socket manager and a node loop");
  }
  if (running_.exchange(true)) return Status::InvalidArgument("node is already running");
  struct RunningGuard {
    std::atomic<bool>& running;
    ~RunningGuard() { running.store(false); }
  } guard{running_};

  Context context(*this, node_name, std::move(stop_token));
  Status status = sockets_->Open();
  if (status.ok()) status = loop_->Prepare(*sockets_);
  if (status.ok()) status = AwaitSubscribers(context);
  if (!status.ok()) {
    context.SetState(NodeState::kFailed, StatusEvent::kError, status, 0);
    sockets_->Close();
    return status;
  }

  context.SetState(options_.start_paused ? NodeState::kPaused : NodeState::kRunning,
                   StatusEvent::kStateChanged, Status::Ok(), 0);
  status = loop_->Run(context);
  if (status.ok() && context.finishing()) status = loop_->Drain(context);
  if (status.ok()) {
    context.SetState(NodeState::kStopped, StatusEvent::kStateChanged, status,
                     context.stop_sequence());
  } else if (status.code() == StatusCode::kEndOfStream) {
    context.SetState(NodeState::kStopped, StatusEvent::kEndOfStream, status, 0);
  } else {
    errors_.fetch_add(1);
    context.SetState(NodeState::kFailed, StatusEvent::kError, status, 0);
  }
  sockets_->Close();
  return status;
}

Status NodeCore::AwaitSubscribers(const Context& context) {
  using Clock = std::chrono::steady_clock;
  if (options_.min_output_subscribers.size() > sockets_->output_count()) {
    return Status::InvalidArgument("min_output_subscribers lists more outputs than the node has");
  }
  const auto deadline = Clock::now() + options_.startup_timeout;
  const auto await = [&](const std::function<Status(Timeout)>& wait,
                         const std::string& what) -> Status {
    while (!context.stop_requested()) {
      const Timeout remaining = std::chrono::ceil<Timeout>(deadline - Clock::now());
      if (remaining <= Timeout::zero()) {
        return Status::Timeout("startup barrier: " + what + " did not get its subscribers within " +
                               std::to_string(options_.startup_timeout.count()) + " ms");
      }
      // Binding inputs must be serviced to accept upstream publishers meanwhile.
      sockets_->ServiceConnections();
      Status status = wait(std::min(remaining, options_.poll_interval));
      if (status.code() != StatusCode::kTimeout) return status;
    }
    return Status::Ok();  // stopping anyway; the loop exits right away
  };

  for (std::size_t output = 0; output < options_.min_output_subscribers.size(); ++output) {
    const std::size_t count = options_.min_output_subscribers[output];
    if (count == 0) continue;
    Status status =
        await([&](Timeout timeout) { return sockets_->WaitForSubscribers(output, count, timeout); },
              "output " + std::to_string(output));
    if (!status.ok()) return status;
  }
  if (options_.min_status_subscribers > 0) {
    if (!sockets_->has_status()) {
      return Status::InvalidArgument(
          "min_status_subscribers set, but the node has no status socket");
    }
    return await(
        [&](Timeout timeout) {
          return sockets_->WaitForStatusSubscribers(options_.min_status_subscribers, timeout);
        },
        "status socket");
  }
  return Status::Ok();
}

}  // namespace mloom
