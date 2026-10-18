#pragma once

// Test support: a LoopContext that records what a node loop reports instead of publishing it, so
// loops and the NodeLoop helpers can run on the test thread without a NodeCore.

#include <cstdint>
#include <utility>
#include <vector>

#include "mloom/core/status.hpp"
#include "mloom/core/timeout.hpp"
#include "mloom/node/control.hpp"
#include "mloom/node/node_loop.hpp"
#include "mloom/node/socket_manager.hpp"

namespace mloom::testing {

/// ApplyControl mimics NodeCore without target filtering: Start and Pause switch the state, Stop
/// and Finish request a stop (Finish one that drains) and a Trigger is accepted while Running.
class FakeLoopContext final : public LoopContext {
 public:
  explicit FakeLoopContext(SocketManager& sockets) : sockets_(sockets) {}

  SocketManager& sockets() noexcept override { return sockets_; }
  NodeState state() const noexcept override { return current_state; }
  bool stop_requested() const noexcept override { return stop || finish; }
  bool finishing() const noexcept override { return finish && !stop; }
  Timeout poll_interval() const noexcept override { return interval; }
  Timeout send_timeout() const noexcept override { return send_limit; }

  BackpressureAction OnBackpressure(BackpressureEvent event) override {
    backpressure.push_back(event);
    return on_backpressure ? on_backpressure(event) : BackpressureAction::kReportError;
  }

  bool ApplyControl(const ControlCommand& command) override {
    commands.push_back(command);
    switch (command.type) {
      case ControlType::kStart:
        current_state = NodeState::kRunning;
        return false;
      case ControlType::kPause:
        current_state = NodeState::kPaused;
        return false;
      case ControlType::kStop:
        stop = true;
        return false;
      case ControlType::kTrigger:
        return current_state == NodeState::kRunning;
      case ControlType::kQueryStatus:
        return false;
      case ControlType::kFinish:
        finish = true;
        return false;
    }
    return false;
  }

  void CountInvocation() noexcept override { ++invocations; }
  void ReportError(const Status& status) override { errors.push_back(status); }
  void ReportStepCompleted(std::uint64_t sequence, const Status& status) override {
    steps.emplace_back(sequence, status);
  }

  // Set by the test.
  NodeState current_state = NodeState::kRunning;
  bool stop = false;    // as the stop token or a Stop command
  bool finish = false;  // as a Finish command
  Timeout interval{10};
  Timeout send_limit{60000};
  BackpressureHandler on_backpressure{};

  // Recorded from the loop.
  std::vector<ControlCommand> commands;
  std::uint64_t invocations = 0;
  std::vector<Status> errors;
  std::vector<std::pair<std::uint64_t, Status>> steps;
  std::vector<BackpressureEvent> backpressure;

 private:
  SocketManager& sockets_;
};

}  // namespace mloom::testing
