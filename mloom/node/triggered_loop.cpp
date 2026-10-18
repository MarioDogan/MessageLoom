#include "mloom/node/triggered_loop.hpp"

#include <algorithm>
#include <chrono>
#include <deque>
#include <optional>
#include <string>

namespace mloom {

TriggeredLoop::TriggeredLoop(std::vector<Route> routes, TriggeredLoopOptions options)
    : RoutingLoop(std::move(routes)), options_(options) {}

Status TriggeredLoop::Prepare(const SocketManager& sockets) {
  if (!sockets.has_control())
    return Status::InvalidArgument("TriggeredLoop needs a control socket");
  return RoutingLoop::Prepare(sockets);
}

Status TriggeredLoop::Run(LoopContext& context) {
  using Clock = std::chrono::steady_clock;
  std::deque<std::uint64_t> triggers;
  std::optional<Clock::time_point> deadline;  // of the step in progress
  while (!context.stop_requested()) {
    if (context.state() != NodeState::kRunning) {
      triggers.clear();
      deadline.reset();
    }
    if (triggers.empty()) {
      if (Status status = AwaitControl(context, context.poll_interval(), &triggers); !status.ok()) {
        return status;
      }
      continue;
    }

    // A step is in progress: wait for input while staying responsive to control commands.
    Timeout wait = context.poll_interval();
    if (options_.input_timeout >= Timeout::zero()) {
      if (!deadline) deadline = Clock::now() + options_.input_timeout;
      const Timeout remaining = std::chrono::ceil<Timeout>(*deadline - Clock::now());
      if (remaining <= Timeout::zero()) {
        context.ReportStepCompleted(
            triggers.front(),
            Status::Timeout("no input within " + std::to_string(options_.input_timeout.count()) +
                            " ms"));
        triggers.pop_front();
        deadline.reset();
        continue;
      }
      wait = std::min(wait, remaining);
    }
    Status status = context.sockets().Wait(WaitScope::kControlAndInputs, wait, ready_);
    if (status.code() == StatusCode::kTimeout) continue;
    if (!status.ok()) return status;
    if (ready_.control) {
      if (Status drained = DrainControl(context, &triggers); !drained.ok()) return drained;
    }
    if (context.stop_requested() || context.state() != NodeState::kRunning ||
        ready_.inputs.empty()) {
      continue;
    }

    Status step;
    for (const std::size_t input : ready_.inputs) {
      if (context.stop_requested()) break;  // applied by a blocked publish
      Status processed = ProcessInput(context, input, &triggers);
      if (processed.code() == StatusCode::kEndOfStream) {
        step = std::move(processed);
        break;
      }
      if (step.ok()) step = std::move(processed);
    }
    context.ReportStepCompleted(triggers.front(), step);
    triggers.pop_front();
    deadline.reset();
    if (step.code() == StatusCode::kEndOfStream) return Status::EndOfStream();
  }
  return Status::Ok();
}

}  // namespace mloom
