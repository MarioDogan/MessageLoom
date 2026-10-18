#include "mloom/node/free_running_loop.hpp"

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>

namespace mloom {

FreeRunningLoop::FreeRunningLoop(std::vector<Route> routes, FreeRunningLoopOptions options)
    : RoutingLoop(std::move(routes)), options_(options) {}

Status FreeRunningLoop::Run(LoopContext& context) {
  while (!context.stop_requested()) {
    if (context.state() != NodeState::kRunning) {
      if (Status status = AwaitControl(context, context.poll_interval(), nullptr); !status.ok()) {
        return status;
      }
      continue;
    }
    Status status =
        context.sockets().Wait(WaitScope::kControlAndInputs, context.poll_interval(), ready_);
    if (status.code() == StatusCode::kTimeout) continue;
    if (!status.ok()) return status;
    if (ready_.control) {
      if (Status drained = DrainControl(context, nullptr); !drained.ok()) return drained;
    }
    if (context.stop_requested() || context.state() != NodeState::kRunning) continue;
    for (const std::size_t input : ready_.inputs) {
      if (ProcessInput(context, input).code() == StatusCode::kEndOfStream) {
        return Status::EndOfStream();
      }
      // A blocked publish applies control commands, so the state may have changed.
      if (context.stop_requested() || context.state() != NodeState::kRunning) break;
    }
  }
  return Status::Ok();
}

Status FreeRunningLoop::Drain(LoopContext& context) {
  using Clock = std::chrono::steady_clock;
  const auto until = [](Clock::time_point end) {
    return std::max(Timeout::zero(), std::chrono::ceil<Timeout>(end - Clock::now()));
  };
  std::optional<Clock::time_point> deadline;
  if (options_.drain_timeout >= Timeout::zero()) {
    deadline = Clock::now() + options_.drain_timeout;
  }
  auto quiet_end = Clock::now() + std::max(options_.drain_quiet, Timeout::zero());
  while (context.finishing()) {
    if (deadline && Clock::now() >= *deadline) {
      return Status::Timeout("drain gave up after " +
                             std::to_string(options_.drain_timeout.count()) +
                             " ms: input kept arriving");
    }
    // Wait for the quiet period, but never past the deadline and never longer than the poll
    // interval, so that the stop token is noticed in time.
    Timeout wait = std::min(context.poll_interval(), until(quiet_end));
    if (deadline) wait = std::min(wait, until(*deadline));
    Status status = context.sockets().Wait(WaitScope::kControlAndInputs, wait, ready_);
    if (status.code() == StatusCode::kTimeout) {
      if (Clock::now() >= quiet_end) break;  // quiet: everything is processed
      continue;
    }
    if (!status.ok()) return status;
    if (ready_.control) {
      if (Status drained = DrainControl(context, nullptr); !drained.ok()) return drained;
    }
    if (!context.finishing()) break;  // a Stop cuts the drain short
    for (const std::size_t input : ready_.inputs) {
      if (ProcessInput(context, input).code() == StatusCode::kEndOfStream) {
        return Status::EndOfStream();
      }
      if (!context.finishing()) break;
    }
    if (!ready_.inputs.empty()) {
      quiet_end = Clock::now() + std::max(options_.drain_quiet, Timeout::zero());
    }
  }
  return Status::Ok();
}

}  // namespace mloom
