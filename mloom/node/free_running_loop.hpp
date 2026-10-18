#pragma once

#include <string_view>
#include <vector>

#include "mloom/core/timeout.hpp"
#include "mloom/node/node_loop.hpp"

namespace mloom {

struct FreeRunningLoopOptions {
  /// Finish: the data inputs count as drained once no message arrived on them for this long.
  /// Zero checks them once without waiting.
  Timeout drain_quiet{20};
  /// Finish: upper bound for draining the data inputs, so that a node whose producers keep
  /// sending cannot hang. When it passes, the node fails with Timeout. Negative waits forever.
  /// It is checked between messages: a publish that waits for a slow consumer meanwhile is
  /// bounded by NodeCoreOptions::send_timeout instead.
  Timeout drain_timeout{60000};
};

/// Event-driven processing: every input message runs the routes of its input as soon as it
/// arrives. Control commands are checked whenever the loop wakes up, but the loop never waits
/// for them while Running. While Paused, input messages stay queued.
///
/// A Finish command makes the loop process the queued input, even while Paused, until its inputs
/// stay quiet for `drain_quiet`; then it ends. Finish a node after its producers stopped:
/// otherwise input keeps arriving and the drain ends only at `drain_timeout`.
class FreeRunningLoop final : public RoutingLoop {
 public:
  explicit FreeRunningLoop(std::vector<Route> routes, FreeRunningLoopOptions options = {});

  const FreeRunningLoopOptions& options() const noexcept { return options_; }

  std::string_view name() const noexcept override { return "FreeRunningLoop"; }
  bool triggered() const noexcept override { return false; }
  Status Run(LoopContext& context) override;
  Status Drain(LoopContext& context) override;

 private:
  FreeRunningLoopOptions options_;
  Readiness ready_;
};

}  // namespace mloom
