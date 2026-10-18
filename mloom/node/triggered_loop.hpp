#pragma once

#include <string_view>
#include <vector>

#include "mloom/core/timeout.hpp"
#include "mloom/node/node_loop.hpp"

namespace mloom {

struct TriggeredLoopOptions {
  /// How long a triggered step waits for input before it completes with Timeout. Negative
  /// values wait until input arrives (or the node is paused or stopped).
  Timeout input_timeout = kWaitForever;
};

/// Processing on command: blocks on the control socket until a Trigger arrives, then waits for
/// input and runs one message of every ready input through its routes. Each step is
/// acknowledged with a StepCompleted status carrying the trigger's sequence. Triggers that
/// arrive during a step are queued. Needs a control socket.
class TriggeredLoop final : public RoutingLoop {
 public:
  explicit TriggeredLoop(std::vector<Route> routes, TriggeredLoopOptions options = {});

  std::string_view name() const noexcept override { return "TriggeredLoop"; }
  bool triggered() const noexcept override { return true; }
  Status Prepare(const SocketManager& sockets) override;
  Status Run(LoopContext& context) override;

 private:
  TriggeredLoopOptions options_;
  Readiness ready_;
};

}  // namespace mloom
