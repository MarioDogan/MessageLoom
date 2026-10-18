#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string_view>
#include <vector>

#include "mloom/node/logic_handler.hpp"
#include "mloom/node/node_loop.hpp"

namespace mloom {

struct FreeRunningSourceOptions {
  /// Minimum time between two Produce() calls; zero produces as fast as the source returns.
  std::chrono::nanoseconds period{0};
};

/// Runs a source without external triggering: it produces continuously while Running, checking
/// the control socket (without blocking on it) before every Produce(). The node has no inputs.
/// A source that blocks inside Produce() delays the reaction to control commands accordingly.
class FreeRunningSourceLoop final : public NodeLoop {
 public:
  FreeRunningSourceLoop(std::shared_ptr<SourceHandlerBase> source, std::vector<std::size_t> outputs,
                        FreeRunningSourceOptions options = {});

  std::string_view name() const noexcept override { return "FreeRunningSourceLoop"; }
  bool triggered() const noexcept override { return false; }
  Status Prepare(const SocketManager& sockets) override;
  Status Run(LoopContext& context) override;

 private:
  std::shared_ptr<SourceHandlerBase> source_;
  std::vector<std::size_t> outputs_;
  FreeRunningSourceOptions options_;
};

}  // namespace mloom
