#pragma once

#include <cstddef>
#include <memory>
#include <string_view>
#include <vector>

#include "mloom/node/logic_handler.hpp"
#include "mloom/node/node_loop.hpp"

namespace mloom {

/// Runs a source on command: blocks on the control socket and calls Produce() once per Trigger,
/// acknowledging every step with a StepCompleted status. Needs a control socket and no inputs.
class TriggeredSourceLoop final : public NodeLoop {
 public:
  TriggeredSourceLoop(std::shared_ptr<SourceHandlerBase> source, std::vector<std::size_t> outputs);

  std::string_view name() const noexcept override { return "TriggeredSourceLoop"; }
  bool triggered() const noexcept override { return true; }
  Status Prepare(const SocketManager& sockets) override;
  Status Run(LoopContext& context) override;

 private:
  std::shared_ptr<SourceHandlerBase> source_;
  std::vector<std::size_t> outputs_;
};

}  // namespace mloom
