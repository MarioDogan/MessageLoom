#pragma once

#include <memory>
#include <stop_token>
#include <string>

#include "mloom/core/status.hpp"
#include "mloom/node/control.hpp"
#include "mloom/node/node_core.hpp"

namespace mloom {

/// Deployment unit: a named NodeCore. The node has in/out data sockets plus one control and one
/// status socket, all owned by the core's SocketManager. Run() executes the node on the calling
/// thread; node runners decide which thread that is.
class Node {
 public:
  Node(std::string name, std::unique_ptr<NodeCore> core);
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;
  Node(Node&&) noexcept = default;
  Node& operator=(Node&&) noexcept = default;

  const std::string& name() const noexcept { return name_; }

  /// Thread-safe.
  NodeState state() const noexcept { return core_ ? core_->state() : NodeState::kFailed; }

  /// Runs until `stop_token` is triggered, a Stop command arrives or the logic ends the stream.
  Status Run(std::stop_token stop_token = {});

  const NodeCore& core() const noexcept { return *core_; }

 private:
  std::string name_;
  std::unique_ptr<NodeCore> core_;
};

}  // namespace mloom
