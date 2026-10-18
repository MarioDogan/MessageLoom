#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mloom/core/status.hpp"
#include "mloom/core/timeout.hpp"
#include "mloom/node/node.hpp"
#include "mloom/node/node_core.hpp"
#include "mloom/node/node_loop.hpp"
#include "mloom/transport/message_queue.hpp"

namespace mloom {

/// Declares one node of a stage: the channels its sockets read and write, and its loop.
struct NodeSpec {
  std::string name{};
  std::vector<std::string> inputs{};   ///< Channel of each input socket, by index.
  std::vector<std::string> outputs{};  ///< Channel of each output socket, by index.
  /// Builds the node's loop; route indices refer to `inputs` and `outputs`.
  std::function<std::unique_ptr<NodeLoop>()> loop{};
  /// Node options. Stage::Build fills in the startup barrier and start_paused.
  NodeCoreOptions options{};
};

struct StageOptions {
  /// Give every node a control and a status socket, and make nodes wait for the orchestrator's
  /// status subscription before they start. Without it the stage is purely event-driven and
  /// can only be stopped through the runner.
  bool orchestrated = true;
  /// Start every node Paused, for a coordinated Orchestrator::Start().
  bool start_paused = false;
  /// Startup barrier timeout of every node.
  Timeout startup_timeout{5000};
};

/// Staging: describes how the nodes of a deployment connect through named channels and builds
/// the nodes accordingly.
///
/// Endpoints are generated, nobody configures addresses: each producer of a channel binds its
/// own endpoint and every consumer connects to all producers of the channel, so channels may
/// fan in and fan out. Every node waits until all consumers of its outputs (and the
/// orchestrator) subscribed before it runs, so no message is lost at startup. All endpoints
/// are inproc: the stage runs inside one process on one shared MessageQueue.
class Stage {
 public:
  explicit Stage(std::string name, StageOptions options = {});

  /// Adds a node. Names must be unique; names and channels must not contain ',' or blanks.
  Status Add(NodeSpec spec);

  const std::string& name() const noexcept { return name_; }
  const StageOptions& options() const noexcept { return options_; }
  std::vector<std::string> node_names() const;

  /// Checks that every consumed channel has a producer and that channels are unique per node.
  Status Validate() const;

  /// Node names with producers before their consumers; nodes in cycles keep declaration order.
  std::vector<std::string> TopologicalOrder() const;

  /// Creates the nodes, ready to run, on `queue`.
  std::pair<std::vector<std::unique_ptr<Node>>, Status> Build(
      const std::shared_ptr<MessageQueue>& queue) const;

  /// The orchestrator binds this endpoint; every node's control socket connects to it.
  std::string ControlEndpoint() const;
  /// Each node binds its own status endpoint; the orchestrator connects to all of them.
  std::string StatusEndpoint(std::string_view node) const;
  /// Endpoint `node` binds for its output `channel`.
  std::string ChannelEndpoint(std::string_view node, std::string_view channel) const;

 private:
  std::vector<const NodeSpec*> Producers(std::string_view channel) const;
  std::size_t Consumers(std::string_view channel) const;

  std::string name_;
  StageOptions options_;
  std::vector<NodeSpec> nodes_;
};

}  // namespace mloom
