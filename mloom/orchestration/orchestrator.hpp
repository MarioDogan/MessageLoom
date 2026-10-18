#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mloom/core/status.hpp"
#include "mloom/core/timeout.hpp"
#include "mloom/node/control.hpp"
#include "mloom/orchestration/stage.hpp"
#include "mloom/transport/message_queue.hpp"

namespace mloom {

/// Drives the nodes of an orchestrated Stage through their control and status sockets: starts,
/// pauses and stops them, triggers steps and tracks every node's latest status.
///
/// Create it before or right after launching the stage's nodes: nodes wait (up to their startup
/// timeout) for the orchestrator's status subscription. Not thread-safe.
class Orchestrator {
 public:
  static std::pair<std::unique_ptr<Orchestrator>, Status> Create(
      const std::shared_ptr<MessageQueue>& queue, const Stage& stage);

  /// Waits until every node listens for commands and has announced its initial state.
  Status AwaitReady(Timeout timeout);

  /// Sends a command to `node`, or to every node when empty. Returns the command's sequence.
  std::pair<std::uint64_t, Status> Send(ControlType type, std::string_view node = {});

  /// Start / Pause / Stop every node (or one) and wait until the state is reached.
  Status Start(Timeout timeout, std::string_view node = {});
  Status Pause(Timeout timeout, std::string_view node = {});
  Status Stop(Timeout timeout, std::string_view node = {});

  /// Sends Finish to `nodes` one after another, e.g. in Stage::TopologicalOrder(), and waits
  /// until each one has processed its queued input and stopped before finishing the next. In
  /// that order every node drains what its producers sent, so nothing is lost. Nodes that
  /// already stopped are skipped. A node that fails while finishing, e.g. because its drain
  /// timed out, ends Finish with its status; the nodes after it keep running.
  Status Finish(const std::vector<std::string>& nodes, Timeout timeout);

  /// Triggers `node` and waits for its StepCompleted. Returns the step's outcome, or the
  /// node's error when it rejected the trigger.
  Status Step(std::string_view node, Timeout timeout);

  /// Steps `nodes` one after another, e.g. Stage::TopologicalOrder() of the triggered nodes.
  /// Stops at the first step that is not Ok or NeedMoreData and returns its status.
  Status StepAll(const std::vector<std::string>& nodes, Timeout timeout);

  /// Waits for the next status report that satisfies `match`; every report seen on the way
  /// updates the snapshot.
  std::optional<NodeStatus> AwaitReport(const std::function<bool(const NodeStatus&)>& match,
                                        Timeout timeout);

  Status AwaitState(std::string_view node, NodeState state, Timeout timeout);

  /// Applies pending status reports without blocking and returns the latest report per node.
  const std::map<std::string, NodeStatus>& Snapshot();

  /// Error reports received so far; clears the list.
  std::vector<NodeStatus> TakeErrors();

  const std::vector<std::string>& nodes() const noexcept { return nodes_; }

 private:
  Orchestrator(std::vector<std::string> nodes, std::unique_ptr<PubSocket> control,
               std::unique_ptr<SubSocket> status);

  void Record(const NodeStatus& report);
  bool InState(std::string_view node, NodeState state) const;
  Status SendAndAwait(ControlType type, std::string_view node, NodeState state, Timeout timeout);

  std::vector<std::string> nodes_;
  std::unique_ptr<PubSocket> control_;
  std::unique_ptr<SubSocket> status_;
  std::map<std::string, NodeStatus> latest_;
  std::vector<NodeStatus> errors_;
  std::uint64_t next_sequence_ = 1;
};

}  // namespace mloom
