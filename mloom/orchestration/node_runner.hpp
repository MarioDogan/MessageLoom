#pragma once

#include <memory>
#include <string>
#include <vector>

#include "mloom/core/status.hpp"
#include "mloom/node/node.hpp"

namespace mloom {

/// Final outcome of a node's Run().
struct NodeResult {
  std::string node{};
  Status status{};
};

/// Executes nodes. Implementations decide where a node runs: a thread, a pool, a process.
class NodeRunner {
 public:
  virtual ~NodeRunner() = default;

  /// Starts running `node`; the runner keeps it until it finished.
  virtual Status Launch(std::unique_ptr<Node> node) = 0;

  /// Asks every running node to stop.
  virtual void RequestStop() = 0;

  /// Waits until every launched node finished and returns the results in launch order.
  virtual std::vector<NodeResult> Wait() = 0;
};

}  // namespace mloom
