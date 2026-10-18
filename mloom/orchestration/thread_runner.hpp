#pragma once

#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include "mloom/orchestration/node_runner.hpp"

namespace mloom {

/// Runs every node on its own thread. Destruction stops and joins all nodes.
class ThreadRunner final : public NodeRunner {
 public:
  ThreadRunner() = default;
  ~ThreadRunner() override;
  ThreadRunner(const ThreadRunner&) = delete;
  ThreadRunner& operator=(const ThreadRunner&) = delete;

  Status Launch(std::unique_ptr<Node> node) override;
  void RequestStop() override;
  std::vector<NodeResult> Wait() override;

  /// Launches each node in order; stops at the first failure.
  Status LaunchAll(std::vector<std::unique_ptr<Node>> nodes);

 private:
  struct Slot {
    std::unique_ptr<Node> node;
    std::optional<Status> result;
    std::jthread thread;
  };

  std::vector<std::unique_ptr<Slot>> slots_;
};

}  // namespace mloom
