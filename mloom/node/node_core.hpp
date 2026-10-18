#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stop_token>
#include <string_view>
#include <vector>

#include "mloom/core/status.hpp"
#include "mloom/core/timeout.hpp"
#include "mloom/node/backpressure.hpp"
#include "mloom/node/control.hpp"
#include "mloom/node/node_loop.hpp"
#include "mloom/node/socket_manager.hpp"

namespace mloom {

struct NodeCoreOptions {
  /// Upper bound of every blocking wait; stop requests are noticed within this time.
  Timeout poll_interval{20};
  /// Start Paused and run business logic only after a Start command.
  bool start_paused = false;
  /// Startup barrier: minimum subscribers per output (by index) before the loop starts, so that
  /// the first messages are not lost to joining subscribers. Missing entries mean zero.
  std::vector<std::size_t> min_output_subscribers{};
  /// Startup barrier for the status socket.
  std::size_t min_status_subscribers = 0;
  /// How long the startup barrier may take before Run() fails with Timeout.
  Timeout startup_timeout{5000};
  /// Data outputs: how long publishing a message may wait for room at a slow consumer
  /// (backpressure) before `on_backpressure` decides about it. Negative waits forever.
  Timeout send_timeout{60000};
  /// Decides about a message that got no room within send_timeout. Without a handler the
  /// message is dropped and reported as an error.
  BackpressureHandler on_backpressure{};
};

/// Runtime engine of a node. Owns the injected SocketManager and NodeLoop, runs the loop,
/// applies control commands and reports on the status socket.
class NodeCore {
 public:
  NodeCore(std::unique_ptr<SocketManager> sockets, std::unique_ptr<NodeLoop> loop,
           NodeCoreOptions options = {});
  ~NodeCore();
  NodeCore(const NodeCore&) = delete;
  NodeCore& operator=(const NodeCore&) = delete;

  /// Opens the sockets, runs the loop on the calling thread and closes the sockets again.
  /// Returns Ok after a stop request, EndOfStream when the logic ended the stream, and the
  /// failure when startup or the transport failed.
  Status Run(std::string_view node_name, std::stop_token stop_token);

  /// Thread-safe observers.
  NodeState state() const noexcept { return state_.load(std::memory_order_acquire); }
  std::uint64_t invocations() const noexcept { return invocations_.load(); }
  std::uint64_t errors() const noexcept { return errors_.load(); }

  const SocketManager& sockets() const noexcept { return *sockets_; }
  const NodeLoop& loop() const noexcept { return *loop_; }
  const NodeCoreOptions& options() const noexcept { return options_; }

 private:
  class Context;

  Status AwaitSubscribers(const Context& context);

  std::unique_ptr<SocketManager> sockets_;
  std::unique_ptr<NodeLoop> loop_;
  NodeCoreOptions options_;
  std::atomic<NodeState> state_{NodeState::kCreated};
  std::atomic<std::uint64_t> invocations_{0};
  std::atomic<std::uint64_t> errors_{0};
  std::atomic<bool> running_{false};
};

}  // namespace mloom
