#pragma once

// Test support: runs one node on its own thread, with test-side sockets wired to the node's
// inputs, outputs, control and status sockets.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mloom/node/node.hpp"
#include "mloom/transport/message_queue.hpp"
#include "mloom/transport/zmq/zmq_message_queue.hpp"

namespace mloom::testing {

inline constexpr Timeout kTestTimeout{5000};

/// Process-unique inproc address.
std::string UniqueInproc(const std::string& base);

struct HarnessConfig {
  std::size_t inputs = 1;
  std::size_t outputs = 1;
  bool control = true;
  bool status = true;
  /// Node inputs bind and the harness publishers connect once the node runs, which needs the
  /// node to service its inputs.
  bool bind_inputs = false;
};

class NodeHarness {
 public:
  explicit NodeHarness(HarnessConfig config = {}, std::shared_ptr<MessageQueue> queue =
                                                      std::make_shared<zeromq::ZmqMessageQueue>());
  ~NodeHarness();

  /// Node-side layout matching the test-side sockets.
  const SocketLayout& layout() const noexcept { return layout_; }

  /// Options with a startup barrier on every output and the status socket, so no output or
  /// status message is lost.
  NodeCoreOptions BarrierOptions() const;

  /// Runs `name` with `loop` on a new thread and waits until the node announced its initial
  /// state and every test-side publisher sees the node.
  Status Start(std::unique_ptr<NodeLoop> loop, NodeCoreOptions options, std::string name = "node");
  /// Starts without waiting, for nodes that are expected to fail during startup.
  void Launch(std::unique_ptr<NodeLoop> loop, NodeCoreOptions options, std::string name = "node");

  /// Waits until the control and input publishers see a subscriber each. Start() does this;
  /// tests that open a SocketManager on layout() themselves call it after Open().
  Status AwaitSubscribers();

  Status Send(std::size_t input, MessagePtr message);
  std::pair<MessagePtr, Status> Receive(std::size_t output, Timeout timeout = kTestTimeout);
  Status Command(ControlType type, std::uint64_t sequence = 0, std::string target = {});
  /// Publishes any message on the control socket.
  Status SendControl(MessagePtr message);

  /// Next status report matching `match`; reports that do not match are discarded.
  std::optional<NodeStatus> AwaitStatus(const std::function<bool(const NodeStatus&)>& match,
                                        Timeout timeout = kTestTimeout);
  std::optional<NodeStatus> AwaitEvent(StatusEvent event, Timeout timeout = kTestTimeout);

  /// Requests a stop through the stop token and waits for Run() to return its status.
  Status Stop();
  /// Waits for Run() to return on its own.
  Status Join();

  const Node& node() const { return *node_; }
  MessageQueue& queue() { return *queue_; }

 private:
  std::shared_ptr<MessageQueue> queue_;
  HarnessConfig config_;
  SocketLayout layout_;
  std::vector<std::string> input_addresses_;
  std::vector<std::unique_ptr<PubSocket>> inputs_;
  std::vector<std::unique_ptr<SubSocket>> outputs_;
  std::unique_ptr<PubSocket> control_;
  std::unique_ptr<SubSocket> status_;
  std::unique_ptr<Node> node_;
  std::optional<Status> result_;
  std::jthread thread_;
};

/// A SocketManager on a NodeHarness layout, for tests that use a node's sockets directly instead
/// of running a node; the harness's sockets are the peers. Does not support bind_inputs.
struct SocketRig {
  explicit SocketRig(HarnessConfig config = {}, zeromq::ZmqOptions options = {});

  /// Opens the manager and waits until every peer is connected.
  Status Open();
  /// Waits until the manager reports `input` readable.
  Status AwaitInput(std::size_t input);

  std::shared_ptr<MessageQueue> queue;
  NodeHarness harness;
  SocketManager sockets;
};

}  // namespace mloom::testing
