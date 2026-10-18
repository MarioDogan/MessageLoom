#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "mloom/core/message.hpp"
#include "mloom/core/status.hpp"
#include "mloom/core/timeout.hpp"
#include "mloom/node/backpressure.hpp"
#include "mloom/node/control.hpp"
#include "mloom/node/logic_handler.hpp"
#include "mloom/node/socket_manager.hpp"

namespace mloom {

/// Feeds the messages of one input socket into a logic handler and publishes the handler's
/// results on the given output sockets. Several routes may share an input.
struct Route {
  std::size_t input = 0;
  std::shared_ptr<LogicHandlerBase> handler{};
  std::vector<std::size_t> outputs{};
};

/// Services the NodeCore provides to the NodeLoop it runs.
class LoopContext {
 public:
  virtual ~LoopContext() = default;

  virtual SocketManager& sockets() noexcept = 0;
  virtual NodeState state() const noexcept = 0;

  /// True once the loop should end: through the stop token, a Stop or a Finish command.
  virtual bool stop_requested() const noexcept = 0;

  /// True while the loop ends because of a Finish command alone, so that queued input is still
  /// processed (see NodeLoop::Drain). The stop token or a Stop command ends finishing.
  virtual bool finishing() const noexcept = 0;

  /// Upper bound for every blocking wait, so that stop requests are noticed in time.
  virtual Timeout poll_interval() const noexcept = 0;

  /// How long publishing a message may wait for room at a consumer before OnBackpressure()
  /// decides about it. Negative waits forever.
  virtual Timeout send_timeout() const noexcept = 0;

  /// Decides about a message that waited send_timeout() for room; fills in the node's name.
  virtual BackpressureAction OnBackpressure(BackpressureEvent event) = 0;

  /// Applies a control command (state switch, status query). Returns true for a Trigger the
  /// loop must execute; commands for other nodes are ignored.
  virtual bool ApplyControl(const ControlCommand& command) = 0;

  /// Accounts for one business logic invocation.
  virtual void CountInvocation() noexcept = 0;

  /// Publishes a non-fatal error on the status socket.
  virtual void ReportError(const Status& status) = 0;

  /// Acknowledges a finished triggered step on the status socket.
  virtual void ReportStepCompleted(std::uint64_t sequence, const Status& status) = 0;
};

/// Decides when and how the business logic is called. NodeCore calls Prepare() once the sockets
/// are open and then Run() on the node's thread.
class NodeLoop {
 public:
  virtual ~NodeLoop() = default;

  virtual std::string_view name() const noexcept = 0;

  /// True for loops driven by Trigger commands.
  virtual bool triggered() const noexcept = 0;

  /// Validates the loop against the node's sockets.
  virtual Status Prepare(const SocketManager& sockets) = 0;

  /// Runs until a stop is requested (returns Ok), the business logic ends the stream (returns
  /// EndOfStream) or the transport fails (returns the failure).
  virtual Status Run(LoopContext& context) = 0;

  /// Called after Run() returned Ok while the context is finishing: processes the input that is
  /// still queued and returns once it is done, or once the context stops finishing. Results as
  /// for Run(). The default has nothing to drain.
  virtual Status Drain(LoopContext& context);

 protected:
  /// Applies all pending control commands without blocking, until a Stop or the stop token ends
  /// the loop (while finishing, commands are still read). Trigger sequences are appended to
  /// `triggers` when given. Fails only on transport errors.
  static Status DrainControl(LoopContext& context, std::deque<std::uint64_t>* triggers);

  /// Waits up to `timeout` for control commands and applies them.
  static Status AwaitControl(LoopContext& context, Timeout timeout,
                             std::deque<std::uint64_t>* triggers);

  /// Publishes an Ok result on `outputs` (see Publish) and reports failures. Returns the logic's
  /// status. Triggers that arrive while publishing waits are appended to `triggers` when given.
  static Status Dispatch(LoopContext& context, std::pair<MessagePtr, Status> result,
                         const std::vector<std::size_t>& outputs,
                         std::deque<std::uint64_t>* triggers = nullptr);

  /// Publishes `message` on `output`, waiting while a consumer has no room (backpressure).
  /// Meanwhile control commands are applied, and a Stop or the stop token gives up on the
  /// message (Closed); a Finish does not. After send_timeout() the context's backpressure
  /// handler decides: skip the message (Ok), report it (Timeout), or wait once more.
  static Status Publish(LoopContext& context, std::size_t output, const MessagePtr& message,
                        std::deque<std::uint64_t>* triggers);

  static Status ValidateOutputs(const SocketManager& sockets,
                                const std::vector<std::size_t>& outputs, std::string_view owner);
};

/// Base of loops that run routes on input messages.
class RoutingLoop : public NodeLoop {
 public:
  Status Prepare(const SocketManager& sockets) override;

 protected:
  explicit RoutingLoop(std::vector<Route> routes);

  /// Receives one message from `input` and runs every route of that input. Returns the first
  /// non-Ok status of the invoked handlers, or EndOfStream as soon as a handler ends the stream.
  /// `triggers` as for Dispatch.
  Status ProcessInput(LoopContext& context, std::size_t input,
                      std::deque<std::uint64_t>* triggers = nullptr);

 private:
  std::vector<Route> routes_;
  std::vector<std::vector<const Route*>> routes_by_input_;
};

}  // namespace mloom
