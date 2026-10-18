#pragma once

// Test support: a NodeLoop whose Prepare(), Run() and Drain() are callables, to test NodeCore
// with exactly the loop behaviour a test needs.

#include <functional>
#include <string_view>
#include <utility>

#include "mloom/core/status.hpp"
#include "mloom/node/node_loop.hpp"
#include "mloom/node/socket_manager.hpp"

namespace mloom::testing {

class ScriptedLoop final : public NodeLoop {
 public:
  using RunFn = std::function<Status(LoopContext&)>;
  using PrepareFn = std::function<Status(const SocketManager&)>;
  using DrainFn = std::function<Status(LoopContext&)>;

  /// `run` and `drain` execute on the node's thread. Without `prepare` and `drain`, Prepare()
  /// and Drain() succeed.
  explicit ScriptedLoop(RunFn run, bool triggered = false, PrepareFn prepare = {},
                        DrainFn drain = {})
      : run_(std::move(run)),
        prepare_(std::move(prepare)),
        drain_(std::move(drain)),
        triggered_(triggered) {}

  std::string_view name() const noexcept override { return "ScriptedLoop"; }
  bool triggered() const noexcept override { return triggered_; }
  Status Prepare(const SocketManager& sockets) override {
    return prepare_ ? prepare_(sockets) : Status::Ok();
  }
  Status Run(LoopContext& context) override { return run_ ? run_(context) : Status::Ok(); }
  Status Drain(LoopContext& context) override { return drain_ ? drain_(context) : Status::Ok(); }

  /// A RunFn that only serves control commands until a stop is requested.
  static Status ServeControl(LoopContext& context) {
    while (!context.stop_requested()) {
      if (Status status = AwaitControl(context, context.poll_interval(), nullptr); !status.ok()) {
        return status;
      }
    }
    return Status::Ok();
  }

 private:
  RunFn run_;
  PrepareFn prepare_;
  DrainFn drain_;
  bool triggered_;
};

}  // namespace mloom::testing
