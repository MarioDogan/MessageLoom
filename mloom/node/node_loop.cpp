#include "mloom/node/node_loop.hpp"

#include <algorithm>
#include <chrono>
#include <string>

namespace mloom {
namespace {

/// Errors about a single bad message: the message is gone, the socket is fine.
bool IsMessageError(StatusCode code) {
  return code == StatusCode::kTypeMismatch || code == StatusCode::kInvalidArgument ||
         code == StatusCode::kUnsupported;
}

/// True once the stop token or a Stop command ended the loop; a Finish alone does not.
bool Aborted(const LoopContext& context) {
  return context.stop_requested() && !context.finishing();
}

}  // namespace

Status NodeLoop::DrainControl(LoopContext& context, std::deque<std::uint64_t>* triggers) {
  SocketManager& sockets = context.sockets();
  if (!sockets.has_control()) return Status::Ok();
  while (!Aborted(context)) {
    auto [command, status] = sockets.ReceiveControl(kNoWait);
    if (status.code() == StatusCode::kTimeout) break;
    if (!status.ok()) {
      if (!IsMessageError(status.code())) return status;
      context.ReportError(status);
      continue;
    }
    if (context.ApplyControl(command) && triggers != nullptr) {
      triggers->push_back(command.sequence);
    }
  }
  return Status::Ok();
}

Status NodeLoop::Drain(LoopContext& /*context*/) { return Status::Ok(); }

Status NodeLoop::AwaitControl(LoopContext& context, Timeout timeout,
                              std::deque<std::uint64_t>* triggers) {
  Readiness ready;
  Status status = context.sockets().Wait(WaitScope::kControl, timeout, ready);
  if (status.code() == StatusCode::kTimeout) return Status::Ok();
  if (!status.ok()) return status;
  return ready.control ? DrainControl(context, triggers) : Status::Ok();
}

Status NodeLoop::Dispatch(LoopContext& context, std::pair<MessagePtr, Status> result,
                          const std::vector<std::size_t>& outputs,
                          std::deque<std::uint64_t>* triggers) {
  auto& [message, status] = result;
  context.CountInvocation();
  switch (status.code()) {
    case StatusCode::kOk:
      if (message) {
        for (const std::size_t output : outputs) {
          Status sent = Publish(context, output, message, triggers);
          if (sent.ok()) continue;
          if (Aborted(context)) break;  // stopping: the message is given up
          context.ReportError(sent);
        }
      }
      break;
    case StatusCode::kNeedMoreData:
    case StatusCode::kEndOfStream:
      break;
    default:
      context.ReportError(status);
      break;
  }
  return std::move(status);
}

Status NodeLoop::Publish(LoopContext& context, std::size_t output, const MessagePtr& message,
                         std::deque<std::uint64_t>* triggers) {
  using Clock = std::chrono::steady_clock;
  for (std::uint32_t attempt = 1;; ++attempt) {
    const Timeout limit = context.send_timeout();
    const auto start = Clock::now();
    while (true) {
      Timeout slice = context.poll_interval();
      if (limit >= Timeout::zero()) {
        const auto left = std::chrono::ceil<Timeout>(start + limit - Clock::now());
        slice = std::min(slice, std::max(left, Timeout::zero()));
      }
      Status sent = context.sockets().Publish(output, message, slice);
      if (sent.code() != StatusCode::kTimeout) return sent;  // published, or the transport failed
      // No room yet: stay responsive to control commands and stop requests meanwhile.
      if (Status control = DrainControl(context, triggers); !control.ok()) return control;
      if (Aborted(context)) {
        return Status::Closed("stopped while output " + std::to_string(output) + " had no room");
      }
      if (limit >= Timeout::zero() && Clock::now() - start >= limit) break;
    }
    const BackpressureAction action =
        context.OnBackpressure(BackpressureEvent{{}, output, message, limit, attempt});
    if (action == BackpressureAction::kRetry) continue;
    if (action == BackpressureAction::kSkip) return Status::Ok();
    return Status::Timeout("output " + std::to_string(output) + " had no room for " +
                           std::to_string(limit.count()) + " ms; message dropped");
  }
}

Status NodeLoop::ValidateOutputs(const SocketManager& sockets,
                                 const std::vector<std::size_t>& outputs, std::string_view owner) {
  for (const std::size_t output : outputs) {
    if (output >= sockets.output_count()) {
      return Status::InvalidArgument(std::string(owner) + " publishes on output " +
                                     std::to_string(output) + ", but the node has " +
                                     std::to_string(sockets.output_count()) + " outputs");
    }
  }
  return Status::Ok();
}

RoutingLoop::RoutingLoop(std::vector<Route> routes) : routes_(std::move(routes)) {}

Status RoutingLoop::Prepare(const SocketManager& sockets) {
  if (routes_.empty()) return Status::InvalidArgument(std::string(name()) + " has no routes");
  routes_by_input_.assign(sockets.input_count(), {});
  for (std::size_t i = 0; i < routes_.size(); ++i) {
    const Route& route = routes_[i];
    const std::string owner = "route " + std::to_string(i);
    if (!route.handler) return Status::InvalidArgument(owner + " has no logic handler");
    if (route.input >= sockets.input_count()) {
      return Status::InvalidArgument(owner + " reads input " + std::to_string(route.input) +
                                     ", but the node has " + std::to_string(sockets.input_count()) +
                                     " inputs");
    }
    if (Status status = ValidateOutputs(sockets, route.outputs, owner); !status.ok()) {
      return status;
    }
    routes_by_input_[route.input].push_back(&route);
  }
  for (std::size_t input = 0; input < routes_by_input_.size(); ++input) {
    if (routes_by_input_[input].empty()) {
      return Status::InvalidArgument("input " + std::to_string(input) +
                                     " has no route; its messages would be lost");
    }
  }
  return Status::Ok();
}

Status RoutingLoop::ProcessInput(LoopContext& context, std::size_t input,
                                 std::deque<std::uint64_t>* triggers) {
  auto [message, received] = context.sockets().Receive(input, kNoWait);
  if (received.code() == StatusCode::kTimeout) return Status::Ok();
  if (!received.ok()) {
    context.ReportError(received);
    return received;
  }
  const std::vector<const Route*>& routes = routes_by_input_[input];
  Status first_failure;
  for (std::size_t i = 0; i < routes.size(); ++i) {
    // The last route gets our reference, so a consuming handler can move the payload out
    // when no one else holds the message.
    MessagePtr argument = i + 1 == routes.size() ? std::move(message) : message;
    Status status = Dispatch(context, routes[i]->handler->Handle(std::move(argument)),
                             routes[i]->outputs, triggers);
    if (status.code() == StatusCode::kEndOfStream) return status;
    if (first_failure.ok()) first_failure = std::move(status);
  }
  return first_failure;
}

}  // namespace mloom
