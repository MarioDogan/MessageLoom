#include "mloom/node/triggered_source_loop.hpp"

#include <deque>

namespace mloom {

TriggeredSourceLoop::TriggeredSourceLoop(std::shared_ptr<SourceHandlerBase> source,
                                         std::vector<std::size_t> outputs)
    : source_(std::move(source)), outputs_(std::move(outputs)) {}

Status TriggeredSourceLoop::Prepare(const SocketManager& sockets) {
  if (!source_) return Status::InvalidArgument("TriggeredSourceLoop has no source handler");
  if (!sockets.has_control()) {
    return Status::InvalidArgument("TriggeredSourceLoop needs a control socket");
  }
  if (sockets.input_count() != 0) {
    return Status::InvalidArgument("TriggeredSourceLoop does not read inputs");
  }
  return ValidateOutputs(sockets, outputs_, name());
}

Status TriggeredSourceLoop::Run(LoopContext& context) {
  std::deque<std::uint64_t> triggers;
  while (!context.stop_requested()) {
    if (triggers.empty()) {
      if (Status status = AwaitControl(context, context.poll_interval(), &triggers); !status.ok()) {
        return status;
      }
      continue;
    }
    // Pick up Pause/Stop that arrived while earlier triggers were being served.
    if (Status status = DrainControl(context, &triggers); !status.ok()) return status;
    if (context.stop_requested()) break;
    if (context.state() != NodeState::kRunning) {
      triggers.clear();
      continue;
    }
    const std::uint64_t sequence = triggers.front();
    triggers.pop_front();
    Status step = Dispatch(context, source_->Produce(), outputs_, &triggers);
    context.ReportStepCompleted(sequence, step);
    if (step.code() == StatusCode::kEndOfStream) return Status::EndOfStream();
  }
  return Status::Ok();
}

}  // namespace mloom
