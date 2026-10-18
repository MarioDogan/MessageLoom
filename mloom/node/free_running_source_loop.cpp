#include "mloom/node/free_running_source_loop.hpp"

#include <algorithm>

namespace mloom {

FreeRunningSourceLoop::FreeRunningSourceLoop(std::shared_ptr<SourceHandlerBase> source,
                                             std::vector<std::size_t> outputs,
                                             FreeRunningSourceOptions options)
    : source_(std::move(source)), outputs_(std::move(outputs)), options_(options) {}

Status FreeRunningSourceLoop::Prepare(const SocketManager& sockets) {
  if (!source_) return Status::InvalidArgument("FreeRunningSourceLoop has no source handler");
  if (sockets.input_count() != 0) {
    return Status::InvalidArgument("FreeRunningSourceLoop does not read inputs");
  }
  return ValidateOutputs(sockets, outputs_, name());
}

Status FreeRunningSourceLoop::Run(LoopContext& context) {
  using Clock = std::chrono::steady_clock;
  auto next_slot = Clock::now();
  while (!context.stop_requested()) {
    if (context.state() != NodeState::kRunning) {
      if (Status status = AwaitControl(context, context.poll_interval(), nullptr); !status.ok()) {
        return status;
      }
      next_slot = Clock::now();
      continue;
    }
    if (options_.period > std::chrono::nanoseconds::zero()) {
      const auto now = Clock::now();
      if (now < next_slot) {
        // Sleep until the next slot, but stay responsive to control commands.
        const Timeout wait =
            std::min(std::chrono::ceil<Timeout>(next_slot - now), context.poll_interval());
        if (Status status = AwaitControl(context, wait, nullptr); !status.ok()) return status;
        continue;
      }
      next_slot += options_.period;
      if (next_slot <= now) next_slot = now + options_.period;  // fell behind: no burst
    }
    if (Status status = DrainControl(context, nullptr); !status.ok()) return status;
    if (context.stop_requested() || context.state() != NodeState::kRunning) continue;
    if (Dispatch(context, source_->Produce(), outputs_).code() == StatusCode::kEndOfStream) {
      return Status::EndOfStream();
    }
  }
  return Status::Ok();
}

}  // namespace mloom
