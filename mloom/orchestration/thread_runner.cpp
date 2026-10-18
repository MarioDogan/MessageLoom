#include "mloom/orchestration/thread_runner.hpp"

#include <utility>

namespace mloom {

ThreadRunner::~ThreadRunner() {
  RequestStop();
  Wait();
}

Status ThreadRunner::Launch(std::unique_ptr<Node> node) {
  if (!node) return Status::InvalidArgument("cannot launch a null node");
  auto slot = std::make_unique<Slot>();
  slot->node = std::move(node);
  Slot* raw = slot.get();
  slot->thread = std::jthread(
      [raw](std::stop_token stop_token) { raw->result = raw->node->Run(std::move(stop_token)); });
  slots_.push_back(std::move(slot));
  return Status::Ok();
}

Status ThreadRunner::LaunchAll(std::vector<std::unique_ptr<Node>> nodes) {
  for (auto& node : nodes) {
    if (Status status = Launch(std::move(node)); !status.ok()) return status;
  }
  return Status::Ok();
}

void ThreadRunner::RequestStop() {
  for (const auto& slot : slots_) slot->thread.request_stop();
}

std::vector<NodeResult> ThreadRunner::Wait() {
  std::vector<NodeResult> results;
  for (const auto& slot : slots_) {
    if (slot->thread.joinable()) slot->thread.join();
    results.push_back({slot->node->name(), slot->result.value_or(Status::Error("never ran"))});
  }
  return results;
}

}  // namespace mloom
