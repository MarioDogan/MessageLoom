#include "mloom/node/testing/node_harness.hpp"

#include <unistd.h>

#include <algorithm>

namespace mloom::testing {

std::string UniqueInproc(const std::string& base) {
  static std::atomic<int> counter{0};
  return "inproc://" + base + "-" + std::to_string(::getpid()) + "-" + std::to_string(counter++);
}

NodeHarness::NodeHarness(HarnessConfig config, std::shared_ptr<MessageQueue> queue)
    : queue_(std::move(queue)), config_(config) {
  // Test publishers bind and the node's subscribers connect; the node's publishers bind and
  // the test's subscribers connect (inproc allows connecting before the bind).
  for (std::size_t i = 0; i < config.inputs; ++i) {
    const std::string address = UniqueInproc("input");
    input_addresses_.push_back(address);
    if (config.bind_inputs) {
      layout_.inputs.push_back("@" + address);
    } else {
      inputs_.push_back(queue_->CreatePublisher(address).first);
      layout_.inputs.push_back(address);
    }
  }
  for (std::size_t i = 0; i < config.outputs; ++i) {
    const std::string address = UniqueInproc("output");
    outputs_.push_back(queue_->CreateSubscriber(address).first);
    layout_.outputs.push_back(address);
  }
  if (config.control) {
    layout_.control = UniqueInproc("control");
    control_ = queue_->CreatePublisher(layout_.control).first;
  }
  if (config.status) {
    layout_.status = UniqueInproc("status");
    status_ = queue_->CreateSubscriber(layout_.status).first;
  }
}

NodeHarness::~NodeHarness() {
  if (thread_.joinable()) (void)Stop();
}

NodeCoreOptions NodeHarness::BarrierOptions() const {
  NodeCoreOptions options;
  options.min_output_subscribers.assign(outputs_.size(), 1);
  options.min_status_subscribers = status_ ? 1 : 0;
  return options;
}

void NodeHarness::Launch(std::unique_ptr<NodeLoop> loop, NodeCoreOptions options,
                         std::string name) {
  auto sockets = std::make_unique<SocketManager>(queue_, layout_);
  auto core = std::make_unique<NodeCore>(std::move(sockets), std::move(loop), std::move(options));
  node_ = std::make_unique<Node>(std::move(name), std::move(core));
  thread_ = std::jthread(
      [this](std::stop_token stop_token) { result_ = node_->Run(std::move(stop_token)); });
}

Status NodeHarness::Start(std::unique_ptr<NodeLoop> loop, NodeCoreOptions options,
                          std::string name) {
  Launch(std::move(loop), std::move(options), std::move(name));
  if (control_) {
    if (Status status = control_->WaitForSubscribers(1, kTestTimeout); !status.ok()) {
      return status;
    }
  }
  if (status_) {
    const auto initial = AwaitEvent(StatusEvent::kStateChanged);
    if (!initial) return Status::Timeout("node did not announce its initial state");
  }
  if (config_.bind_inputs) {
    // The node's inputs are bound by now; connecting publishers must be accepted by it.
    for (const std::string& address : input_addresses_) {
      auto [input, status] = queue_->CreatePublisher(">" + address);
      if (!status.ok()) return status;
      inputs_.push_back(std::move(input));
    }
  }
  return AwaitSubscribers();
}

Status NodeHarness::AwaitSubscribers() {
  if (control_) {
    if (Status status = control_->WaitForSubscribers(1, kTestTimeout); !status.ok()) {
      return status;
    }
  }
  for (auto& input : inputs_) {
    if (Status status = input->WaitForSubscribers(1, kTestTimeout); !status.ok()) return status;
  }
  return Status::Ok();
}

Status NodeHarness::Send(std::size_t input, MessagePtr message) {
  return inputs_.at(input)->Send(std::move(message));
}

std::pair<MessagePtr, Status> NodeHarness::Receive(std::size_t output, Timeout timeout) {
  return outputs_.at(output)->Receive(timeout);
}

Status NodeHarness::Command(ControlType type, std::uint64_t sequence, std::string target) {
  return SendControl(MakeMessage(ControlCommand{type, std::move(target), sequence}));
}

Status NodeHarness::SendControl(MessagePtr message) {
  if (!control_) return Status::Closed("harness has no control socket");
  return control_->Send(std::move(message));
}

std::optional<NodeStatus> NodeHarness::AwaitStatus(
    const std::function<bool(const NodeStatus&)>& match, Timeout timeout) {
  if (!status_) return std::nullopt;
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (true) {
    const auto remaining = std::chrono::ceil<Timeout>(deadline - std::chrono::steady_clock::now());
    if (remaining <= Timeout::zero()) return std::nullopt;
    auto [message, status] = status_->Receive(remaining);
    if (!status.ok()) return std::nullopt;
    if (const NodeStatus* report = message->As<NodeStatus>(); report && match(*report)) {
      return *report;
    }
  }
}

std::optional<NodeStatus> NodeHarness::AwaitEvent(StatusEvent event, Timeout timeout) {
  return AwaitStatus([event](const NodeStatus& report) { return report.event == event; }, timeout);
}

Status NodeHarness::Stop() {
  thread_.request_stop();
  return Join();
}

Status NodeHarness::Join() {
  if (thread_.joinable()) thread_.join();
  return result_.value_or(Status::Error("node never ran"));
}

SocketRig::SocketRig(HarnessConfig config, zeromq::ZmqOptions options)
    : queue(std::make_shared<zeromq::ZmqMessageQueue>(std::move(options))),
      harness(config, queue),
      sockets(queue, harness.layout()) {}

Status SocketRig::Open() {
  if (Status status = sockets.Open(); !status.ok()) return status;
  for (std::size_t output = 0; output < sockets.output_count(); ++output) {
    if (Status status = sockets.WaitForSubscribers(output, 1, kTestTimeout); !status.ok()) {
      return status;
    }
  }
  if (sockets.has_status()) {
    if (Status status = sockets.WaitForStatusSubscribers(1, kTestTimeout); !status.ok()) {
      return status;
    }
  }
  return harness.AwaitSubscribers();
}

Status SocketRig::AwaitInput(std::size_t input) {
  const auto deadline = std::chrono::steady_clock::now() + kTestTimeout;
  Readiness ready;
  while (std::chrono::steady_clock::now() < deadline) {
    Status status = sockets.Wait(WaitScope::kControlAndInputs, Timeout(10), ready);
    if (status.code() == StatusCode::kTimeout) continue;
    if (!status.ok()) return status;
    if (std::find(ready.inputs.begin(), ready.inputs.end(), input) != ready.inputs.end()) {
      return Status::Ok();
    }
  }
  return Status::Timeout("input " + std::to_string(input) + " never became readable");
}

}  // namespace mloom::testing
