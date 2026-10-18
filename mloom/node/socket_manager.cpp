#include "mloom/node/socket_manager.hpp"

#include <string_view>

namespace mloom {
namespace {

Status Annotate(const Status& status, std::string_view role, const std::string& spec) {
  return Status(status.code(), std::string(role) + " '" + spec + "': " + status.message());
}

Status OutOfRange(std::string_view role, std::size_t index, std::size_t count) {
  return Status::InvalidArgument(std::string(role) + " " + std::to_string(index) +
                                 " does not exist (node has " + std::to_string(count) + ")");
}

}  // namespace

SocketManager::SocketManager(std::shared_ptr<MessageQueue> queue, SocketLayout layout)
    : queue_(std::move(queue)), layout_(std::move(layout)) {}

SocketManager::~SocketManager() { Close(); }

Status SocketManager::Open() {
  if (open_) return Status::InvalidArgument("sockets are already open");
  if (!queue_) return Status::InvalidArgument("socket manager has no message queue");
  if (Status status = RegisterControlCodecs(queue_->codecs()); !status.ok()) return status;
  if (Status status = OpenSockets(); !status.ok()) {
    Close();
    return status;
  }
  open_ = true;
  return Status::Ok();
}

Status SocketManager::OpenSockets() {
  for (const std::string& spec : layout_.outputs) {
    auto [socket, status] = queue_->CreatePublisher(spec);
    if (!status.ok()) return Annotate(status, "output", spec);
    outputs_.push_back(std::move(socket));
  }
  if (has_status()) {
    auto [socket, status] = queue_->CreatePublisher(layout_.status);
    if (!status.ok()) return Annotate(status, "status", layout_.status);
    status_ = std::move(socket);
  }
  for (const std::string& spec : layout_.inputs) {
    auto [socket, status] = queue_->CreateSubscriber(spec);
    if (!status.ok()) return Annotate(status, "input", spec);
    inputs_.push_back(std::move(socket));
  }
  if (has_control()) {
    auto [socket, status] = queue_->CreateSubscriber(layout_.control);
    if (!status.ok()) return Annotate(status, "control", layout_.control);
    control_ = std::move(socket);
  }

  control_poller_ = queue_->CreatePoller();
  all_poller_ = queue_->CreatePoller();
  if (control_) {
    if (Status status = control_poller_->Add(*control_).second; !status.ok()) return status;
    if (Status status = all_poller_->Add(*control_).second; !status.ok()) return status;
  }
  for (const auto& input : inputs_) {
    if (Status status = all_poller_->Add(*input).second; !status.ok()) return status;
    if (Status status = control_poller_->Add(*input, PollInterest::kServiceOnly).second;
        !status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

void SocketManager::Close() noexcept {
  // Pollers reference the sockets, so they go first.
  control_poller_.reset();
  all_poller_.reset();
  control_.reset();
  inputs_.clear();
  status_.reset();
  outputs_.clear();
  open_ = false;
}

Status SocketManager::Wait(WaitScope scope, Timeout timeout, Readiness& ready) {
  ready.control = false;
  ready.inputs.clear();
  if (!open_) return Status::Closed("sockets are not open");
  Poller& poller = scope == WaitScope::kControl ? *control_poller_ : *all_poller_;
  if (Status status = poller.Wait(timeout, scratch_); !status.ok()) return status;
  const std::size_t first_input = control_ ? 1 : 0;
  for (const std::size_t index : scratch_) {
    if (index < first_input) {
      ready.control = true;
    } else {
      ready.inputs.push_back(index - first_input);
    }
  }
  return Status::Ok();
}

void SocketManager::ServiceConnections() {
  if (open_) (void)Wait(WaitScope::kControl, kNoWait, service_scratch_);
}

std::pair<MessagePtr, Status> SocketManager::Receive(std::size_t input, Timeout timeout) {
  if (!open_) return {nullptr, Status::Closed("sockets are not open")};
  if (input >= inputs_.size()) return {nullptr, OutOfRange("input", input, inputs_.size())};
  return inputs_[input]->Receive(timeout);
}

std::pair<ControlCommand, Status> SocketManager::ReceiveControl(Timeout timeout) {
  if (!open_) return {{}, Status::Closed("sockets are not open")};
  if (!control_) return {{}, Status::Closed("node has no control socket")};
  auto [message, status] = control_->Receive(timeout);
  if (!status.ok()) return {{}, std::move(status)};
  const ControlCommand* command = message->As<ControlCommand>();
  if (command == nullptr) {
    return {{},
            Status::TypeMismatch("control socket received '" + std::string(message->type().name()) +
                                 "'")};
  }
  return {*command, Status::Ok()};
}

Status SocketManager::Publish(std::size_t output, MessagePtr message, Timeout timeout) {
  if (!open_) return Status::Closed("sockets are not open");
  if (output >= outputs_.size()) return OutOfRange("output", output, outputs_.size());
  return outputs_[output]->Send(std::move(message), timeout);
}

Status SocketManager::PublishStatus(const NodeStatus& status) {
  if (!status_) return Status::Ok();
  return status_->Send(MakeMessage(status));
}

Status SocketManager::WaitForSubscribers(std::size_t output, std::size_t count, Timeout timeout) {
  if (!open_) return Status::Closed("sockets are not open");
  if (output >= outputs_.size()) return OutOfRange("output", output, outputs_.size());
  return outputs_[output]->WaitForSubscribers(count, timeout);
}

Status SocketManager::WaitForStatusSubscribers(std::size_t count, Timeout timeout) {
  if (!status_) return Status::Closed("node has no status socket");
  return status_->WaitForSubscribers(count, timeout);
}

std::vector<Endpoint> SocketManager::output_endpoints(std::size_t output) const {
  if (output >= outputs_.size()) return {};
  return outputs_[output]->endpoints();
}

std::vector<Endpoint> SocketManager::status_endpoints() const {
  return status_ ? status_->endpoints() : std::vector<Endpoint>{};
}

}  // namespace mloom
