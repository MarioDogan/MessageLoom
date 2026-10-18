#include "mloom/node/node.hpp"

#include <utility>

namespace mloom {

Node::Node(std::string name, std::unique_ptr<NodeCore> core)
    : name_(std::move(name)), core_(std::move(core)) {}

Status Node::Run(std::stop_token stop_token) {
  if (!core_) return Status::InvalidArgument("node '" + name_ + "' has no core");
  if (name_.empty()) return Status::InvalidArgument("a node needs a name");
  return core_->Run(name_, std::move(stop_token));
}

}  // namespace mloom
