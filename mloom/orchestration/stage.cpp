#include "mloom/orchestration/stage.hpp"

#include <algorithm>
#include <set>

namespace mloom {
namespace {

bool IsValidIdentifier(std::string_view text) {
  return !text.empty() && text.find_first_of(", \t\r\n") == std::string_view::npos;
}

using BuiltNodes = std::pair<std::vector<std::unique_ptr<Node>>, Status>;

BuiltNodes Failed(Status status) {
  return {std::vector<std::unique_ptr<Node>>(), std::move(status)};
}

bool HasDuplicates(std::vector<std::string> values) {
  std::sort(values.begin(), values.end());
  return std::adjacent_find(values.begin(), values.end()) != values.end();
}

}  // namespace

Stage::Stage(std::string name, StageOptions options) : name_(std::move(name)), options_(options) {}

Status Stage::Add(NodeSpec spec) {
  if (!IsValidIdentifier(spec.name)) {
    return Status::InvalidArgument("invalid node name '" + spec.name + "'");
  }
  for (const NodeSpec& node : nodes_) {
    if (node.name == spec.name) {
      return Status::InvalidArgument("stage '" + name_ + "' already has a node '" + spec.name +
                                     "'");
    }
  }
  for (const auto* channels : {&spec.inputs, &spec.outputs}) {
    for (const std::string& channel : *channels) {
      if (!IsValidIdentifier(channel)) {
        return Status::InvalidArgument("node '" + spec.name + "' uses invalid channel name '" +
                                       channel + "'");
      }
    }
  }
  if (!spec.loop) return Status::InvalidArgument("node '" + spec.name + "' has no loop factory");
  nodes_.push_back(std::move(spec));
  return Status::Ok();
}

std::vector<std::string> Stage::node_names() const {
  std::vector<std::string> names;
  for (const NodeSpec& node : nodes_) names.push_back(node.name);
  return names;
}

std::vector<const NodeSpec*> Stage::Producers(std::string_view channel) const {
  std::vector<const NodeSpec*> producers;
  for (const NodeSpec& node : nodes_) {
    if (std::find(node.outputs.begin(), node.outputs.end(), channel) != node.outputs.end()) {
      producers.push_back(&node);
    }
  }
  return producers;
}

std::size_t Stage::Consumers(std::string_view channel) const {
  std::size_t consumers = 0;
  for (const NodeSpec& node : nodes_) {
    consumers +=
        static_cast<std::size_t>(std::count(node.inputs.begin(), node.inputs.end(), channel));
  }
  return consumers;
}

Status Stage::Validate() const {
  if (!IsValidIdentifier(name_))
    return Status::InvalidArgument("invalid stage name '" + name_ + "'");
  if (nodes_.empty()) return Status::InvalidArgument("stage '" + name_ + "' has no nodes");
  for (const NodeSpec& node : nodes_) {
    if (HasDuplicates(node.inputs) || HasDuplicates(node.outputs)) {
      return Status::InvalidArgument("node '" + node.name + "' lists a channel twice");
    }
    for (const std::string& channel : node.inputs) {
      if (Producers(channel).empty()) {
        return Status::InvalidArgument("node '" + node.name + "' reads channel '" + channel +
                                       "', which no node produces");
      }
    }
  }
  return Status::Ok();
}

std::vector<std::string> Stage::TopologicalOrder() const {
  std::vector<std::string> order;
  std::set<std::string_view> placed;
  // Repeatedly place every node whose producers are all placed (Kahn's algorithm, stable).
  bool progress = true;
  while (progress && order.size() < nodes_.size()) {
    progress = false;
    for (const NodeSpec& node : nodes_) {
      if (placed.contains(node.name)) continue;
      const bool ready = std::all_of(node.inputs.begin(), node.inputs.end(), [&](const auto& ch) {
        const auto producers = Producers(ch);
        return std::all_of(producers.begin(), producers.end(), [&](const NodeSpec* producer) {
          return producer == &node || placed.contains(producer->name);
        });
      });
      if (ready) {
        placed.insert(node.name);
        order.push_back(node.name);
        progress = true;
      }
    }
  }
  for (const NodeSpec& node : nodes_) {  // cycles
    if (!placed.contains(node.name)) order.push_back(node.name);
  }
  return order;
}

std::string Stage::ControlEndpoint() const { return "inproc://" + name_ + "/control"; }

std::string Stage::StatusEndpoint(std::string_view node) const {
  return "inproc://" + name_ + "/" + std::string(node) + "/status";
}

std::string Stage::ChannelEndpoint(std::string_view node, std::string_view channel) const {
  return "inproc://" + name_ + "/" + std::string(node) + "/out/" + std::string(channel);
}

std::pair<std::vector<std::unique_ptr<Node>>, Status> Stage::Build(
    const std::shared_ptr<MessageQueue>& queue) const {
  if (!queue) return Failed(Status::InvalidArgument("stage needs a message queue"));
  if (Status status = Validate(); !status.ok()) return Failed(std::move(status));

  std::vector<std::unique_ptr<Node>> nodes;
  for (const NodeSpec& spec : nodes_) {
    SocketLayout layout;
    for (const std::string& channel : spec.inputs) {
      std::string endpoints;
      for (const NodeSpec* producer : Producers(channel)) {
        if (!endpoints.empty()) endpoints += ",";
        endpoints += ChannelEndpoint(producer->name, channel);
      }
      layout.inputs.push_back(std::move(endpoints));
    }
    NodeCoreOptions options = spec.options;
    options.min_output_subscribers.clear();
    for (const std::string& channel : spec.outputs) {
      layout.outputs.push_back(ChannelEndpoint(spec.name, channel));
      options.min_output_subscribers.push_back(Consumers(channel));
    }
    if (options_.orchestrated) {
      layout.control = ControlEndpoint();
      layout.status = StatusEndpoint(spec.name);
      options.min_status_subscribers = 1;
    }
    options.start_paused = options.start_paused || options_.start_paused;
    options.startup_timeout = options_.startup_timeout;

    std::unique_ptr<NodeLoop> loop = spec.loop();
    if (!loop) {
      return Failed(
          Status::InvalidArgument("loop factory of node '" + spec.name + "' returned no loop"));
    }
    auto core =
        std::make_unique<NodeCore>(std::make_unique<SocketManager>(queue, std::move(layout)),
                                   std::move(loop), std::move(options));
    nodes.push_back(std::make_unique<Node>(spec.name, std::move(core)));
  }
  return {std::move(nodes), Status::Ok()};
}

}  // namespace mloom
