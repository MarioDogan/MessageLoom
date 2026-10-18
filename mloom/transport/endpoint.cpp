#include "mloom/transport/endpoint.hpp"

#include <array>

namespace mloom {
namespace {

constexpr std::array<std::pair<std::string_view, Transport>, 6> kSchemes = {{
    {"inproc", Transport::kInproc},
    {"ipc", Transport::kIpc},
    {"tcp", Transport::kTcp},
    {"udp", Transport::kUdp},
    {"pgm", Transport::kPgm},
    {"epgm", Transport::kEpgm},
}};

std::string_view Trim(std::string_view text) {
  constexpr std::string_view kSpace = " \t\r\n";
  const std::size_t begin = text.find_first_not_of(kSpace);
  if (begin == std::string_view::npos) return {};
  return text.substr(begin, text.find_last_not_of(kSpace) - begin + 1);
}

std::pair<Endpoint, Status> ParseOne(std::string_view entry) {
  Endpoint endpoint;
  if (entry.starts_with('@')) {
    endpoint.mode = EndpointMode::kBind;
    entry.remove_prefix(1);
  } else if (entry.starts_with('>')) {
    endpoint.mode = EndpointMode::kConnect;
    entry.remove_prefix(1);
  }
  entry = Trim(entry);
  const std::size_t separator = entry.find("://");
  if (separator == std::string_view::npos || separator == 0) {
    return {{},
            Status::InvalidArgument("endpoint '" + std::string(entry) +
                                    "' lacks a transport scheme such as inproc://")};
  }
  const std::string_view scheme = entry.substr(0, separator);
  bool known = false;
  for (const auto& [name, transport] : kSchemes) {
    if (name == scheme) {
      endpoint.transport = transport;
      known = true;
    }
  }
  if (!known) {
    return {{}, Status::InvalidArgument("unknown transport '" + std::string(scheme) + "'")};
  }
  if (separator + 3 == entry.size()) {
    return {{}, Status::InvalidArgument("endpoint '" + std::string(entry) + "' has no address")};
  }
  endpoint.address = std::string(entry);
  return {std::move(endpoint), Status::Ok()};
}

}  // namespace

std::string_view ToString(Transport transport) noexcept {
  for (const auto& [name, value] : kSchemes) {
    if (value == transport) return name;
  }
  return "unknown";
}

std::string Endpoint::ToString() const {
  switch (mode) {
    case EndpointMode::kBind:
      return "@" + address;
    case EndpointMode::kConnect:
      return ">" + address;
    case EndpointMode::kDefault:
      break;
  }
  return address;
}

std::pair<std::vector<Endpoint>, Status> ParseEndpoints(std::string_view spec) {
  std::vector<Endpoint> endpoints;
  if (Trim(spec).empty()) return {{}, Status::InvalidArgument("empty endpoint specification")};
  while (true) {
    const std::size_t comma = spec.find(',');
    const std::string_view entry = Trim(spec.substr(0, comma));
    if (entry.empty()) {
      return {{}, Status::InvalidArgument("empty entry in endpoint list")};
    }
    auto [endpoint, status] = ParseOne(entry);
    if (!status.ok()) return {{}, std::move(status)};
    endpoints.push_back(std::move(endpoint));
    if (comma == std::string_view::npos) break;
    spec.remove_prefix(comma + 1);
  }
  return {std::move(endpoints), Status::Ok()};
}

}  // namespace mloom
