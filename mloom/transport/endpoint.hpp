#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mloom/core/status.hpp"

namespace mloom {

enum class Transport : std::uint8_t { kInproc, kIpc, kTcp, kUdp, kPgm, kEpgm };

/// Whether a socket binds an address or connects to it.
enum class EndpointMode : std::uint8_t {
  kDefault,  ///< Publishers bind, subscribers connect.
  kBind,
  kConnect,
};

std::string_view ToString(Transport transport) noexcept;

/// One address a socket binds or connects to.
struct Endpoint {
  EndpointMode mode = EndpointMode::kDefault;
  Transport transport = Transport::kInproc;
  std::string address;  ///< Full address including scheme, e.g. "tcp://127.0.0.1:5555".

  /// True when messages never leave the process and can be passed by pointer.
  bool is_local() const noexcept { return transport == Transport::kInproc; }

  EndpointMode ResolvedMode(EndpointMode fallback) const noexcept {
    return mode == EndpointMode::kDefault ? fallback : mode;
  }

  /// The address with its '@' / '>' prefix, as accepted by ParseEndpoints.
  std::string ToString() const;

  friend bool operator==(const Endpoint&, const Endpoint&) = default;
};

/// Parses a comma separated endpoint list. Each entry is a transport address with an optional
/// prefix: '@' binds, '>' connects, none uses the socket's default (publishers bind, subscribers
/// connect). Example: "@tcp://*:5555, >inproc://camera".
std::pair<std::vector<Endpoint>, Status> ParseEndpoints(std::string_view spec);

}  // namespace mloom
