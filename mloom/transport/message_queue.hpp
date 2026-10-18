#pragma once

#include <memory>
#include <string_view>
#include <utility>

#include "mloom/core/codec.hpp"
#include "mloom/core/status.hpp"
#include "mloom/transport/socket.hpp"

namespace mloom {

/// Factory for pub and sub sockets; abstracts the communication mechanism.
///
/// Endpoint lists use the ParseEndpoints syntax. Publishers bind and subscribers connect unless
/// an entry says otherwise ('@' binds, '>' connects). Implementations are thread-safe.
class MessageQueue {
 public:
  virtual ~MessageQueue() = default;

  virtual std::pair<std::unique_ptr<PubSocket>, Status> CreatePublisher(
      std::string_view endpoints) = 0;
  virtual std::pair<std::unique_ptr<SubSocket>, Status> CreateSubscriber(
      std::string_view endpoints) = 0;
  virtual std::unique_ptr<Poller> CreatePoller() = 0;

  /// Codecs used for messages that have to leave the process.
  virtual CodecRegistry& codecs() noexcept = 0;
};

}  // namespace mloom
