#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "mloom/core/message.hpp"
#include "mloom/core/status.hpp"
#include "mloom/core/timeout.hpp"
#include "mloom/transport/endpoint.hpp"

namespace mloom {

/// A data channel endpoint. Sockets are either publishers or subscribers; the transport behind
/// them (inproc, ipc, tcp, ...) is hidden by the MessageQueue that created them.
///
/// A socket is not thread-safe: use it from one thread at a time.
class Socket {
 public:
  virtual ~Socket() = default;
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;

  /// Addresses the socket is attached to. Bound wildcard addresses (tcp://127.0.0.1:*) are
  /// reported resolved, so peers can connect to them.
  virtual const std::vector<Endpoint>& endpoints() const noexcept = 0;

 protected:
  Socket() = default;
};

/// Sends messages to every connected subscriber.
class PubSocket : public Socket {
 public:
  /// Publishes `message` to every subscriber. No subscriber misses a message silently: while one
  /// of them has no room left (its queue reached the high-water mark), the call waits up to
  /// `timeout` for room and then fails with Timeout, and no subscriber receives the message.
  /// kNoWait never blocks; a negative timeout waits until there is room.
  virtual Status Send(MessagePtr message, Timeout timeout) = 0;

  /// Send() without waiting.
  Status Send(MessagePtr message) { return Send(std::move(message), kNoWait); }

  /// Waits until at least `count` subscriptions are known to this publisher. Pub/sub drops
  /// messages for subscribers that have not finished joining; this is the synchronization point.
  ///
  /// A subscriber that binds (the publisher connects) accepts the publisher only while it is
  /// being read or polled, see PollInterest::kServiceOnly. The default topology, publishers bind
  /// and subscribers connect, has no such restriction.
  virtual Status WaitForSubscribers(std::size_t count, Timeout timeout) = 0;
};

/// Receives every message published by the publishers it is attached to.
class SubSocket : public Socket {
 public:
  /// The next message, or a Timeout status when none arrived in time.
  virtual std::pair<MessagePtr, Status> Receive(Timeout timeout) = 0;
};

enum class PollInterest : std::uint8_t {
  kMessages,     ///< Wait() reports the socket when it has a message.
  kServiceOnly,  ///< Wait() keeps the socket's connections serviced (accepts joining publishers)
                 ///< but never reports it and never wakes up for its messages.
};

/// Waits on several subscriber sockets of the same MessageQueue implementation at once.
class Poller {
 public:
  virtual ~Poller() = default;

  /// Adds `socket`, which must outlive the poller. Returns its index within the poller.
  virtual std::pair<std::size_t, Status> Add(SubSocket& socket,
                                             PollInterest interest = PollInterest::kMessages) = 0;

  /// Waits until a socket is readable and stores the readable indices, ascending, in `ready`.
  /// Returns Timeout when no socket became readable.
  virtual Status Wait(Timeout timeout, std::vector<std::size_t>& ready) = 0;
};

}  // namespace mloom
