#pragma once

// Internal to //mloom/transport/zmq. Use ZmqMessageQueue to create these sockets.

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <zmq.hpp>

#include "mloom/core/codec.hpp"
#include "mloom/transport/socket.hpp"

namespace mloom::zeromq::internal {

struct SocketOptions {
  int high_water_mark = 1000;
  int linger_ms = 0;
};

class ZmqPubSocket final : public PubSocket {
 public:
  static std::pair<std::unique_ptr<PubSocket>, Status> Create(
      std::shared_ptr<::zmq::context_t> context, std::shared_ptr<const CodecRegistry> codecs,
      std::vector<Endpoint> endpoints, const SocketOptions& options);

  using PubSocket::Send;
  Status Send(MessagePtr message, Timeout timeout) override;
  Status WaitForSubscribers(std::size_t count, Timeout timeout) override;
  const std::vector<Endpoint>& endpoints() const noexcept override { return endpoints_; }

 private:
  ZmqPubSocket(std::shared_ptr<::zmq::context_t> context,
               std::shared_ptr<const CodecRegistry> codecs, std::vector<Endpoint> endpoints);

  /// Reads the subscribe/unsubscribe notifications XPUB queued since the last call.
  Status DrainSubscriptions();

  // The context is declared first so that it outlives the socket.
  std::shared_ptr<::zmq::context_t> context_;
  std::shared_ptr<const CodecRegistry> codecs_;
  ::zmq::socket_t socket_;
  std::vector<Endpoint> endpoints_;
  bool local_;
  std::size_t subscriptions_ = 0;
  int send_timeout_ms_ = -1;  // ZMQ_SNDTIMEO currently set on socket_
};

class ZmqSubSocket final : public SubSocket {
 public:
  static std::pair<std::unique_ptr<SubSocket>, Status> Create(
      std::shared_ptr<::zmq::context_t> context, std::shared_ptr<const CodecRegistry> codecs,
      std::vector<Endpoint> endpoints, const SocketOptions& options);

  std::pair<MessagePtr, Status> Receive(Timeout timeout) override;
  const std::vector<Endpoint>& endpoints() const noexcept override { return endpoints_; }

  void* handle() noexcept { return socket_.handle(); }

 private:
  ZmqSubSocket(std::shared_ptr<::zmq::context_t> context,
               std::shared_ptr<const CodecRegistry> codecs, std::vector<Endpoint> endpoints);

  std::pair<MessagePtr, Status> Decode(const ::zmq::message_t& frame) const;

  std::shared_ptr<::zmq::context_t> context_;
  std::shared_ptr<const CodecRegistry> codecs_;
  ::zmq::socket_t socket_;
  std::vector<Endpoint> endpoints_;
  bool local_;
};

class ZmqPoller final : public Poller {
 public:
  std::pair<std::size_t, Status> Add(SubSocket& socket, PollInterest interest) override;
  Status Wait(Timeout timeout, std::vector<std::size_t>& ready) override;

 private:
  std::vector<zmq_pollitem_t> items_;
};

}  // namespace mloom::zeromq::internal
