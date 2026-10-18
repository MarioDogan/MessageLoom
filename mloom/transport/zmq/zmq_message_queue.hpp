#pragma once

#include <memory>
#include <string_view>
#include <utility>

#include "mloom/core/codec.hpp"
#include "mloom/core/timeout.hpp"
#include "mloom/transport/message_queue.hpp"

namespace zmq {
class context_t;
}  // namespace zmq

namespace mloom::zeromq {

struct ZmqOptions {
  /// Background I/O threads of the ZeroMQ context (used by ipc and tcp only).
  int io_threads = 1;
  /// Messages queued per subscriber before the publisher's Send waits or fails (backpressure).
  /// inproc pipes hold the sum of both high-water marks.
  int send_high_water_mark = 1000;
  /// Messages queued in a subscriber before it stops accepting more.
  int receive_high_water_mark = 1000;
  /// How long closing a socket may wait to flush unsent ipc/tcp messages.
  Timeout linger{100};
  /// Codec registry for ipc/tcp. A private registry is created when null.
  std::shared_ptr<CodecRegistry> codecs{};
};

/// MessageQueue backed by ZeroMQ.
///
/// inproc endpoints pass MessagePtr by pointer: no copy, no serialization, and every subscriber
/// shares the published Message. ipc and tcp endpoints serialize through codecs(). Publishers
/// are XPUB sockets (they track subscriptions); subscribers are SUB sockets that receive
/// everything. A socket's endpoints must be all inproc or all ipc/tcp.
///
/// inproc only reaches sockets of the same ZeroMQ context, so all nodes of a process that talk
/// inproc must share one ZmqMessageQueue. Sockets keep the context alive, so the queue may be
/// destroyed before them.
class ZmqMessageQueue final : public MessageQueue {
 public:
  explicit ZmqMessageQueue(ZmqOptions options = {});
  ~ZmqMessageQueue() override;

  std::pair<std::unique_ptr<PubSocket>, Status> CreatePublisher(
      std::string_view endpoints) override;
  std::pair<std::unique_ptr<SubSocket>, Status> CreateSubscriber(
      std::string_view endpoints) override;
  std::unique_ptr<Poller> CreatePoller() override;
  CodecRegistry& codecs() noexcept override { return *codecs_; }

 private:
  ZmqOptions options_;
  std::shared_ptr<CodecRegistry> codecs_;
  std::shared_ptr<::zmq::context_t> context_;
};

}  // namespace mloom::zeromq
