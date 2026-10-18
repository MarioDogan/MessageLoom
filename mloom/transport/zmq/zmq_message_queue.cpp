#include "mloom/transport/zmq/zmq_message_queue.hpp"

#include <algorithm>
#include <zmq.hpp>

#include "mloom/transport/zmq/zmq_sockets.hpp"

namespace mloom::zeromq {

ZmqMessageQueue::ZmqMessageQueue(ZmqOptions options)
    : options_(std::move(options)),
      codecs_(options_.codecs ? options_.codecs : std::make_shared<CodecRegistry>()),
      context_(std::make_shared<::zmq::context_t>(std::max(options_.io_threads, 1))) {
  // Never let context shutdown hang on unsent messages; sockets set their own linger.
  zmq_ctx_set(context_->handle(), ZMQ_BLOCKY, 0);
}

ZmqMessageQueue::~ZmqMessageQueue() = default;

std::pair<std::unique_ptr<PubSocket>, Status> ZmqMessageQueue::CreatePublisher(
    std::string_view spec) {
  auto [endpoints, status] = ParseEndpoints(spec);
  if (!status.ok()) return {nullptr, std::move(status)};
  return internal::ZmqPubSocket::Create(
      context_, codecs_, std::move(endpoints),
      {options_.send_high_water_mark, static_cast<int>(options_.linger.count())});
}

std::pair<std::unique_ptr<SubSocket>, Status> ZmqMessageQueue::CreateSubscriber(
    std::string_view spec) {
  auto [endpoints, status] = ParseEndpoints(spec);
  if (!status.ok()) return {nullptr, std::move(status)};
  return internal::ZmqSubSocket::Create(
      context_, codecs_, std::move(endpoints),
      {options_.receive_high_water_mark, static_cast<int>(options_.linger.count())});
}

std::unique_ptr<Poller> ZmqMessageQueue::CreatePoller() {
  return std::make_unique<internal::ZmqPoller>();
}

}  // namespace mloom::zeromq
