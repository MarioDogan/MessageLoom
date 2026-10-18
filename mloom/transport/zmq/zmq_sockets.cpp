#include "mloom/transport/zmq/zmq_sockets.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <string>
#include <thread>

namespace mloom::zeromq::internal {
namespace {

/// Heap cell carried by an inproc frame. ZeroMQ hands the pointer through its pipes untouched
/// and calls ReleaseLocalFrame once the last copy of the frame is gone (received by every
/// subscriber, dropped, or discarded at shutdown), so a published message cannot leak.
struct LocalFrame {
  static constexpr std::uint64_t kMagic = 0x4D4C4F4F4D505452;  // "MLOOMPTR"
  std::uint64_t magic = kMagic;
  MessagePtr message;
};

void ReleaseLocalFrame(void* data, void* /*hint*/) noexcept {
  delete static_cast<LocalFrame*>(data);
}

void ReleaseBytes(void* /*data*/, void* hint) noexcept { delete static_cast<Bytes*>(hint); }

Status ToStatus(const ::zmq::error_t& error, std::string_view action) {
  std::string message = std::string(action) + ": " + error.what();
  return error.num() == ETERM ? Status::Closed(std::move(message))
                              : Status::Error(std::move(message));
}

long ToZmqTimeout(Timeout timeout) {
  return timeout.count() < 0 ? -1 : static_cast<long>(timeout.count());
}

/// > 0 when readable, 0 on timeout, < 0 on error (errno set).
int PollIn(void* socket, Timeout timeout) {
  zmq_pollitem_t item{socket, 0, ZMQ_POLLIN, 0};
  return zmq_poll(&item, 1, ToZmqTimeout(timeout));
}

Status PollError(std::string_view action) {
  if (errno == EINTR) return Status::Timeout("interrupted");
  const std::string message = std::string(action) + ": " + zmq_strerror(errno);
  return errno == ETERM ? Status::Closed(message) : Status::Error(message);
}

Status ValidateEndpoints(const std::vector<Endpoint>& endpoints) {
  for (const Endpoint& endpoint : endpoints) {
    switch (endpoint.transport) {
      case Transport::kInproc:
      case Transport::kIpc:
      case Transport::kTcp:
        break;
      case Transport::kUdp:
        return Status::Unsupported("'" + endpoint.address +
                                   "': udp needs ZeroMQ RADIO/DISH sockets (draft API), which "
                                   "this libzmq build does not provide");
      case Transport::kPgm:
      case Transport::kEpgm:
        return Status::Unsupported("'" + endpoint.address +
                                   "': pgm/epgm multicast is not built into libzmq");
    }
    if (endpoint.is_local() != endpoints.front().is_local()) {
      return Status::InvalidArgument(
          "cannot mix inproc with ipc/tcp endpoints on one socket ('" + endpoints.front().address +
          "', '" + endpoint.address +
          "'): in-process peers receive pointers, remote peers serialized frames");
    }
  }
  return Status::Ok();
}

/// Binds or connects `socket` to every endpoint, recording resolved addresses of bound ones.
Status Attach(::zmq::socket_t& socket, std::vector<Endpoint>& endpoints,
              EndpointMode default_mode) {
  for (Endpoint& endpoint : endpoints) {
    const bool bind = endpoint.ResolvedMode(default_mode) == EndpointMode::kBind;
    try {
      if (bind) {
        socket.bind(endpoint.address);
        endpoint.address = socket.get(::zmq::sockopt::last_endpoint);
      } else {
        socket.connect(endpoint.address);
      }
    } catch (const ::zmq::error_t& error) {
      return Status::InvalidArgument(std::string(bind ? "cannot bind '" : "cannot connect '") +
                                     endpoint.address + "': " + error.what());
    }
  }
  return Status::Ok();
}

}  // namespace

// ---------------------------------------------------------------------------------------------
// ZmqPubSocket

ZmqPubSocket::ZmqPubSocket(std::shared_ptr<::zmq::context_t> context,
                           std::shared_ptr<const CodecRegistry> codecs,
                           std::vector<Endpoint> endpoints)
    : context_(std::move(context)),
      codecs_(std::move(codecs)),
      socket_(*context_, ::zmq::socket_type::xpub),
      endpoints_(std::move(endpoints)),
      local_(endpoints_.front().is_local()) {}

std::pair<std::unique_ptr<PubSocket>, Status> ZmqPubSocket::Create(
    std::shared_ptr<::zmq::context_t> context, std::shared_ptr<const CodecRegistry> codecs,
    std::vector<Endpoint> endpoints, const SocketOptions& options) {
  if (Status status = ValidateEndpoints(endpoints); !status.ok()) return {nullptr, status};
  std::unique_ptr<ZmqPubSocket> socket;
  try {
    socket.reset(new ZmqPubSocket(std::move(context), std::move(codecs), std::move(endpoints)));
    socket->socket_.set(::zmq::sockopt::sndhwm, options.high_water_mark);
    socket->socket_.set(::zmq::sockopt::linger, options.linger_ms);
    // A full subscriber makes sends wait or fail (backpressure) instead of losing messages.
    socket->socket_.set(::zmq::sockopt::xpub_nodrop, true);
    // Report every subscribe and unsubscribe, not only the first per topic.
    socket->socket_.set(::zmq::sockopt::xpub_verboser, true);
  } catch (const ::zmq::error_t& error) {
    return {nullptr, ToStatus(error, "create publisher")};
  }
  if (Status status = Attach(socket->socket_, socket->endpoints_, EndpointMode::kBind);
      !status.ok()) {
    return {nullptr, status};
  }
  return {std::move(socket), Status::Ok()};
}

Status ZmqPubSocket::Send(MessagePtr message, Timeout timeout) {
  if (!message) return Status::InvalidArgument("cannot publish a null message");
  try {
    ::zmq::message_t frame;
    if (local_) {
      auto cell = std::make_unique<LocalFrame>();
      cell->message = std::move(message);
      frame.rebuild(cell.get(), sizeof(LocalFrame), &ReleaseLocalFrame, nullptr);
      cell.release();  // owned by the frame now
    } else {
      auto bytes = std::make_unique<Bytes>();
      if (Status status = codecs_->Encode(*message, *bytes); !status.ok()) return status;
      frame.rebuild(bytes->data(), bytes->size(), &ReleaseBytes, bytes.get());
      bytes.release();  // owned by the frame now
    }
    // With xpub_nodrop the send fails (EAGAIN) while any subscriber has no room; the frame then
    // still owns the message and releases it.
    bool sent = false;
    if (timeout == kNoWait) {
      sent = socket_.send(frame, ::zmq::send_flags::dontwait).has_value();
    } else {
      const int timeout_ms = static_cast<int>(ToZmqTimeout(timeout));
      if (timeout_ms != send_timeout_ms_) {
        socket_.set(::zmq::sockopt::sndtimeo, timeout_ms);
        send_timeout_ms_ = timeout_ms;
      }
      sent = socket_.send(frame, ::zmq::send_flags::none).has_value();
    }
    if (!sent)
      return Status::Timeout("a subscriber of '" + endpoints_.front().address + "' has no room");
  } catch (const ::zmq::error_t& error) {
    if (error.num() == EINTR) return Status::Timeout("interrupted");
    return ToStatus(error, "publish");
  }
  return Status::Ok();
}

Status ZmqPubSocket::DrainSubscriptions() {
  try {
    ::zmq::message_t event;
    while (socket_.recv(event, ::zmq::recv_flags::dontwait)) {
      if (event.size() == 0) continue;
      const auto kind = *static_cast<const unsigned char*>(event.data());
      if (kind == 1) {
        ++subscriptions_;
      } else if (kind == 0 && subscriptions_ > 0) {
        --subscriptions_;
      }
    }
  } catch (const ::zmq::error_t& error) {
    return ToStatus(error, "read subscriptions");
  }
  return Status::Ok();
}

Status ZmqPubSocket::WaitForSubscribers(std::size_t count, Timeout timeout) {
  using Clock = std::chrono::steady_clock;
  const auto deadline = Clock::now() + std::max(timeout, Timeout::zero());
  while (true) {
    if (Status status = DrainSubscriptions(); !status.ok()) return status;
    if (subscriptions_ >= count) return Status::Ok();
    Timeout remaining = kWaitForever;
    if (timeout >= Timeout::zero()) {
      remaining = std::chrono::ceil<Timeout>(deadline - Clock::now());
      if (remaining <= Timeout::zero()) {
        return Status::Timeout(std::to_string(subscriptions_) + " of " + std::to_string(count) +
                               " subscribers on '" + endpoints_.front().address + "'");
      }
    }
    if (PollIn(socket_.handle(), remaining) < 0) {
      Status status = PollError("wait for subscribers");
      if (status.code() != StatusCode::kTimeout) return status;
    }
  }
}

// ---------------------------------------------------------------------------------------------
// ZmqSubSocket

ZmqSubSocket::ZmqSubSocket(std::shared_ptr<::zmq::context_t> context,
                           std::shared_ptr<const CodecRegistry> codecs,
                           std::vector<Endpoint> endpoints)
    : context_(std::move(context)),
      codecs_(std::move(codecs)),
      socket_(*context_, ::zmq::socket_type::sub),
      endpoints_(std::move(endpoints)),
      local_(endpoints_.front().is_local()) {}

std::pair<std::unique_ptr<SubSocket>, Status> ZmqSubSocket::Create(
    std::shared_ptr<::zmq::context_t> context, std::shared_ptr<const CodecRegistry> codecs,
    std::vector<Endpoint> endpoints, const SocketOptions& options) {
  if (Status status = ValidateEndpoints(endpoints); !status.ok()) return {nullptr, status};
  std::unique_ptr<ZmqSubSocket> socket;
  try {
    socket.reset(new ZmqSubSocket(std::move(context), std::move(codecs), std::move(endpoints)));
    socket->socket_.set(::zmq::sockopt::rcvhwm, options.high_water_mark);
    socket->socket_.set(::zmq::sockopt::linger, options.linger_ms);
    socket->socket_.set(::zmq::sockopt::subscribe, "");
  } catch (const ::zmq::error_t& error) {
    return {nullptr, ToStatus(error, "create subscriber")};
  }
  if (Status status = Attach(socket->socket_, socket->endpoints_, EndpointMode::kConnect);
      !status.ok()) {
    return {nullptr, status};
  }
  return {std::move(socket), Status::Ok()};
}

std::pair<MessagePtr, Status> ZmqSubSocket::Receive(Timeout timeout) {
  if (timeout != kNoWait) {
    const int ready = PollIn(socket_.handle(), timeout);
    if (ready < 0) return {nullptr, PollError("receive")};
    if (ready == 0) return {nullptr, Status::Timeout()};
  }
  ::zmq::message_t frame;
  try {
    if (!socket_.recv(frame, ::zmq::recv_flags::dontwait)) return {nullptr, Status::Timeout()};
    if (frame.more()) {
      // Not from a MessageLoom publisher; ZeroMQ delivers all parts at once, discard them.
      bool more = true;
      while (more && socket_.recv(frame, ::zmq::recv_flags::dontwait)) more = frame.more();
      return {nullptr, Status::InvalidArgument("unexpected multipart message")};
    }
  } catch (const ::zmq::error_t& error) {
    return {nullptr, ToStatus(error, "receive")};
  }
  return Decode(frame);
}

std::pair<MessagePtr, Status> ZmqSubSocket::Decode(const ::zmq::message_t& frame) const {
  if (!local_) {
    return codecs_->Decode(ByteSpan(static_cast<const std::byte*>(frame.data()), frame.size()));
  }
  // inproc peers live in this process and share our private context, so the frame is a cell
  // created by ZmqPubSocket::Send. The checks guard against misuse, not against attackers.
  if (frame.size() != sizeof(LocalFrame)) {
    return {nullptr, Status::InvalidArgument("malformed inproc frame")};
  }
  const auto* cell = static_cast<const LocalFrame*>(frame.data());
  if (cell->magic != LocalFrame::kMagic || !cell->message) {
    return {nullptr, Status::InvalidArgument("malformed inproc frame")};
  }
  return {cell->message, Status::Ok()};
}

// ---------------------------------------------------------------------------------------------
// ZmqPoller

std::pair<std::size_t, Status> ZmqPoller::Add(SubSocket& socket, PollInterest interest) {
  auto* zmq_socket = dynamic_cast<ZmqSubSocket*>(&socket);
  if (zmq_socket == nullptr) {
    return {0, Status::InvalidArgument("socket was not created by a ZmqMessageQueue")};
  }
  // zmq_poll processes pending commands (e.g. joining peers) of every listed socket, even of
  // items that request no events; such items never wake the poll up.
  const short events = interest == PollInterest::kMessages ? ZMQ_POLLIN : 0;
  items_.push_back(zmq_pollitem_t{zmq_socket->handle(), 0, events, 0});
  return {items_.size() - 1, Status::Ok()};
}

Status ZmqPoller::Wait(Timeout timeout, std::vector<std::size_t>& ready) {
  ready.clear();
  if (items_.empty()) {
    if (timeout < Timeout::zero()) {
      return Status::InvalidArgument("waiting forever on an empty poller");
    }
    std::this_thread::sleep_for(timeout);
    return Status::Timeout();
  }
  const int count = zmq_poll(items_.data(), static_cast<int>(items_.size()), ToZmqTimeout(timeout));
  if (count < 0) return PollError("poll");
  if (count == 0) return Status::Timeout();
  for (std::size_t i = 0; i < items_.size(); ++i) {
    if ((items_[i].revents & ZMQ_POLLIN) != 0) ready.push_back(i);
  }
  return Status::Ok();
}

}  // namespace mloom::zeromq::internal
