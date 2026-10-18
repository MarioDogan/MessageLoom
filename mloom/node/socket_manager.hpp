#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "mloom/core/message.hpp"
#include "mloom/core/status.hpp"
#include "mloom/core/timeout.hpp"
#include "mloom/node/control.hpp"
#include "mloom/transport/message_queue.hpp"
#include "mloom/transport/socket.hpp"

namespace mloom {

/// Endpoint lists (ParseEndpoints syntax) of a node's sockets.
struct SocketLayout {
  std::vector<std::string> inputs{};   ///< Data subscribers; connect by default.
  std::vector<std::string> outputs{};  ///< Data publishers; bind by default.
  std::string control{};               ///< Control subscriber; empty for none.
  std::string status{};                ///< Status publisher; empty for none.
};

enum class WaitScope : std::uint8_t {
  kControl,           ///< Only the control socket.
  kControlAndInputs,  ///< The control socket and every input.
};

/// Sockets that have data after SocketManager::Wait.
struct Readiness {
  bool control = false;
  std::vector<std::size_t> inputs;  ///< Readable input indices, ascending.
};

/// Owns and multiplexes a node's sockets: data inputs (subscribers), data outputs (publishers),
/// one control subscriber and one status publisher.
///
/// Open() creates the sockets; from then on the manager must only be used by the thread that
/// runs the node. The control socket is opened last, so once an orchestrator sees the node's
/// control subscription, all other sockets of the node exist.
class SocketManager {
 public:
  SocketManager(std::shared_ptr<MessageQueue> queue, SocketLayout layout);
  ~SocketManager();
  SocketManager(const SocketManager&) = delete;
  SocketManager& operator=(const SocketManager&) = delete;

  Status Open();
  void Close() noexcept;
  bool is_open() const noexcept { return open_; }

  const SocketLayout& layout() const noexcept { return layout_; }
  std::size_t input_count() const noexcept { return layout_.inputs.size(); }
  std::size_t output_count() const noexcept { return layout_.outputs.size(); }
  bool has_control() const noexcept { return !layout_.control.empty(); }
  bool has_status() const noexcept { return !layout_.status.empty(); }

  /// Waits for activity within `scope`. Returns Timeout when nothing became readable. Without a
  /// control socket a kControl wait just sleeps for `timeout`. A kControl wait still services
  /// the inputs' connections, so binding inputs accept publishers while the node is idle.
  Status Wait(WaitScope scope, Timeout timeout, Readiness& ready);

  /// Services pending connection work of every subscriber without consuming messages.
  void ServiceConnections();

  std::pair<MessagePtr, Status> Receive(std::size_t input, Timeout timeout = kNoWait);

  /// Receives the next control command. A message of another type is consumed and reported
  /// as TypeMismatch.
  std::pair<ControlCommand, Status> ReceiveControl(Timeout timeout = kNoWait);

  /// Publishes on a data output, waiting up to `timeout` while a consumer has no room.
  Status Publish(std::size_t output, MessagePtr message, Timeout timeout = kNoWait);

  /// Publishes on the status socket; Ok without effect when the node has none.
  Status PublishStatus(const NodeStatus& status);

  Status WaitForSubscribers(std::size_t output, std::size_t count, Timeout timeout);
  Status WaitForStatusSubscribers(std::size_t count, Timeout timeout);

  /// Resolved endpoints of an output, e.g. to learn a bound tcp port. Empty before Open().
  std::vector<Endpoint> output_endpoints(std::size_t output) const;
  std::vector<Endpoint> status_endpoints() const;

 private:
  Status OpenSockets();

  std::shared_ptr<MessageQueue> queue_;
  SocketLayout layout_;
  std::vector<std::unique_ptr<PubSocket>> outputs_;
  std::unique_ptr<PubSocket> status_;
  std::vector<std::unique_ptr<SubSocket>> inputs_;
  std::unique_ptr<SubSocket> control_;
  std::unique_ptr<Poller> control_poller_;  // control, plus the inputs as service-only
  std::unique_ptr<Poller> all_poller_;      // control (index 0, if any) followed by the inputs
  std::vector<std::size_t> scratch_;
  Readiness service_scratch_;
  bool open_ = false;
};

}  // namespace mloom
