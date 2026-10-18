#include "mloom/node/socket_manager.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "mloom/node/testing/node_harness.hpp"
#include "mloom/transport/zmq/zmq_message_queue.hpp"

namespace mloom {
namespace {

using namespace std::chrono_literals;
using testing::HarnessConfig;
using testing::kTestTimeout;
using testing::SocketRig;
using testing::UniqueInproc;

constexpr Timeout kQuiet = 100ms;  // how long "nothing happens" is observed

bool Contains(const Status& status, const std::string& text) {
  return status.message().find(text) != std::string::npos;
}

/// Waits until `scope` reports at least what `expected` lists.
Readiness AwaitReadiness(SocketManager& sockets, WaitScope scope, const Readiness& expected) {
  Readiness ready;
  const auto deadline = std::chrono::steady_clock::now() + kTestTimeout;
  while (std::chrono::steady_clock::now() < deadline) {
    (void)sockets.Wait(scope, Timeout(10), ready);
    if (ready.control == expected.control && ready.inputs == expected.inputs) break;
  }
  return ready;
}

// --- Layout and lifecycle -----------------------------------------------------------------------

TEST(SocketManagerTest, CountsComeFromTheLayout) {
  SocketManager sockets(nullptr, SocketLayout{.inputs = {"inproc://a", "inproc://b"},
                                              .outputs = {"inproc://c"},
                                              .control = {},
                                              .status = "inproc://d"});
  EXPECT_EQ(sockets.input_count(), 2u);
  EXPECT_EQ(sockets.output_count(), 1u);
  EXPECT_FALSE(sockets.has_control());
  EXPECT_TRUE(sockets.has_status());
  EXPECT_EQ(sockets.layout().status, "inproc://d");
  EXPECT_FALSE(sockets.is_open());
}

TEST(SocketManagerTest, OperationsBeforeOpenReportClosed) {
  SocketRig rig;
  SocketManager& sockets = rig.sockets;
  EXPECT_EQ(sockets.Receive(0).second.code(), StatusCode::kClosed);
  EXPECT_EQ(sockets.ReceiveControl().second.code(), StatusCode::kClosed);
  EXPECT_EQ(sockets.Publish(0, MakeMessage(1)).code(), StatusCode::kClosed);
  EXPECT_EQ(sockets.WaitForSubscribers(0, 1, kNoWait).code(), StatusCode::kClosed);
  EXPECT_EQ(sockets.WaitForStatusSubscribers(1, kNoWait).code(), StatusCode::kClosed);
  Readiness ready{.control = true, .inputs = {3}};
  EXPECT_EQ(sockets.Wait(WaitScope::kControlAndInputs, kNoWait, ready).code(), StatusCode::kClosed);
  EXPECT_FALSE(ready.control);  // stale readiness is cleared
  EXPECT_TRUE(ready.inputs.empty());
  EXPECT_TRUE(sockets.output_endpoints(0).empty());
  EXPECT_TRUE(sockets.status_endpoints().empty());
  sockets.ServiceConnections();  // no-op
}

TEST(SocketManagerTest, OpenNeedsAQueueAndHappensOnce) {
  SocketManager queueless(
      nullptr,
      SocketLayout{.inputs = {}, .outputs = {UniqueInproc("out")}, .control = {}, .status = {}});
  EXPECT_EQ(queueless.Open().code(), StatusCode::kInvalidArgument);
  EXPECT_FALSE(queueless.is_open());

  SocketRig rig;
  ASSERT_TRUE(rig.sockets.Open().ok());
  EXPECT_EQ(rig.sockets.Open().code(), StatusCode::kInvalidArgument);
  EXPECT_TRUE(rig.sockets.is_open());
}

TEST(SocketManagerTest, OpenRegistersControlCodecs) {
  SocketRig rig;
  EXPECT_FALSE(rig.queue->codecs().Contains<ControlCommand>());
  ASSERT_TRUE(rig.sockets.Open().ok());
  EXPECT_TRUE(rig.queue->codecs().Contains<ControlCommand>());
  EXPECT_TRUE(rig.queue->codecs().Contains<NodeStatus>());
}

TEST(SocketManagerTest, FailedOpenNamesTheSocketAndClosesTheOthers) {
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  const auto make_layout = [](std::string input, std::string output, std::string control,
                              std::string status) {
    return SocketLayout{.inputs = {std::move(input)},
                        .outputs = {std::move(output)},
                        .control = std::move(control),
                        .status = std::move(status)};
  };
  struct Case {
    std::string role;
    SocketLayout layout;
  };
  const std::vector<Case> cases = {
      {"output 'nonsense'", make_layout(UniqueInproc("in"), "nonsense", {}, {})},
      {"status 'nonsense'", make_layout(UniqueInproc("in"), UniqueInproc("out"), {}, "nonsense")},
      {"input 'nonsense'",
       make_layout("nonsense", UniqueInproc("out"), {}, UniqueInproc("status"))},
      {"control 'nonsense'",
       make_layout(UniqueInproc("in"), UniqueInproc("out"), "nonsense", UniqueInproc("status"))},
  };
  for (const Case& test : cases) {
    SocketManager sockets(queue, test.layout);
    const Status status = sockets.Open();
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument) << test.role;
    EXPECT_TRUE(Contains(status, test.role)) << status;
    EXPECT_FALSE(sockets.is_open());
    EXPECT_TRUE(sockets.output_endpoints(0).empty()) << test.role;  // already closed again
  }
}

TEST(SocketManagerTest, CloseIsIdempotent) {
  SocketRig rig;
  ASSERT_TRUE(rig.sockets.Open().ok());
  EXPECT_FALSE(rig.sockets.output_endpoints(0).empty());
  rig.sockets.Close();
  rig.sockets.Close();
  EXPECT_FALSE(rig.sockets.is_open());
  EXPECT_TRUE(rig.sockets.output_endpoints(0).empty());
  EXPECT_EQ(rig.sockets.Publish(0, MakeMessage(1)).code(), StatusCode::kClosed);
}

TEST(SocketManagerTest, BoundTcpEndpointsAreReportedResolved) {
  SocketManager sockets(std::make_shared<zeromq::ZmqMessageQueue>(),
                        SocketLayout{.inputs = {},
                                     .outputs = {"tcp://127.0.0.1:*"},
                                     .control = {},
                                     .status = "tcp://127.0.0.1:*"});
  ASSERT_TRUE(sockets.Open().ok());
  for (const auto& endpoints : {sockets.output_endpoints(0), sockets.status_endpoints()}) {
    ASSERT_EQ(endpoints.size(), 1u);
    EXPECT_EQ(endpoints[0].address.rfind("tcp://127.0.0.1:", 0), 0u) << endpoints[0].address;
    EXPECT_NE(endpoints[0].address.back(), '*') << endpoints[0].address;
  }
}

// --- Data ---------------------------------------------------------------------------------------

TEST(SocketManagerTest, ReceivesInputsAndPublishesOutputs) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());

  MessagePtr sent = MakeMessage(5);
  ASSERT_TRUE(rig.harness.Send(0, sent).ok());
  auto [received, received_status] = rig.sockets.Receive(0, kTestTimeout);
  ASSERT_TRUE(received_status.ok()) << received_status;
  EXPECT_EQ(received.get(), sent.get());  // inproc messages travel by pointer

  ASSERT_TRUE(rig.sockets.Publish(0, MakeMessage(6)).ok());
  auto [published, published_status] = rig.harness.Receive(0);
  ASSERT_TRUE(published_status.ok()) << published_status;
  EXPECT_EQ(*published->As<int>(), 6);

  EXPECT_EQ(rig.sockets.Receive(0, kNoWait).second.code(), StatusCode::kTimeout);
}

TEST(SocketManagerTest, IndicesOutOfRangeAreInvalidArguments) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  const Status input = rig.sockets.Receive(5).second;
  EXPECT_EQ(input.code(), StatusCode::kInvalidArgument);
  EXPECT_TRUE(Contains(input, "input 5 does not exist (node has 1)")) << input;
  const Status output = rig.sockets.Publish(5, MakeMessage(1));
  EXPECT_EQ(output.code(), StatusCode::kInvalidArgument);
  EXPECT_TRUE(Contains(output, "output 5 does not exist (node has 1)")) << output;
  EXPECT_EQ(rig.sockets.WaitForSubscribers(5, 1, kNoWait).code(), StatusCode::kInvalidArgument);
  EXPECT_TRUE(rig.sockets.output_endpoints(5).empty());
}

// --- Control and status -------------------------------------------------------------------------

TEST(SocketManagerTest, ReceiveControlDecodesCommands) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kPause, 4, "camera").ok());
  auto [command, status] = rig.sockets.ReceiveControl(kTestTimeout);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(command.type, ControlType::kPause);
  EXPECT_EQ(command.sequence, 4u);
  EXPECT_EQ(command.target, "camera");
  EXPECT_EQ(rig.sockets.ReceiveControl().second.code(), StatusCode::kTimeout);
}

TEST(SocketManagerTest, ReceiveControlConsumesAndRejectsOtherPayloads) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  ASSERT_TRUE(rig.harness.SendControl(MakeMessage(42)).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kStop, 8).ok());

  const Status rejected = rig.sockets.ReceiveControl(kTestTimeout).second;
  EXPECT_EQ(rejected.code(), StatusCode::kTypeMismatch);
  EXPECT_TRUE(Contains(rejected, "int")) << rejected;
  auto [command, status] = rig.sockets.ReceiveControl(kTestTimeout);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(command.type, ControlType::kStop);
  EXPECT_EQ(command.sequence, 8u);
}

TEST(SocketManagerTest, WithoutControlSocket) {
  SocketRig rig(HarnessConfig{.control = false});
  ASSERT_TRUE(rig.Open().ok());
  EXPECT_FALSE(rig.sockets.has_control());
  EXPECT_EQ(rig.sockets.ReceiveControl().second.code(), StatusCode::kClosed);
  Readiness ready;
  EXPECT_EQ(rig.sockets.Wait(WaitScope::kControl, Timeout(20), ready).code(), StatusCode::kTimeout);
}

TEST(SocketManagerTest, WithoutStatusSocket) {
  SocketRig rig(HarnessConfig{.status = false});
  ASSERT_TRUE(rig.Open().ok());
  EXPECT_FALSE(rig.sockets.has_status());
  EXPECT_TRUE(rig.sockets.PublishStatus(NodeStatus{}).ok());  // nothing to publish on
  EXPECT_EQ(rig.sockets.WaitForStatusSubscribers(1, kNoWait).code(), StatusCode::kClosed);
  EXPECT_TRUE(rig.sockets.status_endpoints().empty());
}

TEST(SocketManagerTest, PublishStatusReachesStatusSubscribers) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  NodeStatus sent;
  sent.node = "camera";
  sent.state = NodeState::kPaused;
  sent.event = StatusEvent::kStateChanged;
  sent.sequence = 3;
  ASSERT_TRUE(rig.sockets.PublishStatus(sent).ok());
  auto report = rig.harness.AwaitStatus([](const NodeStatus&) { return true; });
  ASSERT_TRUE(report);
  EXPECT_EQ(report->node, "camera");
  EXPECT_EQ(report->state, NodeState::kPaused);
  EXPECT_EQ(report->event, StatusEvent::kStateChanged);
  EXPECT_EQ(report->sequence, 3u);
}

// --- Waiting ------------------------------------------------------------------------------------

TEST(SocketManagerTest, WaitReportsReadySocketsByRole) {
  SocketRig rig(HarnessConfig{.inputs = 3});
  ASSERT_TRUE(rig.Open().ok());
  ASSERT_TRUE(rig.harness.Send(2, MakeMessage(1)).ok());
  ASSERT_TRUE(rig.harness.Send(1, MakeMessage(2)).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kQueryStatus).ok());

  const Readiness ready = AwaitReadiness(rig.sockets, WaitScope::kControlAndInputs,
                                         Readiness{.control = true, .inputs = {1, 2}});
  EXPECT_TRUE(ready.control);
  EXPECT_EQ(ready.inputs, (std::vector<std::size_t>{1, 2}));  // ascending input indices
}

TEST(SocketManagerTest, InputIndicesDoNotDependOnTheControlSocket) {
  SocketRig rig(HarnessConfig{.inputs = 2, .control = false});
  ASSERT_TRUE(rig.Open().ok());
  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(1)).ok());
  const Readiness ready = AwaitReadiness(rig.sockets, WaitScope::kControlAndInputs,
                                         Readiness{.control = false, .inputs = {0}});
  EXPECT_FALSE(ready.control);
  EXPECT_EQ(ready.inputs, std::vector<std::size_t>{0});
}

TEST(SocketManagerTest, ControlWaitIgnoresInputs) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(1)).ok());
  Readiness ready;
  EXPECT_EQ(rig.sockets.Wait(WaitScope::kControl, kQuiet, ready).code(), StatusCode::kTimeout);
  EXPECT_FALSE(ready.control);
  EXPECT_TRUE(ready.inputs.empty());
  EXPECT_TRUE(rig.sockets.Receive(0, kTestTimeout).second.ok());  // still queued

  ASSERT_TRUE(rig.harness.Command(ControlType::kQueryStatus).ok());
  EXPECT_TRUE(rig.sockets.Wait(WaitScope::kControl, kTestTimeout, ready).ok());
  EXPECT_TRUE(ready.control);
}

}  // namespace
}  // namespace mloom
