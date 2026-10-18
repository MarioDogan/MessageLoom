#include "mloom/node/node.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "mloom/node/free_running_loop.hpp"
#include "mloom/node/free_running_source_loop.hpp"
#include "mloom/node/testing/node_harness.hpp"
#include "mloom/node/triggered_loop.hpp"
#include "mloom/node/triggered_source_loop.hpp"
#include "mloom/transport/zmq/zmq_message_queue.hpp"

namespace mloom {
namespace {

using namespace std::chrono_literals;
using testing::HarnessConfig;
using testing::kTestTimeout;
using testing::NodeHarness;

constexpr Timeout kQuiet = 100ms;  // how long "nothing happens" is observed

std::shared_ptr<LogicHandlerBase> Doubler() {
  return MakeLogicHandler([](const int& value) { return std::pair{value * 2, Status::Ok()}; });
}

std::unique_ptr<NodeLoop> DoublerLoop() {
  return std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, Doubler(), {0}}});
}

int ReceiveInt(NodeHarness& harness, std::size_t output = 0) {
  auto [message, status] = harness.Receive(output);
  EXPECT_TRUE(status.ok()) << status;
  if (!status.ok() || !message->IsType<int>()) return -1;
  return *message->As<int>();
}

// --- FreeRunningLoop ----------------------------------------------------------------------------

TEST(FreeRunningLoopTest, ProcessesInputsInOrder) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  EXPECT_EQ(harness.node().state(), NodeState::kRunning);
  for (int i = 1; i <= 5; ++i) ASSERT_TRUE(harness.Send(0, MakeMessage(i)).ok());
  for (int i = 1; i <= 5; ++i) EXPECT_EQ(ReceiveInt(harness), 2 * i);
  EXPECT_TRUE(harness.Stop().ok());
  EXPECT_EQ(harness.node().state(), NodeState::kStopped);
  EXPECT_EQ(harness.node().core().invocations(), 5u);
}

TEST(FreeRunningLoopTest, RoutesInputsToOutputs) {
  NodeHarness harness(HarnessConfig{.inputs = 2, .outputs = 2});
  auto negate = MakeLogicHandler([](const int& value) { return std::pair{-value, Status::Ok()}; });
  auto loop = std::make_unique<FreeRunningLoop>(
      std::vector<Route>{{0, Doubler(), {0}}, {1, negate, {0, 1}}});
  ASSERT_TRUE(harness.Start(std::move(loop), harness.BarrierOptions()).ok());

  ASSERT_TRUE(harness.Send(0, MakeMessage(3)).ok());
  EXPECT_EQ(ReceiveInt(harness, 0), 6);
  ASSERT_TRUE(harness.Send(1, MakeMessage(4)).ok());
  EXPECT_EQ(ReceiveInt(harness, 0), -4);
  EXPECT_EQ(ReceiveInt(harness, 1), -4);
  EXPECT_EQ(harness.Receive(1, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(FreeRunningLoopTest, SeveralRoutesShareAnInput) {
  NodeHarness harness(HarnessConfig{.inputs = 1, .outputs = 2});
  auto square =
      MakeLogicHandler([](const int& value) { return std::pair{value * value, Status::Ok()}; });
  auto loop =
      std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, Doubler(), {0}}, {0, square, {1}}});
  ASSERT_TRUE(harness.Start(std::move(loop), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(5)).ok());
  EXPECT_EQ(ReceiveInt(harness, 0), 10);
  EXPECT_EQ(ReceiveInt(harness, 1), 25);
}

TEST(FreeRunningLoopTest, NeedMoreDataAccumulates) {
  NodeHarness harness;
  auto pair_sum = MakeLogicHandler([pending = std::optional<int>()](const int& value) mutable {
    if (!pending) {
      pending = value;
      return std::pair{0, Status::NeedMoreData()};
    }
    const int sum = *pending + value;
    pending.reset();
    return std::pair{sum, Status::Ok()};
  });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, pair_sum, {0}}}),
                         harness.BarrierOptions())
                  .ok());
  for (int i : {1, 2, 3, 4}) ASSERT_TRUE(harness.Send(0, MakeMessage(i)).ok());
  EXPECT_EQ(ReceiveInt(harness), 3);
  EXPECT_EQ(ReceiveInt(harness), 7);
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(FreeRunningLoopTest, ReportsLogicErrorsAndKeepsRunning) {
  NodeHarness harness;
  auto even_only = MakeLogicHandler([](const int& value) {
    return std::pair{value, value % 2 == 0 ? Status::Ok() : Status::Error("odd input")};
  });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, even_only, {0}}}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(1)).ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(2)).ok());
  EXPECT_EQ(ReceiveInt(harness), 2);
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->code, StatusCode::kError);
  EXPECT_EQ(error->detail, "odd input");
  EXPECT_EQ(error->node, "node");
  EXPECT_EQ(harness.node().state(), NodeState::kRunning);
}

TEST(FreeRunningLoopTest, ReportsTypeMismatch) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(std::string("not an int"))).ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->code, StatusCode::kTypeMismatch);
  ASSERT_TRUE(harness.Send(0, MakeMessage(21)).ok());
  EXPECT_EQ(ReceiveInt(harness), 42);
}

TEST(FreeRunningLoopTest, EndOfStreamFromLogicStopsTheNode) {
  NodeHarness harness;
  auto until_zero = MakeLogicHandler([](const int& value) {
    return std::pair{value, value == 0 ? Status::EndOfStream() : Status::Ok()};
  });
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, until_zero, {0}}}),
                 harness.BarrierOptions())
          .ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(5)).ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(0)).ok());
  EXPECT_EQ(ReceiveInt(harness), 5);
  EXPECT_TRUE(harness.AwaitEvent(StatusEvent::kEndOfStream));
  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
  EXPECT_EQ(harness.node().state(), NodeState::kStopped);
}

TEST(FreeRunningLoopTest, SinkRoutesPublishNothing) {
  std::atomic<int> sunk{0};
  NodeHarness harness;
  auto sink = MakeLogicHandler([&sunk](const int& value) {
    sunk = value;
    return Status::Ok();
  });
  // Routes of an input run in order, so the doubled value proves the sink ran.
  auto loop =
      std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, sink, {}}, {0, Doubler(), {0}}});
  ASSERT_TRUE(harness.Start(std::move(loop), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(5)).ok());
  EXPECT_EQ(ReceiveInt(harness), 10);
  EXPECT_EQ(sunk.load(), 5);
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(FreeRunningLoopTest, StatusRepliesCountErrors) {
  NodeHarness harness;
  auto even_only = MakeLogicHandler([](const int& value) {
    return std::pair{value, value % 2 == 0 ? Status::Ok() : Status::Error("odd input")};
  });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, even_only, {0}}}),
                         harness.BarrierOptions())
                  .ok());
  for (int i : {1, 3, 2}) ASSERT_TRUE(harness.Send(0, MakeMessage(i)).ok());
  EXPECT_EQ(ReceiveInt(harness), 2);  // all three inputs are processed by now
  ASSERT_TRUE(harness.Command(ControlType::kQueryStatus, 6).ok());
  auto reply = harness.AwaitEvent(StatusEvent::kStatusReply);
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->invocations, 3u);
  EXPECT_EQ(reply->errors, 2u);
}

TEST(FreeRunningLoopTest, FinishDrainsQueuedInputEvenWhilePaused) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Command(ControlType::kPause, 1).ok());
  ASSERT_TRUE(harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 1; }));
  for (int i = 1; i <= 3; ++i) ASSERT_TRUE(harness.Send(0, MakeMessage(i)).ok());
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);

  ASSERT_TRUE(harness.Command(ControlType::kFinish, 2).ok());
  for (int i = 1; i <= 3; ++i) EXPECT_EQ(ReceiveInt(harness), 2 * i);
  EXPECT_TRUE(harness.Join().ok());
  EXPECT_EQ(harness.node().state(), NodeState::kStopped);
  auto stopped = harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 2; });
  ASSERT_TRUE(stopped);
  EXPECT_EQ(stopped->state, NodeState::kStopped);
  EXPECT_EQ(stopped->invocations, 3u);
}

TEST(FreeRunningLoopTest, FinishWorksThroughABacklog) {
  NodeHarness harness;
  auto slow = MakeLogicHandler([](const int& value) {
    std::this_thread::sleep_for(10ms);
    return std::pair{value * 2, Status::Ok()};
  });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, slow, {0}}}),
                         harness.BarrierOptions())
                  .ok());
  for (int i = 1; i <= 5; ++i) ASSERT_TRUE(harness.Send(0, MakeMessage(i)).ok());
  ASSERT_TRUE(harness.Command(ControlType::kFinish, 1).ok());  // while most input is queued
  for (int i = 1; i <= 5; ++i) EXPECT_EQ(ReceiveInt(harness), 2 * i);
  EXPECT_TRUE(harness.Join().ok());
  EXPECT_EQ(harness.node().core().invocations(), 5u);
}

TEST(FreeRunningLoopTest, FinishFailsTheNodeWhenInputNeverStops) {
  NodeHarness harness;
  ASSERT_TRUE(harness
                  .Start(std::make_unique<FreeRunningLoop>(
                             std::vector<Route>{{0, Doubler(), {0}}},
                             FreeRunningLoopOptions{.drain_quiet = 50ms, .drain_timeout = 200ms}),
                         harness.BarrierOptions())
                  .ok());
  // A producer that keeps sending, faster than the quiet period.
  std::jthread producer([&harness](std::stop_token stop_token) {
    for (int i = 0; !stop_token.stop_requested(); ++i) {
      (void)harness.Send(0, MakeMessage(i));
      std::this_thread::sleep_for(5ms);
    }
  });
  ASSERT_TRUE(harness.Command(ControlType::kFinish, 1).ok());
  const Status result = harness.Join();  // returns although input keeps arriving
  producer.request_stop();
  producer.join();

  EXPECT_EQ(result.code(), StatusCode::kTimeout) << result;
  EXPECT_EQ(harness.node().state(), NodeState::kFailed);
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->state, NodeState::kFailed);
  EXPECT_EQ(error->code, StatusCode::kTimeout);
}

// --- Control and status -------------------------------------------------------------------------

TEST(NodeControlTest, PauseHoldsInputUntilStart) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Command(ControlType::kPause, 1).ok());
  auto paused = harness.AwaitEvent(StatusEvent::kStateChanged);
  ASSERT_TRUE(paused);
  EXPECT_EQ(paused->state, NodeState::kPaused);
  EXPECT_EQ(paused->sequence, 1u);

  ASSERT_TRUE(harness.Send(0, MakeMessage(4)).ok());
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);

  ASSERT_TRUE(harness.Command(ControlType::kStart, 2).ok());
  EXPECT_EQ(ReceiveInt(harness), 8);
}

TEST(NodeControlTest, StartPausedWaitsForStart) {
  NodeHarness harness;
  NodeCoreOptions options = harness.BarrierOptions();
  options.start_paused = true;
  ASSERT_TRUE(harness.Start(DoublerLoop(), options).ok());
  EXPECT_EQ(harness.node().state(), NodeState::kPaused);
  ASSERT_TRUE(harness.Send(0, MakeMessage(1)).ok());
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
  ASSERT_TRUE(harness.Command(ControlType::kStart).ok());
  EXPECT_EQ(ReceiveInt(harness), 2);
}

TEST(NodeControlTest, IdleNodesAcceptPublishersOnBindingInputs) {
  for (const bool triggered : {false, true}) {
    NodeHarness harness(HarnessConfig{.bind_inputs = true});
    NodeCoreOptions options = harness.BarrierOptions();
    options.start_paused = !triggered;  // paused, or waiting for a trigger
    std::unique_ptr<NodeLoop> loop =
        triggered ? std::unique_ptr<NodeLoop>(
                        std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}}))
                  : DoublerLoop();
    ASSERT_TRUE(harness.Start(std::move(loop), options).ok()) << "triggered=" << triggered;
    ASSERT_TRUE(harness.Send(0, MakeMessage(7)).ok());
    ASSERT_TRUE(harness.Command(triggered ? ControlType::kTrigger : ControlType::kStart).ok());
    EXPECT_EQ(ReceiveInt(harness), 14);
  }
}

TEST(NodeControlTest, StopCommandEndsTheRun) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Command(ControlType::kStop, 9).ok());
  auto stopped = harness.AwaitEvent(StatusEvent::kStateChanged);
  ASSERT_TRUE(stopped);
  EXPECT_EQ(stopped->state, NodeState::kStopped);
  EXPECT_EQ(stopped->sequence, 9u);
  EXPECT_TRUE(harness.Join().ok());
}

TEST(NodeControlTest, QueryStatusReportsCounters) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(harness.Send(0, MakeMessage(i)).ok());
    ReceiveInt(harness);
  }
  ASSERT_TRUE(harness.Command(ControlType::kQueryStatus, 5).ok());
  auto reply = harness.AwaitEvent(StatusEvent::kStatusReply);
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->sequence, 5u);
  EXPECT_EQ(reply->state, NodeState::kRunning);
  EXPECT_EQ(reply->invocations, 3u);
  EXPECT_EQ(reply->errors, 0u);
}

TEST(NodeControlTest, CommandsForOtherNodesAreIgnored) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions(), "alpha").ok());
  ASSERT_TRUE(harness.Command(ControlType::kPause, 1, "beta").ok());
  ASSERT_TRUE(harness.Command(ControlType::kQueryStatus, 2, "alpha").ok());
  auto reply = harness.AwaitStatus([](const NodeStatus& s) { return s.sequence != 0; });
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->sequence, 2u);  // the Pause for beta produced no report
  EXPECT_EQ(reply->event, StatusEvent::kStatusReply);
  EXPECT_EQ(reply->state, NodeState::kRunning);
  EXPECT_EQ(reply->node, "alpha");
}

TEST(NodeControlTest, TriggerOnFreeRunningNodeIsRejected) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 3).ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->code, StatusCode::kUnsupported);
  EXPECT_EQ(error->sequence, 3u);
}

TEST(NodeControlTest, StopTokenWorksWithoutControlSocket) {
  NodeHarness harness(HarnessConfig{.control = false, .status = false});
  NodeCoreOptions options;
  options.min_output_subscribers = {1};
  ASSERT_TRUE(harness.Start(DoublerLoop(), options).ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(2)).ok());
  EXPECT_EQ(ReceiveInt(harness), 4);
  EXPECT_TRUE(harness.Stop().ok());
}

TEST(NodeControlTest, StartWhileRunningIsOnlyAcknowledged) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Command(ControlType::kStart, 3).ok());
  auto reply = harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 3; });
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->event, StatusEvent::kStatusReply);
  EXPECT_EQ(reply->state, NodeState::kRunning);
}

TEST(NodeControlTest, StopWhilePausedEndsTheRun) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.Command(ControlType::kPause, 1).ok());
  ASSERT_TRUE(harness.AwaitEvent(StatusEvent::kStateChanged));
  ASSERT_TRUE(harness.Command(ControlType::kStop, 2).ok());
  EXPECT_TRUE(harness.Join().ok());
  EXPECT_EQ(harness.node().state(), NodeState::kStopped);
  auto stopped = harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 2; });
  ASSERT_TRUE(stopped);
  EXPECT_EQ(stopped->state, NodeState::kStopped);
}

TEST(NodeControlTest, ForeignControlPayloadsAreReported) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(DoublerLoop(), harness.BarrierOptions()).ok());
  ASSERT_TRUE(harness.SendControl(MakeMessage(5)).ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->code, StatusCode::kTypeMismatch);
  ASSERT_TRUE(harness.Send(0, MakeMessage(4)).ok());
  EXPECT_EQ(ReceiveInt(harness), 8);
  EXPECT_EQ(harness.node().state(), NodeState::kRunning);
}

// --- Source loops -------------------------------------------------------------------------------

std::shared_ptr<SourceHandlerBase> CountTo(int last) {
  return MakeSourceHandler([next = 0, last]() mutable {
    const int value = next++;
    return std::pair{value, value > last ? Status::EndOfStream() : Status::Ok()};
  });
}

TEST(FreeRunningSourceLoopTest, ProducesUntilEndOfStream) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningSourceLoop>(CountTo(4), std::vector<std::size_t>{0}),
                 harness.BarrierOptions())
          .ok());
  for (int i = 0; i <= 4; ++i) EXPECT_EQ(ReceiveInt(harness), i);
  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(FreeRunningSourceLoopTest, HonoursPeriod) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  const auto start = std::chrono::steady_clock::now();
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningSourceLoop>(CountTo(4), std::vector<std::size_t>{0},
                                                         FreeRunningSourceOptions{.period = 20ms}),
                 harness.BarrierOptions())
          .ok());
  for (int i = 0; i <= 4; ++i) EXPECT_EQ(ReceiveInt(harness), i);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 80ms);
  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
}

TEST(TriggeredSourceLoopTest, ProducesOncePerTrigger) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<TriggeredSourceLoop>(CountTo(100), std::vector<std::size_t>{0}),
                 harness.BarrierOptions())
          .ok());
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);

  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 7).ok());
  EXPECT_EQ(ReceiveInt(harness), 0);
  auto step = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->sequence, 7u);
  EXPECT_EQ(step->code, StatusCode::kOk);

  for (std::uint64_t seq = 10; seq < 13; ++seq) {
    ASSERT_TRUE(harness.Command(ControlType::kTrigger, seq).ok());
  }
  for (int i = 1; i <= 3; ++i) EXPECT_EQ(ReceiveInt(harness), i);
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(TriggeredSourceLoopTest, TriggersWhilePausedAreRejectedUntilStart) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  NodeCoreOptions options = harness.BarrierOptions();
  options.start_paused = true;
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<TriggeredSourceLoop>(CountTo(100), std::vector<std::size_t>{0}),
                 options)
          .ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->sequence, 1u);
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);

  ASSERT_TRUE(harness.Command(ControlType::kStart, 2).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 3).ok());
  EXPECT_EQ(ReceiveInt(harness), 0);
}

TEST(FreeRunningSourceLoopTest, PauseHoldsProductionAndStartResumesIt) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  NodeCoreOptions options = harness.BarrierOptions();
  options.start_paused = true;
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningSourceLoop>(CountTo(1000), std::vector<std::size_t>{0},
                                                         FreeRunningSourceOptions{.period = 5ms}),
                 options)
          .ok());
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);

  ASSERT_TRUE(harness.Command(ControlType::kStart, 1).ok());
  EXPECT_EQ(ReceiveInt(harness), 0);
  EXPECT_EQ(ReceiveInt(harness), 1);
  ASSERT_TRUE(harness.Command(ControlType::kPause, 2).ok());
  auto paused = harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 2; });
  ASSERT_TRUE(paused);
  EXPECT_EQ(paused->state, NodeState::kPaused);
  int last = 1;
  while (true) {  // what was produced before the pause took effect
    auto [message, status] = harness.Receive(0, kQuiet);
    if (!status.ok()) break;
    last = *message->As<int>();
  }

  ASSERT_TRUE(harness.Command(ControlType::kStart, 3).ok());
  EXPECT_EQ(ReceiveInt(harness), last + 1);  // nothing was produced, or lost, while paused
}

TEST(FreeRunningSourceLoopTest, ReportsSourceErrorsAndKeepsProducing) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  auto evens = MakeSourceHandler([next = 0]() mutable {
    const int value = next++;
    if (value == 5) return std::pair{value, Status::EndOfStream()};
    return std::pair{value, value % 2 == 0 ? Status::Ok() : Status::Error("odd")};
  });
  NodeCoreOptions options = harness.BarrierOptions();
  options.start_paused = true;  // the source must not finish before Start() returns
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningSourceLoop>(evens, std::vector<std::size_t>{0}),
                 options)
          .ok());
  ASSERT_TRUE(harness.Command(ControlType::kStart).ok());
  for (int i : {0, 2, 4}) EXPECT_EQ(ReceiveInt(harness), i);
  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
  EXPECT_EQ(harness.node().core().errors(), 2u);
  EXPECT_EQ(harness.node().core().invocations(), 6u);
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->detail, "odd");
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(FreeRunningSourceLoopTest, NeedMoreDataProducesNothing) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  auto every_third = MakeSourceHandler([next = 0]() mutable {
    const int value = next++;
    if (value > 6) return std::pair{value, Status::EndOfStream()};
    return std::pair{value, value % 3 == 0 ? Status::Ok() : Status::NeedMoreData()};
  });
  NodeCoreOptions options = harness.BarrierOptions();
  options.start_paused = true;
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningSourceLoop>(every_third, std::vector<std::size_t>{0}),
                 options)
          .ok());
  ASSERT_TRUE(harness.Command(ControlType::kStart).ok());
  for (int i : {0, 3, 6}) EXPECT_EQ(ReceiveInt(harness), i);
  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
  EXPECT_EQ(harness.node().core().errors(), 0u);
}

TEST(FreeRunningSourceLoopTest, StopCommandEndsProduction) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningSourceLoop>(CountTo(1000), std::vector<std::size_t>{0},
                                                         FreeRunningSourceOptions{.period = 5ms}),
                 harness.BarrierOptions())
          .ok());
  EXPECT_EQ(ReceiveInt(harness), 0);
  ASSERT_TRUE(harness.Command(ControlType::kStop, 4).ok());
  EXPECT_TRUE(harness.Join().ok());
  auto stopped = harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 4; });
  ASSERT_TRUE(stopped);
  EXPECT_EQ(stopped->state, NodeState::kStopped);
}

TEST(TriggeredSourceLoopTest, EndOfStreamCompletesTheStepAndStopsTheNode) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<TriggeredSourceLoop>(CountTo(0), std::vector<std::size_t>{0}),
                 harness.BarrierOptions())
          .ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  EXPECT_EQ(ReceiveInt(harness), 0);
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 2).ok());
  auto step = harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 2; });
  ASSERT_TRUE(step);
  EXPECT_EQ(step->event, StatusEvent::kStepCompleted);
  EXPECT_EQ(step->code, StatusCode::kEndOfStream);
  EXPECT_TRUE(harness.AwaitEvent(StatusEvent::kEndOfStream));
  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
}

TEST(TriggeredSourceLoopTest, StepsReportSourceErrors) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  auto broken = MakeSourceHandler([] { return std::pair{0, Status::Error("sensor offline")}; });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredSourceLoop>(broken, std::vector<std::size_t>{0}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 5).ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->detail, "sensor offline");
  auto step = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->sequence, 5u);
  EXPECT_EQ(step->code, StatusCode::kError);
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
  EXPECT_EQ(harness.node().state(), NodeState::kRunning);
}

TEST(NodeControlTest, FinishStopsTriggeredAndSourceNodesRightAway) {
  {
    NodeHarness harness;
    ASSERT_TRUE(harness
                    .Start(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}}),
                           harness.BarrierOptions())
                    .ok());
    ASSERT_TRUE(harness.Send(0, MakeMessage(5)).ok());
    ASSERT_TRUE(harness.Command(ControlType::kFinish, 1).ok());
    EXPECT_TRUE(harness.Join().ok());
    EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);  // not triggered
  }
  {
    NodeHarness harness(HarnessConfig{.inputs = 0});
    ASSERT_TRUE(harness
                    .Start(std::make_unique<FreeRunningSourceLoop>(
                               CountTo(1000), std::vector<std::size_t>{0},
                               FreeRunningSourceOptions{.period = 5ms}),
                           harness.BarrierOptions())
                    .ok());
    EXPECT_EQ(ReceiveInt(harness), 0);
    ASSERT_TRUE(harness.Command(ControlType::kFinish, 2).ok());
    EXPECT_TRUE(harness.Join().ok());  // Ok, not EndOfStream: the node was told to finish
    EXPECT_EQ(harness.node().state(), NodeState::kStopped);
  }
}

// --- Backpressure -------------------------------------------------------------------------------

std::shared_ptr<zeromq::ZmqMessageQueue> SmallQueues() {
  return std::make_shared<zeromq::ZmqMessageQueue>(
      zeromq::ZmqOptions{.send_high_water_mark = 4, .receive_high_water_mark = 4});
}

TEST(BackpressureNodeTest, BlockedSourceWaitsForItsConsumerAndLosesNothing) {
  NodeHarness harness(HarnessConfig{.inputs = 0}, SmallQueues());
  ASSERT_TRUE(harness
                  .Start(std::make_unique<FreeRunningSourceLoop>(CountTo(1'000'000),
                                                                 std::vector<std::size_t>{0}),
                         harness.BarrierOptions())
                  .ok());
  std::this_thread::sleep_for(100ms);  // nobody reads: the source blocks once the output is full

  // Blocked, the source still answers control commands, and it has not run ahead.
  ASSERT_TRUE(harness.Command(ControlType::kQueryStatus, 1).ok());
  auto blocked = harness.AwaitEvent(StatusEvent::kStatusReply);
  ASSERT_TRUE(blocked);
  EXPECT_LE(blocked->invocations, 12u);  // the queue holds 8, plus the message that waits
  EXPECT_EQ(blocked->errors, 0u);

  for (int i = 0; i < 100; ++i) ASSERT_EQ(ReceiveInt(harness), i);  // no gaps

  ASSERT_TRUE(harness.Command(ControlType::kStop, 2).ok());  // while blocked again
  const auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(harness.Join().ok());
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
}

TEST(BackpressureNodeTest, HandlerSkipsWhatTheConsumerHasNoRoomFor) {
  NodeHarness harness(HarnessConfig{.inputs = 0}, SmallQueues());
  NodeCoreOptions options = harness.BarrierOptions();
  options.send_timeout = 30ms;
  options.on_backpressure = [](const BackpressureEvent&) { return BackpressureAction::kSkip; };
  ASSERT_TRUE(harness
                  .Start(std::make_unique<FreeRunningSourceLoop>(CountTo(1'000'000),
                                                                 std::vector<std::size_t>{0}),
                         options)
                  .ok());
  std::this_thread::sleep_for(150ms);  // several send timeouts pass

  // What was queued arrives in order; after it, values are missing: they were skipped.
  int expected = 0;
  bool gap = false;
  for (int i = 0; i < 50 && !gap; ++i) {
    const int value = ReceiveInt(harness);
    gap = value != expected;
    expected = value + 1;
  }
  EXPECT_TRUE(gap);
  ASSERT_TRUE(harness.Command(ControlType::kQueryStatus, 1).ok());
  auto reply = harness.AwaitEvent(StatusEvent::kStatusReply);
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->errors, 0u);
  EXPECT_TRUE(harness.Stop().ok());
}

TEST(BackpressureNodeTest, WithoutAHandlerWhatGotNoRoomIsReported) {
  NodeHarness harness(HarnessConfig{.inputs = 0}, SmallQueues());
  NodeCoreOptions options = harness.BarrierOptions();
  options.send_timeout = 30ms;
  ASSERT_TRUE(harness
                  .Start(std::make_unique<FreeRunningSourceLoop>(CountTo(1'000'000),
                                                                 std::vector<std::size_t>{0}),
                         options)
                  .ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->code, StatusCode::kTimeout);
  EXPECT_NE(error->detail.find("output 0 had no room for 30 ms"), std::string::npos)
      << error->detail;
  EXPECT_EQ(error->state, NodeState::kRunning);  // the node carries on
  EXPECT_TRUE(harness.Stop().ok());
}

// --- TriggeredLoop ------------------------------------------------------------------------------

TEST(TriggeredLoopTest, ProcessesInputOnlyWhenTriggered) {
  NodeHarness harness;
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(5)).ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(6)).ok());
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);

  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  EXPECT_EQ(ReceiveInt(harness), 10);
  auto step = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->sequence, 1u);
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);

  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 2).ok());
  EXPECT_EQ(ReceiveInt(harness), 12);
}

TEST(TriggeredLoopTest, TriggerWaitsForInput) {
  NodeHarness harness;
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
  ASSERT_TRUE(harness.Send(0, MakeMessage(8)).ok());
  EXPECT_EQ(ReceiveInt(harness), 16);
}

TEST(TriggeredLoopTest, StepTimesOutWithoutInput) {
  NodeHarness harness;
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}},
                                                 TriggeredLoopOptions{.input_timeout = 30ms}),
                 harness.BarrierOptions())
          .ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 4).ok());
  auto step = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->sequence, 4u);
  EXPECT_EQ(step->code, StatusCode::kTimeout);
}

TEST(TriggeredLoopTest, QueuedTriggersAreServedInOrder) {
  NodeHarness harness;
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 2).ok());

  ASSERT_TRUE(harness.Send(0, MakeMessage(3)).ok());
  EXPECT_EQ(ReceiveInt(harness), 6);
  auto first = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(first);
  EXPECT_EQ(first->sequence, 1u);

  ASSERT_TRUE(harness.Send(0, MakeMessage(4)).ok());
  EXPECT_EQ(ReceiveInt(harness), 8);
  auto second = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(second);
  EXPECT_EQ(second->sequence, 2u);
}

TEST(TriggeredLoopTest, PauseDiscardsPendingTriggers) {
  NodeHarness harness;
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());  // waits for input
  ASSERT_TRUE(harness.Command(ControlType::kPause, 2).ok());
  ASSERT_TRUE(harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 2; }));
  ASSERT_TRUE(harness.Command(ControlType::kStart, 3).ok());
  ASSERT_TRUE(harness.AwaitStatus([](const NodeStatus& s) { return s.sequence == 3; }));

  ASSERT_TRUE(harness.Send(0, MakeMessage(5)).ok());
  EXPECT_EQ(harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);  // trigger 1 is gone
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 4).ok());
  EXPECT_EQ(ReceiveInt(harness), 10);
  auto step = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->sequence, 4u);  // the discarded trigger was never acknowledged
}

TEST(TriggeredLoopTest, OneStepServesEveryReadyInput) {
  NodeHarness harness(HarnessConfig{.inputs = 2, .outputs = 2});
  auto negate = MakeLogicHandler([](const int& value) { return std::pair{-value, Status::Ok()}; });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredLoop>(
                             std::vector<Route>{{0, Doubler(), {0}}, {1, negate, {1}}}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(2)).ok());
  ASSERT_TRUE(harness.Send(1, MakeMessage(3)).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  EXPECT_EQ(ReceiveInt(harness, 0), 4);
  EXPECT_EQ(ReceiveInt(harness, 1), -3);
  auto step = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->sequence, 1u);
  EXPECT_FALSE(harness.AwaitEvent(StatusEvent::kStepCompleted, kQuiet));  // a single step
}

TEST(TriggeredLoopTest, StepReportsLogicFailures) {
  NodeHarness harness;
  auto even_only = MakeLogicHandler([](const int& value) {
    return std::pair{value, value % 2 == 0 ? Status::Ok() : Status::Error("odd input")};
  });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, even_only, {0}}}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(3)).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->detail, "odd input");
  auto step = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->sequence, 1u);
  EXPECT_EQ(step->code, StatusCode::kError);
}

TEST(TriggeredLoopTest, EndOfStreamFromLogicStopsTheNode) {
  NodeHarness harness;
  auto until_zero = MakeLogicHandler([](const int& value) {
    return std::pair{value, value == 0 ? Status::EndOfStream() : Status::Ok()};
  });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, until_zero, {0}}}),
                         harness.BarrierOptions())
                  .ok());
  ASSERT_TRUE(harness.Send(0, MakeMessage(0)).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  auto step = harness.AwaitEvent(StatusEvent::kStepCompleted);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->code, StatusCode::kEndOfStream);
  EXPECT_TRUE(harness.AwaitEvent(StatusEvent::kEndOfStream));
  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
  EXPECT_EQ(harness.node().state(), NodeState::kStopped);
}

// --- Validation and startup ---------------------------------------------------------------------

Status RunToCompletion(NodeHarness& harness, std::unique_ptr<NodeLoop> loop,
                       NodeCoreOptions options = {}) {
  harness.Launch(std::move(loop), std::move(options));
  return harness.Join();
}

TEST(NodeValidationTest, TriggeredLoopsNeedControl) {
  NodeHarness harness(HarnessConfig{.control = false});
  EXPECT_EQ(RunToCompletion(
                harness, std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}}))
                .code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(harness.node().state(), NodeState::kFailed);
}

TEST(NodeValidationTest, RoutesMustMatchSockets) {
  {
    NodeHarness harness;
    EXPECT_EQ(RunToCompletion(harness, std::make_unique<FreeRunningLoop>(
                                           std::vector<Route>{{0, Doubler(), {1}}}))
                  .code(),
              StatusCode::kInvalidArgument);
  }
  {
    NodeHarness harness;
    EXPECT_EQ(RunToCompletion(harness, std::make_unique<FreeRunningLoop>(
                                           std::vector<Route>{{1, Doubler(), {0}}}))
                  .code(),
              StatusCode::kInvalidArgument);
  }
  {
    NodeHarness harness(HarnessConfig{.inputs = 2});
    EXPECT_EQ(RunToCompletion(harness, std::make_unique<FreeRunningLoop>(
                                           std::vector<Route>{{0, Doubler(), {0}}}))
                  .code(),
              StatusCode::kInvalidArgument);  // input 1 has no route
  }
  {
    NodeHarness harness;
    EXPECT_EQ(RunToCompletion(
                  harness, std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, nullptr, {0}}}))
                  .code(),
              StatusCode::kInvalidArgument);
  }
}

TEST(NodeValidationTest, SourcesHaveNoInputs) {
  NodeHarness harness;
  EXPECT_EQ(RunToCompletion(harness, std::make_unique<FreeRunningSourceLoop>(
                                         CountTo(1), std::vector<std::size_t>{0}))
                .code(),
            StatusCode::kInvalidArgument);
}

TEST(NodeValidationTest, StartupBarrierTimesOut) {
  NodeHarness harness;
  NodeCoreOptions options;
  options.min_output_subscribers = {2};  // only the harness subscribes
  options.startup_timeout = 50ms;
  EXPECT_EQ(RunToCompletion(harness, DoublerLoop(), options).code(), StatusCode::kTimeout);
  EXPECT_EQ(harness.node().state(), NodeState::kFailed);
}

TEST(NodeValidationTest, StartupFailureIsReportedOnStatusSocket) {
  NodeHarness harness;
  NodeCoreOptions options = harness.BarrierOptions();
  options.min_output_subscribers = {1};
  harness.Launch(std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, Doubler(), {3}}}),
                 options);
  EXPECT_EQ(harness.Join().code(), StatusCode::kInvalidArgument);
}

TEST(NodeValidationTest, BadEndpointsFailStartup) {
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  auto sockets = std::make_unique<SocketManager>(
      queue, SocketLayout{
                 .inputs = {"nonsense"}, .outputs = {"inproc://ok"}, .control = {}, .status = {}});
  Node node("bad", std::make_unique<NodeCore>(std::move(sockets), DoublerLoop()));
  EXPECT_EQ(node.Run().code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(node.state(), NodeState::kFailed);
}

// --- Remote node --------------------------------------------------------------------------------

TEST(RemoteNodeTest, NodeRunsEntirelyOverTcp) {
  // The node and the test use separate ZeroMQ contexts, as separate processes would.
  zeromq::ZmqMessageQueue test_side;
  auto node_side = std::make_shared<zeromq::ZmqMessageQueue>();
  ASSERT_TRUE(test_side.codecs().RegisterTrivial<int>().ok());
  ASSERT_TRUE(node_side->codecs().RegisterTrivial<int>().ok());
  ASSERT_TRUE(RegisterControlCodecs(test_side.codecs()).ok());

  // Every test-side socket binds a wildcard port; the node connects to the resolved address.
  auto input = test_side.CreatePublisher("tcp://127.0.0.1:*").first;
  auto control = test_side.CreatePublisher("tcp://127.0.0.1:*").first;
  auto output = test_side.CreateSubscriber("@tcp://127.0.0.1:*").first;
  auto status = test_side.CreateSubscriber("@tcp://127.0.0.1:*").first;
  ASSERT_TRUE(input && control && output && status);

  SocketLayout layout{.inputs = {input->endpoints()[0].address},
                      .outputs = {">" + output->endpoints()[0].address},
                      .control = control->endpoints()[0].address,
                      .status = ">" + status->endpoints()[0].address};
  NodeCoreOptions options;
  options.min_output_subscribers = {1};
  options.min_status_subscribers = 1;
  Node node("remote", std::make_unique<NodeCore>(
                          std::make_unique<SocketManager>(node_side, std::move(layout)),
                          DoublerLoop(), std::move(options)));
  std::optional<Status> result;
  std::jthread thread([&](std::stop_token stop_token) { result = node.Run(stop_token); });

  // Bound subscribers accept the node's connecting publishers only while they are serviced.
  auto poller = test_side.CreatePoller();
  ASSERT_TRUE(poller->Add(*output, PollInterest::kServiceOnly).second.ok());
  ASSERT_TRUE(poller->Add(*status).second.ok());
  std::vector<std::size_t> ready;
  const auto deadline = std::chrono::steady_clock::now() + kTestTimeout;
  while (ready.empty() && std::chrono::steady_clock::now() < deadline) {
    (void)poller->Wait(10ms, ready);
  }
  ASSERT_TRUE(input->WaitForSubscribers(1, kTestTimeout).ok());
  ASSERT_TRUE(control->WaitForSubscribers(1, kTestTimeout).ok());
  auto [running, running_status] = status->Receive(kTestTimeout);
  ASSERT_TRUE(running_status.ok()) << running_status;
  ASSERT_TRUE(running->IsType<NodeStatus>());
  EXPECT_EQ(running->As<NodeStatus>()->state, NodeState::kRunning);

  ASSERT_TRUE(input->Send(MakeMessage(21)).ok());
  auto [doubled, doubled_status] = output->Receive(kTestTimeout);
  ASSERT_TRUE(doubled_status.ok()) << doubled_status;
  EXPECT_EQ(*doubled->As<int>(), 42);

  ASSERT_TRUE(control->Send(MakeMessage(ControlCommand{ControlType::kStop, "remote", 11})).ok());
  thread.join();
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->ok()) << *result;
  auto [stopped, stopped_status] = status->Receive(kTestTimeout);
  ASSERT_TRUE(stopped_status.ok()) << stopped_status;
  EXPECT_EQ(stopped->As<NodeStatus>()->state, NodeState::kStopped);
  EXPECT_EQ(stopped->As<NodeStatus>()->sequence, 11u);
}

}  // namespace
}  // namespace mloom
