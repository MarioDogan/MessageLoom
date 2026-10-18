#include "mloom/node/node_core.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "mloom/node/node.hpp"
#include "mloom/node/testing/node_harness.hpp"
#include "mloom/node/testing/scripted_loop.hpp"
#include "mloom/transport/zmq/zmq_message_queue.hpp"

namespace mloom {
namespace {

using namespace std::chrono_literals;
using testing::HarnessConfig;
using testing::kTestTimeout;
using testing::NodeHarness;
using testing::ScriptedLoop;

constexpr Timeout kQuiet = 100ms;  // how long "nothing happens" is observed

using Report = std::tuple<StatusEvent, NodeState, std::uint64_t>;

/// Every report still queued on the harness's status socket. Call after the node finished.
std::vector<NodeStatus> AllReports(NodeHarness& harness) {
  std::vector<NodeStatus> reports;
  while (auto report = harness.AwaitStatus([](const NodeStatus&) { return true; }, kQuiet)) {
    reports.push_back(*report);
  }
  return reports;
}

/// Event, state and sequence of every queued report.
std::vector<Report> Reports(NodeHarness& harness) {
  std::vector<Report> summary;
  for (const NodeStatus& report : AllReports(harness)) {
    summary.emplace_back(report.event, report.state, report.sequence);
  }
  return summary;
}

std::unique_ptr<ScriptedLoop> Script(ScriptedLoop::RunFn run, bool triggered = false) {
  return std::make_unique<ScriptedLoop>(std::move(run), triggered);
}

/// Runs `run` as the loop of a node named "node" and waits for the node to finish. Options without
/// an output barrier are replaced by the harness's barrier options, so no report is lost.
Status RunScript(NodeHarness& harness, ScriptedLoop::RunFn run, bool triggered = false,
                 NodeCoreOptions options = {}) {
  if (options.min_output_subscribers.empty()) options = harness.BarrierOptions();
  harness.Launch(Script(std::move(run), triggered), std::move(options));
  return harness.Join();
}

constexpr auto kChanged = StatusEvent::kStateChanged;
constexpr auto kReply = StatusEvent::kStatusReply;
constexpr auto kRunning = NodeState::kRunning;
constexpr auto kPaused = NodeState::kPaused;
constexpr auto kStopped = NodeState::kStopped;

// --- Lifecycle ----------------------------------------------------------------------------------

TEST(NodeCoreTest, RunNeedsSocketsAndALoop) {
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  NodeHarness harness({}, queue);
  NodeCore without_sockets(nullptr, Script(nullptr));
  EXPECT_EQ(without_sockets.Run("node", {}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(without_sockets.state(), NodeState::kCreated);

  NodeCore without_loop(std::make_unique<SocketManager>(queue, harness.layout()), nullptr);
  EXPECT_EQ(without_loop.Run("node", {}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(without_loop.state(), NodeState::kCreated);
}

TEST(NodeCoreTest, ExposesItsParts) {
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  NodeHarness harness(HarnessConfig{.inputs = 2, .outputs = 3}, queue);
  NodeCoreOptions options;
  options.poll_interval = Timeout(7);
  NodeCore core(std::make_unique<SocketManager>(queue, harness.layout()), Script(nullptr), options);
  EXPECT_EQ(core.sockets().input_count(), 2u);
  EXPECT_EQ(core.sockets().output_count(), 3u);
  EXPECT_EQ(core.loop().name(), "ScriptedLoop");
  EXPECT_EQ(core.options().poll_interval, Timeout(7));
  EXPECT_EQ(core.state(), NodeState::kCreated);
  EXPECT_EQ(core.invocations(), 0u);
  EXPECT_EQ(core.errors(), 0u);
}

TEST(NodeCoreTest, ReportsStateChangesAroundTheLoop) {
  NodeHarness harness;
  NodeState state_in_loop = NodeState::kCreated;
  bool sockets_open_in_loop = false;
  EXPECT_TRUE(RunScript(harness, [&](LoopContext& context) {
                state_in_loop = context.state();
                sockets_open_in_loop = context.sockets().is_open();
                return Status::Ok();
              }).ok());
  EXPECT_EQ(state_in_loop, kRunning);
  EXPECT_TRUE(sockets_open_in_loop);
  EXPECT_EQ(harness.node().state(), kStopped);
  EXPECT_FALSE(harness.node().core().sockets().is_open());

  const std::vector<NodeStatus> reports = AllReports(harness);
  ASSERT_EQ(reports.size(), 2u);
  EXPECT_EQ(reports[0].node, "node");
  EXPECT_EQ(Report(reports[0].event, reports[0].state, reports[0].sequence),
            Report(kChanged, kRunning, 0));
  EXPECT_EQ(Report(reports[1].event, reports[1].state, reports[1].sequence),
            Report(kChanged, kStopped, 0));
  EXPECT_EQ(reports[1].code, StatusCode::kOk);
}

TEST(NodeCoreTest, StartPausedEntersTheLoopPaused) {
  NodeHarness harness;
  NodeCoreOptions options = harness.BarrierOptions();
  options.start_paused = true;
  NodeState state_in_loop = NodeState::kCreated;
  EXPECT_TRUE(RunScript(
                  harness,
                  [&](LoopContext& context) {
                    state_in_loop = context.state();
                    return Status::Ok();
                  },
                  false, options)
                  .ok());
  EXPECT_EQ(state_in_loop, kPaused);
  EXPECT_EQ(Reports(harness),
            (std::vector<Report>{{kChanged, kPaused, 0}, {kChanged, kStopped, 0}}));
}

TEST(NodeCoreTest, PrepareFailureFailsTheNodeWithoutRunningTheLoop) {
  NodeHarness harness;
  bool ran = false;
  harness.Launch(
      std::make_unique<ScriptedLoop>(
          [&](LoopContext&) {
            ran = true;
            return Status::Ok();
          },
          false, [](const SocketManager&) { return Status::InvalidArgument("bad wiring"); }),
      harness.BarrierOptions());
  EXPECT_EQ(harness.Join(), Status::InvalidArgument("bad wiring"));
  EXPECT_FALSE(ran);
  EXPECT_EQ(harness.node().state(), NodeState::kFailed);
  EXPECT_FALSE(harness.node().core().sockets().is_open());
}

TEST(NodeCoreTest, LoopFailureFailsTheNode) {
  NodeHarness harness;
  EXPECT_EQ(RunScript(harness, [](LoopContext&) { return Status::Closed("transport gone"); }),
            Status::Closed("transport gone"));
  EXPECT_EQ(harness.node().state(), NodeState::kFailed);
  EXPECT_EQ(harness.node().core().errors(), 1u);

  const std::vector<NodeStatus> reports = AllReports(harness);
  ASSERT_EQ(reports.size(), 2u);
  EXPECT_EQ(reports[1].event, StatusEvent::kError);
  EXPECT_EQ(reports[1].state, NodeState::kFailed);
  EXPECT_EQ(reports[1].code, StatusCode::kClosed);
  EXPECT_EQ(reports[1].detail, "transport gone");
  EXPECT_EQ(reports[1].errors, 1u);
}

TEST(NodeCoreTest, EndOfStreamStopsTheNode) {
  NodeHarness harness;
  EXPECT_EQ(RunScript(harness, [](LoopContext&) { return Status::EndOfStream("done"); }).code(),
            StatusCode::kEndOfStream);
  EXPECT_EQ(harness.node().state(), kStopped);
  EXPECT_EQ(harness.node().core().errors(), 0u);
  const std::vector<NodeStatus> reports = AllReports(harness);
  ASSERT_EQ(reports.size(), 2u);
  EXPECT_EQ(reports[1].event, StatusEvent::kEndOfStream);
  EXPECT_EQ(reports[1].state, kStopped);
  EXPECT_EQ(reports[1].code, StatusCode::kEndOfStream);
}

TEST(NodeCoreTest, RejectsASecondConcurrentRun) {
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  NodeHarness harness({}, queue);
  NodeCore core(std::make_unique<SocketManager>(queue, harness.layout()),
                Script(&ScriptedLoop::ServeControl));
  std::optional<Status> result;
  std::jthread thread([&](std::stop_token stop_token) { result = core.Run("node", stop_token); });

  const auto deadline = std::chrono::steady_clock::now() + kTestTimeout;
  while (core.state() != kRunning && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(core.state(), kRunning);
  const Status second = core.Run("again", {});
  EXPECT_EQ(second.code(), StatusCode::kInvalidArgument);
  EXPECT_NE(second.message().find("already running"), std::string::npos) << second;

  thread.request_stop();
  thread.join();
  ASSERT_TRUE(result);
  EXPECT_TRUE(result->ok()) << *result;
}

// --- Startup barrier ----------------------------------------------------------------------------

TEST(NodeCoreTest, BarrierSettingsMustMatchTheSockets) {
  {
    NodeHarness harness;
    NodeCoreOptions options;
    options.min_output_subscribers = {1, 1};  // the node has one output
    EXPECT_EQ(RunScript(harness, nullptr, false, options).code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(harness.node().state(), NodeState::kFailed);
  }
  {
    NodeHarness harness(HarnessConfig{.status = false});
    NodeCoreOptions options;
    options.min_output_subscribers = {1};
    options.min_status_subscribers = 1;
    EXPECT_EQ(RunScript(harness, nullptr, false, options).code(), StatusCode::kInvalidArgument);
  }
}

TEST(NodeCoreTest, StopRequestedBeforeRunSkipsTheStartupBarrier) {
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  NodeHarness harness({}, queue);
  NodeCoreOptions options;
  options.min_output_subscribers = {5};  // never satisfied
  bool loop_saw_stop = false;
  NodeCore core(std::make_unique<SocketManager>(queue, harness.layout()),
                Script([&](LoopContext& context) {
                  loop_saw_stop = context.stop_requested();
                  return Status::Ok();
                }),
                options);
  std::stop_source stop;
  stop.request_stop();
  EXPECT_TRUE(core.Run("node", stop.get_token()).ok());  // not a barrier Timeout
  EXPECT_TRUE(loop_saw_stop);
  EXPECT_EQ(core.state(), kStopped);
}

// --- Control commands ---------------------------------------------------------------------------

TEST(NodeCoreTest, StartAndPauseSwitchStatesAndAcknowledgeNoOps) {
  NodeHarness harness;
  std::vector<NodeState> states;
  std::vector<bool> accepted;
  EXPECT_TRUE(RunScript(harness, [&](LoopContext& context) {
                const std::pair<ControlType, std::uint64_t> commands[] = {{ControlType::kPause, 1},
                                                                          {ControlType::kPause, 2},
                                                                          {ControlType::kStart, 3},
                                                                          {ControlType::kStart, 4}};
                for (const auto& [type, sequence] : commands) {
                  accepted.push_back(context.ApplyControl(ControlCommand{type, "", sequence}));
                  states.push_back(context.state());
                }
                return Status::Ok();
              }).ok());
  EXPECT_EQ(states, (std::vector<NodeState>{kPaused, kPaused, kRunning, kRunning}));
  EXPECT_EQ(accepted, (std::vector<bool>{false, false, false, false}));
  EXPECT_EQ(Reports(harness), (std::vector<Report>{{kChanged, kRunning, 0},
                                                   {kChanged, kPaused, 1},
                                                   {kReply, kPaused, 2},
                                                   {kChanged, kRunning, 3},
                                                   {kReply, kRunning, 4},
                                                   {kChanged, kStopped, 0}}));
}

TEST(NodeCoreTest, CommandsForOtherNodesAreIgnoredSilently) {
  NodeHarness harness;
  bool accepted = true;
  NodeState state_after = NodeState::kCreated;
  EXPECT_TRUE(RunScript(harness, [&](LoopContext& context) {
                accepted = context.ApplyControl(ControlCommand{ControlType::kTrigger, "other", 1});
                (void)context.ApplyControl(ControlCommand{ControlType::kPause, "other", 2});
                state_after = context.state();
                (void)context.ApplyControl(ControlCommand{ControlType::kQueryStatus, "node", 3});
                (void)context.ApplyControl(ControlCommand{ControlType::kQueryStatus, "", 4});
                return Status::Ok();
              }).ok());
  EXPECT_FALSE(accepted);
  EXPECT_EQ(state_after, kRunning);
  EXPECT_EQ(Reports(harness), (std::vector<Report>{{kChanged, kRunning, 0},
                                                   {kReply, kRunning, 3},
                                                   {kReply, kRunning, 4},
                                                   {kChanged, kStopped, 0}}));
}

TEST(NodeCoreTest, StopCommandIsEchoedInTheFinalReport) {
  NodeHarness harness;
  bool before = true;
  bool accepted = true;
  bool after = false;
  EXPECT_TRUE(RunScript(harness, [&](LoopContext& context) {
                before = context.stop_requested();
                accepted = context.ApplyControl(ControlCommand{ControlType::kStop, "", 7});
                after = context.stop_requested();
                return Status::Ok();
              }).ok());
  EXPECT_FALSE(before);
  EXPECT_FALSE(accepted);
  EXPECT_TRUE(after);
  EXPECT_EQ(Reports(harness),
            (std::vector<Report>{{kChanged, kRunning, 0}, {kChanged, kStopped, 7}}));
}

TEST(NodeCoreTest, FinishEndsTheLoopAndDrainsBeforeStopping) {
  NodeHarness harness;
  bool finishing_before = true;
  bool accepted = true;
  bool stop_after = false;
  bool finishing_after = false;
  bool drained_while_finishing = false;
  harness.Launch(std::make_unique<ScriptedLoop>(
                     [&](LoopContext& context) {
                       finishing_before = context.finishing();
                       accepted = context.ApplyControl(ControlCommand{ControlType::kFinish, "", 7});
                       stop_after = context.stop_requested();
                       finishing_after = context.finishing();
                       return Status::Ok();
                     },
                     false, nullptr,
                     [&](LoopContext& context) {
                       drained_while_finishing = context.finishing();
                       return Status::Ok();
                     }),
                 harness.BarrierOptions());
  EXPECT_TRUE(harness.Join().ok());
  EXPECT_FALSE(finishing_before);
  EXPECT_FALSE(accepted);
  EXPECT_TRUE(stop_after);
  EXPECT_TRUE(finishing_after);
  EXPECT_TRUE(drained_while_finishing);
  EXPECT_EQ(Reports(harness),
            (std::vector<Report>{{kChanged, kRunning, 0}, {kChanged, kStopped, 7}}));
}

TEST(NodeCoreTest, StopAfterFinishSkipsTheDrain) {
  NodeHarness harness;
  bool finishing = true;
  bool drained = false;
  harness.Launch(std::make_unique<ScriptedLoop>(
                     [&](LoopContext& context) {
                       (void)context.ApplyControl(ControlCommand{ControlType::kFinish, "", 7});
                       (void)context.ApplyControl(ControlCommand{ControlType::kStop, "", 8});
                       finishing = context.finishing();
                       return Status::Ok();
                     },
                     false, nullptr,
                     [&](LoopContext&) {
                       drained = true;
                       return Status::Ok();
                     }),
                 harness.BarrierOptions());
  EXPECT_TRUE(harness.Join().ok());
  EXPECT_FALSE(finishing);
  EXPECT_FALSE(drained);
  EXPECT_EQ(Reports(harness),
            (std::vector<Report>{{kChanged, kRunning, 0}, {kChanged, kStopped, 8}}));
}

TEST(NodeCoreTest, DrainFailureFailsTheNode) {
  NodeHarness harness;
  harness.Launch(std::make_unique<ScriptedLoop>(
                     [](LoopContext& context) {
                       (void)context.ApplyControl(ControlCommand{ControlType::kFinish, "", 1});
                       return Status::Ok();
                     },
                     false, nullptr, [](LoopContext&) { return Status::Closed("gone"); }),
                 harness.BarrierOptions());
  EXPECT_EQ(harness.Join(), Status::Closed("gone"));
  EXPECT_EQ(harness.node().state(), NodeState::kFailed);
}

TEST(NodeCoreTest, TriggersAreAcceptedOnlyByRunningTriggeredLoops) {
  NodeHarness harness;
  std::vector<bool> accepted;
  EXPECT_TRUE(
      RunScript(
          harness,
          [&](LoopContext& context) {
            accepted.push_back(context.ApplyControl(ControlCommand{ControlType::kTrigger, "", 1}));
            (void)context.ApplyControl(ControlCommand{ControlType::kPause, "", 2});
            accepted.push_back(context.ApplyControl(ControlCommand{ControlType::kTrigger, "", 3}));
            return Status::Ok();
          },
          /*triggered=*/true)
          .ok());
  EXPECT_EQ(accepted, (std::vector<bool>{true, false}));
  EXPECT_EQ(harness.node().core().errors(), 1u);

  const std::vector<NodeStatus> reports = AllReports(harness);
  ASSERT_EQ(reports.size(), 4u);  // Running, Paused, the rejected trigger, Stopped
  EXPECT_EQ(reports[2].event, StatusEvent::kError);
  EXPECT_EQ(reports[2].sequence, 3u);
  EXPECT_EQ(reports[2].code, StatusCode::kInvalidArgument);
  EXPECT_NE(reports[2].detail.find("Paused"), std::string::npos) << reports[2].detail;
}

TEST(NodeCoreTest, TriggersAreUnsupportedByFreeRunningLoops) {
  NodeHarness harness;
  bool accepted = true;
  EXPECT_TRUE(RunScript(harness, [&](LoopContext& context) {
                accepted = context.ApplyControl(ControlCommand{ControlType::kTrigger, "", 5});
                return Status::Ok();
              }).ok());
  EXPECT_FALSE(accepted);
  const std::vector<NodeStatus> reports = AllReports(harness);
  ASSERT_EQ(reports.size(), 3u);
  EXPECT_EQ(reports[1].event, StatusEvent::kError);
  EXPECT_EQ(reports[1].sequence, 5u);
  EXPECT_EQ(reports[1].code, StatusCode::kUnsupported);
  EXPECT_NE(reports[1].detail.find("ScriptedLoop"), std::string::npos) << reports[1].detail;
}

// --- Counters and reports -----------------------------------------------------------------------

TEST(NodeCoreTest, ReportsCarryTheCounters) {
  NodeHarness harness;
  EXPECT_TRUE(RunScript(harness, [](LoopContext& context) {
                for (int i = 0; i < 3; ++i) context.CountInvocation();
                context.ReportError(Status::Error("oops"));
                (void)context.ApplyControl(ControlCommand{ControlType::kQueryStatus, "", 9});
                context.ReportStepCompleted(4, Status::Timeout("slow"));
                return Status::Ok();
              }).ok());
  EXPECT_EQ(harness.node().core().invocations(), 3u);
  EXPECT_EQ(harness.node().core().errors(), 1u);

  const std::vector<NodeStatus> reports = AllReports(harness);
  ASSERT_EQ(reports.size(), 5u);
  const NodeStatus& error = reports[1];
  EXPECT_EQ(error.event, StatusEvent::kError);
  EXPECT_EQ(error.code, StatusCode::kError);
  EXPECT_EQ(error.detail, "oops");
  EXPECT_EQ(error.sequence, 0u);
  EXPECT_EQ(error.invocations, 3u);
  EXPECT_EQ(error.errors, 1u);
  const NodeStatus& reply = reports[2];
  EXPECT_EQ(reply.event, StatusEvent::kStatusReply);
  EXPECT_EQ(reply.sequence, 9u);
  EXPECT_EQ(reply.invocations, 3u);
  EXPECT_EQ(reply.errors, 1u);
  const NodeStatus& step = reports[3];
  EXPECT_EQ(step.event, StatusEvent::kStepCompleted);
  EXPECT_EQ(step.sequence, 4u);
  EXPECT_EQ(step.code, StatusCode::kTimeout);
  EXPECT_EQ(step.detail, "slow");
  EXPECT_EQ(step.errors, 1u);  // a failed step is not counted as an error
  EXPECT_EQ(reports[4].invocations, 3u);
}

TEST(NodeCoreTest, BackpressureHandlerIsInjectedThroughTheOptions) {
  {
    NodeHarness harness;
    NodeCoreOptions options = harness.BarrierOptions();
    options.send_timeout = Timeout(1234);
    std::string node;  // the handler runs on the node's thread; read after Join()
    std::size_t output = 0;
    options.on_backpressure = [&](const BackpressureEvent& event) {
      node = std::string(event.node);
      output = event.output;
      return BackpressureAction::kRetry;
    };
    Timeout send_timeout{};
    BackpressureAction action = BackpressureAction::kSkip;
    EXPECT_TRUE(RunScript(
                    harness,
                    [&](LoopContext& context) {
                      send_timeout = context.send_timeout();
                      action = context.OnBackpressure(
                          BackpressureEvent{{}, 2, MakeMessage(1), Timeout(1234), 1});
                      return Status::Ok();
                    },
                    false, options)
                    .ok());
    EXPECT_EQ(send_timeout, Timeout(1234));
    EXPECT_EQ(action, BackpressureAction::kRetry);
    EXPECT_EQ(node, "node");
    EXPECT_EQ(output, 2u);
  }
  {
    // Defaults: wait one minute, then drop and report.
    NodeHarness harness;
    Timeout send_timeout{};
    BackpressureAction action = BackpressureAction::kSkip;
    EXPECT_TRUE(RunScript(harness, [&](LoopContext& context) {
                  send_timeout = context.send_timeout();
                  action = context.OnBackpressure(BackpressureEvent{});
                  return Status::Ok();
                }).ok());
    EXPECT_EQ(send_timeout, std::chrono::seconds(60));
    EXPECT_EQ(action, BackpressureAction::kReportError);
  }
}

// --- Node ---------------------------------------------------------------------------------------

TEST(NodeTest, RunNeedsACoreAndAName) {
  Node coreless("lonely", nullptr);
  EXPECT_EQ(coreless.name(), "lonely");
  EXPECT_EQ(coreless.state(), NodeState::kFailed);
  EXPECT_EQ(coreless.Run().code(), StatusCode::kInvalidArgument);

  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  NodeHarness harness({}, queue);
  Node unnamed("", std::make_unique<NodeCore>(
                       std::make_unique<SocketManager>(queue, harness.layout()), Script(nullptr)));
  EXPECT_EQ(unnamed.Run().code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(unnamed.state(), NodeState::kCreated);
}

TEST(NodeTest, MovedNodeKeepsNameAndCore) {
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  NodeHarness harness({}, queue);
  Node original("mover",
                std::make_unique<NodeCore>(std::make_unique<SocketManager>(queue, harness.layout()),
                                           Script(nullptr)));
  Node moved(std::move(original));
  EXPECT_EQ(moved.name(), "mover");
  EXPECT_EQ(moved.state(), NodeState::kCreated);
  EXPECT_EQ(original.state(), NodeState::kFailed);  // a moved-from node has no core
  EXPECT_TRUE(moved.Run().ok());
  EXPECT_EQ(moved.state(), kStopped);
}

}  // namespace
}  // namespace mloom
