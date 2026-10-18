#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mloom/node/free_running_loop.hpp"
#include "mloom/node/free_running_source_loop.hpp"
#include "mloom/node/logic_handler.hpp"
#include "mloom/node/node.hpp"
#include "mloom/node/testing/acme_integration.hpp"
#include "mloom/node/testing/node_harness.hpp"
#include "mloom/node/testing/third_party/acme_sensors.hpp"
#include "mloom/node/triggered_loop.hpp"
#include "mloom/node/triggered_source_loop.hpp"
#include "mloom/transport/zmq/zmq_message_queue.hpp"

// Third-party (acme) code integrated with lambdas and run inside real nodes, while control
// commands move the nodes through their states. The acme state captured by the lambdas lives on
// the node's thread: the tests observe it only through outputs and status reports, or after the
// node's thread has ended.

namespace mloom_test {
/// Integrator-defined message that reconfigures an acme::Calibrator.
struct Calibration {
  double gain = 1.0;
  double offset = 0.0;
};
}  // namespace mloom_test

namespace mloom {
namespace {

using namespace std::chrono_literals;
using mloom_test::Calibration;
using testing::DecodeHandler;
using testing::HarnessConfig;
using testing::MakeReading;
using testing::NodeHarness;
using testing::ParseHandler;
using testing::Payload;
using Lines = std::vector<std::string>;

constexpr Timeout kQuiet = 100ms;  // how long "nothing happens" is observed
constexpr auto kRunning = NodeState::kRunning;
constexpr auto kPaused = NodeState::kPaused;
constexpr auto kStopped = NodeState::kStopped;

std::unique_ptr<NodeLoop> FreeRunning(std::vector<Route> routes) {
  return std::make_unique<FreeRunningLoop>(std::move(routes));
}

std::unique_ptr<NodeLoop> Triggered(std::vector<Route> routes) {
  return std::make_unique<TriggeredLoop>(std::move(routes));
}

/// Next message on `output` as a T, or a default T when none arrives.
template <typename T>
T Receive(NodeHarness& harness, std::size_t output = 0) {
  auto [message, status] = harness.Receive(output);
  EXPECT_TRUE(status.ok()) << status;
  if (!status.ok()) return T{};
  const T* value = message->As<T>();
  EXPECT_NE(value, nullptr) << "unexpected payload " << message->type().name();
  return value ? *value : T{};
}

/// True when nothing arrives on `output` for a while.
bool Quiet(NodeHarness& harness, std::size_t output = 0) {
  return harness.Receive(output, kQuiet).second.code() == StatusCode::kTimeout;
}

/// The report caused by the command with `sequence`.
std::optional<NodeStatus> ReportFor(NodeHarness& harness, std::uint64_t sequence) {
  return harness.AwaitStatus([sequence](const NodeStatus& s) { return s.sequence == sequence; });
}

std::optional<NodeStatus> StepReport(NodeHarness& harness, std::uint64_t sequence) {
  return harness.AwaitStatus([sequence](const NodeStatus& s) {
    return s.event == StatusEvent::kStepCompleted && s.sequence == sequence;
  });
}

std::optional<NodeStatus> Query(NodeHarness& harness, std::uint64_t sequence) {
  if (!harness.Command(ControlType::kQueryStatus, sequence).ok()) return std::nullopt;
  return harness.AwaitStatus([sequence](const NodeStatus& s) {
    return s.event == StatusEvent::kStatusReply && s.sequence == sequence;
  });
}

// --- Free-running nodes -------------------------------------------------------------------------

TEST(ThirdPartyNodeTest, SmoothingNodeGoesThroughEveryState) {
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  NodeHarness harness({}, queue);
  // The test keeps a pointer only to look at the average once the node's thread has ended.
  auto average = std::make_shared<acme::MovingAverage>(3);
  auto smooth = MakeLogicHandler([average](const double& value) {
    average->Add(value);
    if (!average->full()) return std::pair{0.0, Status::NeedMoreData()};
    return std::pair{average->average(), Status::Ok()};
  });
  Node node("smoother",
            std::make_unique<NodeCore>(std::make_unique<SocketManager>(queue, harness.layout()),
                                       FreeRunning({{0, smooth, {0}}}), harness.BarrierOptions()));

  // Created: nothing runs yet.
  EXPECT_EQ(node.state(), NodeState::kCreated);
  std::optional<Status> result;
  std::jthread thread([&](std::stop_token stop_token) { result = node.Run(stop_token); });

  // Running: the average fills up, then follows every value.
  auto running = harness.AwaitEvent(StatusEvent::kStateChanged);
  ASSERT_TRUE(running);
  EXPECT_EQ(running->state, kRunning);
  EXPECT_EQ(running->node, "smoother");
  ASSERT_TRUE(harness.AwaitSubscribers().ok());
  for (const double value : {1.0, 2.0, 3.0}) ASSERT_TRUE(harness.Send(0, MakeMessage(value)).ok());
  EXPECT_EQ(Receive<double>(harness), 2.0);

  // Paused: inputs wait in their queue and the third-party code does not run.
  ASSERT_TRUE(harness.Command(ControlType::kPause, 1).ok());
  auto paused = ReportFor(harness, 1);
  ASSERT_TRUE(paused);
  EXPECT_EQ(paused->state, kPaused);
  EXPECT_EQ(node.state(), kPaused);
  for (const double value : {4.0, 5.0}) ASSERT_TRUE(harness.Send(0, MakeMessage(value)).ok());
  EXPECT_TRUE(Quiet(harness));
  auto while_paused = Query(harness, 2);
  ASSERT_TRUE(while_paused);
  EXPECT_EQ(while_paused->state, kPaused);
  EXPECT_EQ(while_paused->invocations, 3u);

  // Running again: the queued inputs continue the average where it stood.
  ASSERT_TRUE(harness.Command(ControlType::kStart, 3).ok());
  auto resumed = ReportFor(harness, 3);
  ASSERT_TRUE(resumed);
  EXPECT_EQ(resumed->state, kRunning);
  EXPECT_EQ(Receive<double>(harness), 3.0);  // (2 + 3 + 4) / 3
  EXPECT_EQ(Receive<double>(harness), 4.0);  // (3 + 4 + 5) / 3

  // Stopped: Run() returns and the final report echoes the Stop command.
  ASSERT_TRUE(harness.Command(ControlType::kStop, 4).ok());
  thread.join();
  ASSERT_TRUE(result);
  EXPECT_TRUE(result->ok()) << *result;
  EXPECT_EQ(node.state(), kStopped);
  auto stopped = ReportFor(harness, 4);
  ASSERT_TRUE(stopped);
  EXPECT_EQ(stopped->state, kStopped);
  EXPECT_EQ(stopped->invocations, 5u);
  EXPECT_EQ(average->count(), 5u);  // safe now: the node's thread has ended
}

TEST(ThirdPartyNodeTest, ParsingNodeReportsBadInputAndKeepsRunning) {
  NodeHarness harness;
  ASSERT_TRUE(
      harness.Start(FreeRunning({{0, ParseHandler(), {0}}}), harness.BarrierOptions(), "parser")
          .ok());
  for (const char* line : {"boiler=70", "nonsense", "mixer=40"}) {
    ASSERT_TRUE(harness.Send(0, MakeMessage(std::string(line))).ok());
  }
  EXPECT_EQ(Receive<acme::Reading>(harness).sensor, "boiler");
  EXPECT_EQ(Receive<acme::Reading>(harness).sensor, "mixer");

  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->node, "parser");
  EXPECT_EQ(error->state, kRunning);
  EXPECT_EQ(error->code, StatusCode::kInvalidArgument);
  EXPECT_EQ(error->detail, "cannot parse 'nonsense'");
  auto reply = Query(harness, 1);
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->state, kRunning);
  EXPECT_EQ(reply->invocations, 3u);
  EXPECT_EQ(reply->errors, 1u);
}

TEST(ThirdPartyNodeTest, CalibratorIsReconfiguredThroughASecondInput) {
  NodeHarness harness(HarnessConfig{.inputs = 2, .outputs = 2});
  // Both routes run on the node's thread, so they share the calibrator without a lock.
  auto calibrator = std::make_shared<acme::Calibrator>();
  auto calibrate = MakeLogicHandler([calibrator](acme::Reading&& reading) {
    reading.celsius = calibrator->Apply(reading.celsius);
    return std::pair{std::move(reading), Status::Ok()};
  });
  auto configure = MakeLogicHandler([calibrator](const Calibration& update) {
    calibrator->Configure(update.gain, update.offset);
    return std::pair{update, Status::Ok()};  // acknowledged on output 1
  });
  calibrator.reset();  // the routes own it from here on
  ASSERT_TRUE(
      harness
          .Start(FreeRunning({{0, calibrate, {0}}, {1, configure, {1}}}), harness.BarrierOptions())
          .ok());

  ASSERT_TRUE(harness.Send(0, MakeMessage(MakeReading("boiler", 20.0))).ok());
  EXPECT_EQ(Receive<acme::Reading>(harness, 0).celsius, 20.0);
  ASSERT_TRUE(harness.Send(1, MakeMessage(Calibration{2.0, -1.0})).ok());
  EXPECT_EQ(Receive<Calibration>(harness, 1).gain, 2.0);
  ASSERT_TRUE(harness.Send(0, MakeMessage(MakeReading("boiler", 20.0))).ok());
  EXPECT_EQ(Receive<acme::Reading>(harness, 0).celsius, 39.0);

  // A configuration sent while paused waits like any other input.
  ASSERT_TRUE(harness.Command(ControlType::kPause, 1).ok());
  ASSERT_TRUE(ReportFor(harness, 1));
  ASSERT_TRUE(harness.Send(1, MakeMessage(Calibration{1.0, 10.0})).ok());
  EXPECT_TRUE(Quiet(harness, 1));
  ASSERT_TRUE(harness.Command(ControlType::kStart, 2).ok());
  EXPECT_EQ(Receive<Calibration>(harness, 1).offset, 10.0);
  ASSERT_TRUE(harness.Send(0, MakeMessage(MakeReading("boiler", 20.0))).ok());
  EXPECT_EQ(Receive<acme::Reading>(harness, 0).celsius, 30.0);
}

TEST(ThirdPartyNodeTest, RecorderNodeEndsTheStreamWhenFull) {
  NodeHarness harness(HarnessConfig{.outputs = 0});
  auto recorder = std::make_shared<acme::Recorder>(2);
  // The integration decides that a full recorder ends this node's work, rather than an error.
  auto record = MakeLogicHandler([recorder](acme::Reading&& reading) {
    if (!recorder->Write(std::move(reading))) return Status::EndOfStream("recorder is full");
    return Status::Ok();
  });
  ASSERT_TRUE(harness.Start(FreeRunning({{0, record, {}}}), harness.BarrierOptions()).ok());
  for (const char* sensor : {"boiler", "mixer", "pump"}) {
    ASSERT_TRUE(harness.Send(0, MakeMessage(MakeReading(sensor, 50.0))).ok());
  }

  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
  EXPECT_EQ(harness.node().state(), kStopped);
  EXPECT_TRUE(harness.AwaitEvent(StatusEvent::kEndOfStream));
  EXPECT_EQ(harness.node().core().invocations(), 3u);
  EXPECT_EQ(harness.node().core().errors(), 0u);
  ASSERT_EQ(recorder->records().size(), 2u);  // safe now: the node's thread has ended
  EXPECT_EQ(recorder->records()[1].sensor, "mixer");
}

TEST(ThirdPartyNodeTest, StartPausedNodeRunsNoThirdPartyCodeUntilStarted) {
  NodeHarness harness;
  auto split =
      MakeLogicHandler([splitter = acme::LineSplitter()](const std::string& chunk) mutable {
        splitter.Feed(chunk);
        Lines lines = splitter.TakeLines();
        const Status status = lines.empty() ? Status::NeedMoreData() : Status::Ok();
        return std::pair{std::move(lines), status};
      });
  NodeCoreOptions options = harness.BarrierOptions();
  options.start_paused = true;
  ASSERT_TRUE(harness.Start(FreeRunning({{0, split, {0}}}), options).ok());
  EXPECT_EQ(harness.node().state(), kPaused);

  ASSERT_TRUE(harness.Send(0, MakeMessage(std::string("boiler=1\nmix"))).ok());
  EXPECT_TRUE(Quiet(harness));
  auto reply = Query(harness, 1);
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->state, kPaused);
  EXPECT_EQ(reply->invocations, 0u);

  ASSERT_TRUE(harness.Command(ControlType::kStart, 2).ok());
  EXPECT_EQ(Receive<Lines>(harness), Lines{"boiler=1"});
  ASSERT_TRUE(harness.Send(0, MakeMessage(std::string("er=2\n"))).ok());
  EXPECT_EQ(Receive<Lines>(harness), Lines{"mixer=2"});  // "mix" was kept between messages
}

// --- Triggered nodes ----------------------------------------------------------------------------

TEST(ThirdPartyNodeTest, TriggeredDetectorWorksOnlyOnCommand) {
  NodeHarness harness;
  auto detector = std::make_shared<acme::JumpDetector>(5.0);
  auto jumps = std::make_shared<std::vector<acme::Jump>>();
  detector->OnJump([jumps](const acme::Jump& jump) { jumps->push_back(jump); });
  auto detect = MakeLogicHandler([detector, jumps](const acme::Reading& reading) {
    jumps->clear();
    detector->Process(reading);
    const Status status = jumps->empty() ? Status::NeedMoreData() : Status::Ok();
    return std::pair{std::move(*jumps), status};
  });
  ASSERT_TRUE(
      harness.Start(Triggered({{0, detect, {0}}}), harness.BarrierOptions(), "detector").ok());

  // Running, but a triggered node only works when told to.
  ASSERT_TRUE(harness.Send(0, MakeMessage(MakeReading("boiler", 20.0))).ok());
  EXPECT_TRUE(Quiet(harness));
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  auto first = StepReport(harness, 1);
  ASSERT_TRUE(first);
  EXPECT_EQ(first->code, StatusCode::kNeedMoreData);  // first value of the sensor
  EXPECT_TRUE(Quiet(harness));

  ASSERT_TRUE(harness.Send(0, MakeMessage(MakeReading("boiler", 30.0))).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 2).ok());
  const auto found = Receive<std::vector<acme::Jump>>(harness);
  ASSERT_EQ(found.size(), 1u);
  EXPECT_EQ(found[0].from, 20.0);
  EXPECT_EQ(found[0].to, 30.0);
  auto second = StepReport(harness, 2);
  ASSERT_TRUE(second);
  EXPECT_EQ(second->code, StatusCode::kOk);

  // Paused: triggers are refused and inputs wait.
  ASSERT_TRUE(harness.Command(ControlType::kPause, 3).ok());
  auto paused = ReportFor(harness, 3);
  ASSERT_TRUE(paused);
  EXPECT_EQ(paused->state, kPaused);
  ASSERT_TRUE(harness.Send(0, MakeMessage(MakeReading("boiler", 31.0))).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 4).ok());
  auto refused = ReportFor(harness, 4);
  ASSERT_TRUE(refused);
  EXPECT_EQ(refused->event, StatusEvent::kError);
  EXPECT_EQ(refused->code, StatusCode::kInvalidArgument);

  // Running again: the next trigger takes the reading that waited.
  ASSERT_TRUE(harness.Command(ControlType::kStart, 5).ok());
  ASSERT_TRUE(ReportFor(harness, 5));
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 6).ok());
  auto third = StepReport(harness, 6);
  ASSERT_TRUE(third);
  EXPECT_EQ(third->code, StatusCode::kNeedMoreData);  // 30 -> 31 is no jump
  auto reply = Query(harness, 7);
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->invocations, 3u);
  EXPECT_EQ(reply->errors, 1u);  // the refused trigger

  ASSERT_TRUE(harness.Command(ControlType::kStop, 8).ok());
  EXPECT_TRUE(harness.Join().ok());
  auto stopped = ReportFor(harness, 8);
  ASSERT_TRUE(stopped);
  EXPECT_EQ(stopped->state, kStopped);
}

TEST(ThirdPartyNodeTest, TriggeredDecoderReportsThirdPartyFailures) {
  NodeHarness harness;
  ASSERT_TRUE(harness.Start(Triggered({{0, DecodeHandler(), {0}}}), harness.BarrierOptions()).ok());

  ASSERT_TRUE(harness.Send(0, MakeMessage(acme::Frame{9, {1, 2, 4}})).ok());  // bad checksum
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 1).ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->code, StatusCode::kInvalidArgument);
  EXPECT_EQ(error->detail, "frame 9 has a bad checksum");
  auto failed_step = StepReport(harness, 1);
  ASSERT_TRUE(failed_step);
  EXPECT_EQ(failed_step->code, StatusCode::kInvalidArgument);

  ASSERT_TRUE(harness.Send(0, MakeMessage(acme::Frame{10, {1, 2, 3}})).ok());
  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 2).ok());
  EXPECT_EQ(Receive<Payload>(harness), (Payload{1, 2}));
  auto step = StepReport(harness, 2);
  ASSERT_TRUE(step);
  EXPECT_EQ(step->code, StatusCode::kOk);
  EXPECT_EQ(harness.node().state(), kRunning);  // a failed step does not fail the node
}

// --- Source nodes -------------------------------------------------------------------------------

TEST(ThirdPartyNodeTest, GeneratorNodeStopsItselfAtEndOfStream) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  auto generate =
      MakeSourceHandler([generator = acme::ReadingGenerator("boiler", 20.0, 0.5, 4)]() mutable {
        if (!generator.HasNext()) return std::pair{acme::Reading{}, Status::EndOfStream()};
        return std::pair{generator.Next(), Status::Ok()};
      });
  NodeCoreOptions options = harness.BarrierOptions();
  options.start_paused = true;  // the source must not finish before Start() returns
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningSourceLoop>(generate, std::vector<std::size_t>{0}),
                 options)
          .ok());

  ASSERT_TRUE(harness.Command(ControlType::kStart).ok());
  for (const double celsius : {20.0, 20.5, 21.0, 21.5}) {
    EXPECT_EQ(Receive<acme::Reading>(harness).celsius, celsius);
  }
  EXPECT_EQ(harness.Join().code(), StatusCode::kEndOfStream);
  EXPECT_EQ(harness.node().state(), kStopped);
  EXPECT_TRUE(harness.AwaitEvent(StatusEvent::kEndOfStream));
  EXPECT_EQ(harness.node().core().invocations(), 5u);  // four readings and the end
  EXPECT_TRUE(Quiet(harness));
}

TEST(ThirdPartyNodeTest, GeneratorNodeResumesWhereItWasPaused) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  auto generate =
      MakeSourceHandler([generator = acme::ReadingGenerator("boiler", 0.0, 1.0, 100000)]() mutable {
        if (!generator.HasNext()) return std::pair{acme::Reading{}, Status::EndOfStream()};
        return std::pair{generator.Next(), Status::Ok()};
      });
  ASSERT_TRUE(
      harness
          .Start(std::make_unique<FreeRunningSourceLoop>(generate, std::vector<std::size_t>{0},
                                                         FreeRunningSourceOptions{.period = 5ms}),
                 harness.BarrierOptions())
          .ok());
  EXPECT_EQ(Receive<acme::Reading>(harness).celsius, 0.0);
  EXPECT_EQ(Receive<acme::Reading>(harness).celsius, 1.0);

  ASSERT_TRUE(harness.Command(ControlType::kPause, 1).ok());
  auto paused = ReportFor(harness, 1);
  ASSERT_TRUE(paused);
  EXPECT_EQ(paused->state, kPaused);
  double last = 1.0;
  while (true) {  // readings produced before the pause took effect
    auto [message, status] = harness.Receive(0, kQuiet);
    if (!status.ok()) break;
    last = message->As<acme::Reading>()->celsius;
  }
  auto reply = Query(harness, 2);
  ASSERT_TRUE(reply);
  EXPECT_EQ(reply->state, kPaused);
  EXPECT_EQ(static_cast<double>(reply->invocations), last + 1);  // all of them arrived

  ASSERT_TRUE(harness.Command(ControlType::kStart, 3).ok());
  EXPECT_EQ(Receive<acme::Reading>(harness).celsius, last + 1);  // nothing skipped or repeated
  ASSERT_TRUE(harness.Command(ControlType::kStop, 4).ok());
  EXPECT_TRUE(harness.Join().ok());
}

TEST(ThirdPartyNodeTest, PolledDeviceNodeOnTriggerUntilTheDeviceDropsOut) {
  NodeHarness harness(HarnessConfig{.inputs = 0});
  auto sensor = std::make_shared<acme::PollingSensor>("pump", 2);  // ready on every second poll
  sensor->DisconnectAfter(4);  // configured before the node runs
  auto poll = MakeSourceHandler([sensor] {
    try {
      std::optional<acme::Reading> reading = sensor->Poll();
      if (!reading) return std::pair{acme::Reading{}, Status::NeedMoreData()};
      return std::pair{std::move(*reading), Status::Ok()};
    } catch (const std::exception& error) {
      return std::pair{acme::Reading{}, Status::Error(error.what())};
    }
  });
  ASSERT_TRUE(harness
                  .Start(std::make_unique<TriggeredSourceLoop>(poll, std::vector<std::size_t>{0}),
                         harness.BarrierOptions(), "pump")
                  .ok());

  for (std::uint64_t sequence = 1; sequence <= 4; ++sequence) {
    ASSERT_TRUE(harness.Command(ControlType::kTrigger, sequence).ok());
    if (sequence % 2 == 0) {
      EXPECT_EQ(Receive<acme::Reading>(harness).celsius, sequence == 2 ? 20.0 : 21.0);
    }
    auto step = StepReport(harness, sequence);
    ASSERT_TRUE(step);
    EXPECT_EQ(step->code, sequence % 2 == 0 ? StatusCode::kOk : StatusCode::kNeedMoreData)
        << "trigger " << sequence;
  }

  ASSERT_TRUE(harness.Command(ControlType::kTrigger, 5).ok());
  auto error = harness.AwaitEvent(StatusEvent::kError);
  ASSERT_TRUE(error);
  EXPECT_EQ(error->detail, "sensor 'pump' disconnected");
  auto failed_step = StepReport(harness, 5);
  ASSERT_TRUE(failed_step);
  EXPECT_EQ(failed_step->code, StatusCode::kError);
  EXPECT_EQ(harness.node().state(), kRunning);  // the device failed, not the node

  ASSERT_TRUE(harness.Command(ControlType::kStop, 6).ok());
  EXPECT_TRUE(harness.Join().ok());
}

// --- Failed nodes -------------------------------------------------------------------------------

TEST(ThirdPartyNodeTest, NodeThatFailsToStartNeverRunsThirdPartyCode) {
  NodeHarness harness(HarnessConfig{.control = false});
  auto recorder = std::make_shared<acme::Recorder>(10);
  auto record = MakeLogicHandler([recorder](acme::Reading&& reading) {
    return recorder->Write(std::move(reading)) ? Status::Ok() : Status::Error("recorder is full");
  });
  harness.Launch(Triggered({{0, record, {}}}), harness.BarrierOptions());  // needs control
  EXPECT_EQ(harness.Join().code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(harness.node().state(), NodeState::kFailed);
  EXPECT_FALSE(harness.node().core().sockets().is_open());
  EXPECT_EQ(harness.node().core().invocations(), 0u);
  EXPECT_TRUE(recorder->records().empty());
}

}  // namespace
}  // namespace mloom
