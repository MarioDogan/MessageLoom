#include "mloom/node/node_loop.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mloom/node/free_running_loop.hpp"
#include "mloom/node/free_running_source_loop.hpp"
#include "mloom/node/testing/fake_loop_context.hpp"
#include "mloom/node/testing/node_harness.hpp"
#include "mloom/node/triggered_loop.hpp"
#include "mloom/node/triggered_source_loop.hpp"

namespace mloom_test {
/// Counts copies, to tell moved payloads from copied ones.
struct Counted {
  static inline int copies = 0;
  explicit Counted(std::string v) : value(std::move(v)) {}
  Counted(const Counted& other) : value(other.value) { ++copies; }
  Counted(Counted&&) noexcept = default;
  Counted& operator=(const Counted&) = delete;
  Counted& operator=(Counted&&) = delete;
  std::string value;
};
}  // namespace mloom_test

namespace mloom {
namespace {

using namespace std::chrono_literals;
using mloom_test::Counted;
using testing::FakeLoopContext;
using testing::HarnessConfig;
using testing::kTestTimeout;
using testing::SocketRig;

constexpr Timeout kQuiet = 100ms;  // how long "nothing happens" is observed

/// Makes NodeLoop's protected helpers callable.
struct LoopHelpers : NodeLoop {
  using NodeLoop::AwaitControl;
  using NodeLoop::Dispatch;
  using NodeLoop::DrainControl;
  using NodeLoop::Publish;
  using NodeLoop::ValidateOutputs;
};

/// Makes RoutingLoop's protected parts usable.
class RoutingProbe final : public RoutingLoop {
 public:
  explicit RoutingProbe(std::vector<Route> routes) : RoutingLoop(std::move(routes)) {}
  using RoutingLoop::ProcessInput;

  std::string_view name() const noexcept override { return "RoutingProbe"; }
  bool triggered() const noexcept override { return false; }
  Status Run(LoopContext&) override { return Status::Ok(); }
};

/// Layout for managers that are never opened: only the counts matter.
SocketLayout Layout(std::size_t inputs, std::size_t outputs, bool control = true) {
  SocketLayout layout;
  for (std::size_t i = 0; i < inputs; ++i) layout.inputs.push_back("inproc://in");
  for (std::size_t i = 0; i < outputs; ++i) layout.outputs.push_back("inproc://out");
  if (control) layout.control = "inproc://control";
  return layout;
}

bool Contains(const Status& status, const std::string& text) {
  return status.message().find(text) != std::string::npos;
}

std::shared_ptr<LogicHandlerBase> Doubler() {
  return MakeLogicHandler([](const int& value) { return std::pair{value * 2, Status::Ok()}; });
}

std::shared_ptr<LogicHandlerBase> Failing(std::string reason) {
  return MakeLogicHandler(
      [reason](const int& value) { return std::pair{value, Status::Error(reason)}; });
}

std::shared_ptr<SourceHandlerBase> Constant() {
  return MakeSourceHandler([] { return std::pair{1, Status::Ok()}; });
}

int ReceiveInt(SocketRig& rig, std::size_t output = 0) {
  auto [message, status] = rig.harness.Receive(output);
  EXPECT_TRUE(status.ok()) << status;
  if (!status.ok() || !message->IsType<int>()) return -1;
  return *message->As<int>();
}

// --- Dispatch -----------------------------------------------------------------------------------

TEST(DispatchTest, PublishesOkResultsOnEveryOutput) {
  SocketRig rig(HarnessConfig{.outputs = 2});
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  MessagePtr message = MakeMessage(7);
  EXPECT_TRUE(LoopHelpers::Dispatch(context, {message, Status::Ok()}, {0, 1}).ok());
  EXPECT_EQ(context.invocations, 1u);
  EXPECT_TRUE(context.errors.empty());
  for (const std::size_t output : {std::size_t{0}, std::size_t{1}}) {
    auto [received, status] = rig.harness.Receive(output);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(received.get(), message.get());  // every output shares the one message
  }
}

TEST(DispatchTest, OkWithoutMessagePublishesNothing) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  EXPECT_TRUE(LoopHelpers::Dispatch(context, {nullptr, Status::Ok()}, {0}).ok());
  EXPECT_EQ(context.invocations, 1u);
  EXPECT_TRUE(context.errors.empty());
  EXPECT_EQ(rig.harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(DispatchTest, FlowControlCodesAreNeitherPublishedNorReported) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  for (const Status& flow : {Status::NeedMoreData(), Status::EndOfStream()}) {
    EXPECT_EQ(LoopHelpers::Dispatch(context, {MakeMessage(1), flow}, {0}), flow);
  }
  EXPECT_EQ(context.invocations, 2u);
  EXPECT_TRUE(context.errors.empty());
  EXPECT_EQ(rig.harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(DispatchTest, ReportsLogicFailures) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  EXPECT_EQ(LoopHelpers::Dispatch(context, {nullptr, Status::Error("boom")}, {0}),
            Status::Error("boom"));
  EXPECT_EQ(context.invocations, 1u);
  EXPECT_EQ(context.errors, std::vector<Status>{Status::Error("boom")});
}

TEST(DispatchTest, ReportsPublishFailuresButReturnsTheLogicStatus) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  EXPECT_TRUE(LoopHelpers::Dispatch(context, {MakeMessage(3), Status::Ok()}, {4, 0}).ok());
  ASSERT_EQ(context.errors.size(), 1u);
  EXPECT_EQ(context.errors[0].code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(ReceiveInt(rig), 3);  // the valid output still got the message
}

TEST(DispatchTest, ValidateOutputsNamesTheOwner) {
  SocketManager sockets(nullptr, Layout(0, 1));
  EXPECT_TRUE(LoopHelpers::ValidateOutputs(sockets, {0}, "sensor").ok());
  EXPECT_TRUE(LoopHelpers::ValidateOutputs(sockets, {}, "sensor").ok());
  const Status status = LoopHelpers::ValidateOutputs(sockets, {0, 2}, "sensor");
  EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
  EXPECT_TRUE(Contains(status, "sensor publishes on output 2, but the node has 1 outputs"))
      << status;
}

// --- Backpressure -------------------------------------------------------------------------------

zeromq::ZmqOptions SmallQueues() {
  return zeromq::ZmqOptions{.send_high_water_mark = 2, .receive_high_water_mark = 2};
}

/// Publishes on `output` without waiting until its consumer has no room; returns how many went
/// out.
int FillOutput(SocketRig& rig, std::size_t output = 0) {
  int sent = 0;
  while (sent < 1000 && rig.sockets.Publish(output, MakeMessage(sent)).ok()) ++sent;
  return sent;
}

BackpressureHandler Always(BackpressureAction action) {
  return [action](const BackpressureEvent&) { return action; };
}

TEST(BackpressureTest, PublishWaitsUntilTheConsumerMakesRoom) {
  SocketRig rig({}, SmallQueues());
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  const int queued = FillOutput(rig);
  std::jthread consumer([&rig, queued] {  // only this thread reads the harness output
    std::this_thread::sleep_for(50ms);
    for (int i = 0; i <= queued; ++i) EXPECT_TRUE(rig.harness.Receive(0).second.ok());
  });
  EXPECT_TRUE(LoopHelpers::Publish(context, 0, MakeMessage(-1), nullptr).ok());
  consumer.join();
  EXPECT_TRUE(context.backpressure.empty());  // room came long before the send timeout
}

TEST(BackpressureTest, HandlerDecidesAboutAMessageThatGotNoRoom) {
  SocketRig rig({}, SmallQueues());
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  context.send_limit = 30ms;
  FillOutput(rig);
  MessagePtr message = MakeMessage(-1);

  context.on_backpressure = Always(BackpressureAction::kSkip);
  EXPECT_TRUE(LoopHelpers::Publish(context, 0, message, nullptr).ok());
  ASSERT_EQ(context.backpressure.size(), 1u);
  EXPECT_EQ(context.backpressure[0].output, 0u);
  EXPECT_EQ(context.backpressure[0].attempt, 1u);
  EXPECT_EQ(context.backpressure[0].waited, 30ms);
  EXPECT_EQ(context.backpressure[0].message.get(), message.get());

  context.on_backpressure = Always(BackpressureAction::kReportError);
  const Status reported = LoopHelpers::Publish(context, 0, message, nullptr);
  EXPECT_EQ(reported.code(), StatusCode::kTimeout);
  EXPECT_TRUE(Contains(reported, "output 0 had no room for 30 ms; message dropped")) << reported;

  context.backpressure.clear();
  context.on_backpressure = [](const BackpressureEvent& event) {
    return event.attempt < 3 ? BackpressureAction::kRetry : BackpressureAction::kSkip;
  };
  EXPECT_TRUE(LoopHelpers::Publish(context, 0, message, nullptr).ok());
  ASSERT_EQ(context.backpressure.size(), 3u);
  EXPECT_EQ(context.backpressure[2].attempt, 3u);
}

TEST(BackpressureTest, DispatchReportsOrSkipsWhatGotNoRoom) {
  SocketRig rig({}, SmallQueues());
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  context.send_limit = 20ms;
  FillOutput(rig);

  // Without a handler the message is reported.
  EXPECT_TRUE(LoopHelpers::Dispatch(context, {MakeMessage(1), Status::Ok()}, {0}).ok());
  ASSERT_EQ(context.errors.size(), 1u);
  EXPECT_EQ(context.errors[0].code(), StatusCode::kTimeout);

  context.on_backpressure = Always(BackpressureAction::kSkip);
  EXPECT_TRUE(LoopHelpers::Dispatch(context, {MakeMessage(2), Status::Ok()}, {0}).ok());
  EXPECT_EQ(context.errors.size(), 1u);  // skipped silently
}

TEST(BackpressureTest, StopGivesUpOnABlockedPublishPromptly) {
  SocketRig rig({}, SmallQueues());
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);  // send timeout one minute
  FillOutput(rig);
  ASSERT_TRUE(rig.harness.Command(ControlType::kStop).ok());

  const auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(LoopHelpers::Dispatch(context, {MakeMessage(1), Status::Ok()}, {0}).ok());
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
  EXPECT_TRUE(context.stop_requested());
  EXPECT_TRUE(context.errors.empty());  // giving up while stopping is no error
  EXPECT_TRUE(context.backpressure.empty());
}

TEST(BackpressureTest, ControlIsServedWhileBlocked) {
  SocketRig rig({}, SmallQueues());
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  context.send_limit = 50ms;
  context.on_backpressure = Always(BackpressureAction::kSkip);
  FillOutput(rig);
  ASSERT_TRUE(rig.harness.Command(ControlType::kTrigger, 1).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kPause, 2).ok());

  std::deque<std::uint64_t> triggers;
  EXPECT_TRUE(LoopHelpers::Publish(context, 0, MakeMessage(1), &triggers).ok());
  EXPECT_EQ(context.commands.size(), 2u);
  EXPECT_EQ(context.state(), NodeState::kPaused);
  EXPECT_EQ(triggers, std::deque<std::uint64_t>{1});  // kept for the loop
}

TEST(BackpressureTest, FinishKeepsWaitingForRoom) {
  SocketRig rig({}, SmallQueues());
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  context.send_limit = 50ms;
  context.on_backpressure = Always(BackpressureAction::kSkip);
  FillOutput(rig);
  ASSERT_TRUE(rig.harness.Command(ControlType::kFinish).ok());

  EXPECT_TRUE(LoopHelpers::Publish(context, 0, MakeMessage(1), nullptr).ok());
  EXPECT_TRUE(context.finishing());
  EXPECT_EQ(context.backpressure.size(), 1u);  // waited the whole send timeout
}

// --- Control helpers ----------------------------------------------------------------------------

TEST(ControlHelpersTest, AppliesPendingCommandsAndCollectsTriggers) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  ASSERT_TRUE(rig.harness.Command(ControlType::kTrigger, 1).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kQueryStatus, 2).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kTrigger, 3).ok());

  std::deque<std::uint64_t> triggers;
  ASSERT_TRUE(LoopHelpers::AwaitControl(context, kTestTimeout, &triggers).ok());
  ASSERT_EQ(context.commands.size(), 3u);
  EXPECT_EQ(context.commands[1].type, ControlType::kQueryStatus);
  EXPECT_EQ(triggers, (std::deque<std::uint64_t>{1, 3}));
}

TEST(ControlHelpersTest, TriggersNeedNotBeCollected) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  ASSERT_TRUE(rig.harness.Command(ControlType::kTrigger, 1).ok());
  ASSERT_TRUE(LoopHelpers::AwaitControl(context, kTestTimeout, nullptr).ok());
  EXPECT_EQ(context.commands.size(), 1u);
}

TEST(ControlHelpersTest, ForeignPayloadsAreReportedAndSkipped) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  ASSERT_TRUE(rig.harness.SendControl(MakeMessage(std::string("junk"))).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kQueryStatus, 5).ok());

  ASSERT_TRUE(LoopHelpers::AwaitControl(context, kTestTimeout, nullptr).ok());
  ASSERT_EQ(context.errors.size(), 1u);
  EXPECT_EQ(context.errors[0].code(), StatusCode::kTypeMismatch);
  ASSERT_EQ(context.commands.size(), 1u);
  EXPECT_EQ(context.commands[0].sequence, 5u);
}

TEST(ControlHelpersTest, DrainingStopsOnceAStopIsRequested) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  ASSERT_TRUE(rig.harness.Command(ControlType::kStop, 1).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kPause, 2).ok());

  ASSERT_TRUE(LoopHelpers::AwaitControl(context, kTestTimeout, nullptr).ok());
  ASSERT_EQ(context.commands.size(), 1u);
  EXPECT_TRUE(context.stop_requested());
  EXPECT_EQ(context.state(), NodeState::kRunning);
  auto [pause, status] = rig.sockets.ReceiveControl(kTestTimeout);  // left for later
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(pause.sequence, 2u);
}

TEST(ControlHelpersTest, FinishingKeepsReadingControl) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  ASSERT_TRUE(rig.harness.Command(ControlType::kFinish, 1).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kQueryStatus, 2).ok());

  ASSERT_TRUE(LoopHelpers::AwaitControl(context, kTestTimeout, nullptr).ok());
  EXPECT_EQ(context.commands.size(), 2u);  // unlike after a Stop, so a Stop could still follow
  EXPECT_TRUE(context.stop_requested());
  EXPECT_TRUE(context.finishing());
}

TEST(ControlHelpersTest, QuietControlSocketIsNotAnError) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(1)).ok());
  EXPECT_TRUE(LoopHelpers::AwaitControl(context, Timeout(20), nullptr).ok());
  EXPECT_TRUE(LoopHelpers::DrainControl(context, nullptr).ok());
  EXPECT_TRUE(context.commands.empty());
  EXPECT_TRUE(context.errors.empty());
  EXPECT_TRUE(rig.sockets.Receive(0, kTestTimeout).second.ok());  // input left alone
}

TEST(ControlHelpersTest, NodesWithoutControlSocketHaveNothingToDrain) {
  SocketRig rig(HarnessConfig{.control = false});
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  EXPECT_TRUE(LoopHelpers::DrainControl(context, nullptr).ok());
  EXPECT_TRUE(LoopHelpers::AwaitControl(context, Timeout(10), nullptr).ok());
}

TEST(ControlHelpersTest, TransportFailuresArePropagated) {
  SocketManager closed(nullptr, Layout(1, 1));
  FakeLoopContext context(closed);
  EXPECT_EQ(LoopHelpers::AwaitControl(context, Timeout(10), nullptr).code(), StatusCode::kClosed);
  EXPECT_EQ(LoopHelpers::DrainControl(context, nullptr).code(), StatusCode::kClosed);
  EXPECT_TRUE(context.errors.empty());  // failures are returned, not reported
}

// --- RoutingLoop --------------------------------------------------------------------------------

TEST(RoutingLoopTest, PrepareValidatesRoutes) {
  SocketManager sockets(nullptr, Layout(2, 1));
  const auto prepare = [&sockets](std::vector<Route> routes) {
    return RoutingProbe(std::move(routes)).Prepare(sockets);
  };
  struct Case {
    std::vector<Route> routes;
    std::string error;
  };
  const std::vector<Case> cases = {
      {{}, "RoutingProbe has no routes"},
      {{{0, nullptr, {0}}, {1, Doubler(), {0}}}, "route 0 has no logic handler"},
      {{{0, Doubler(), {0}}, {2, Doubler(), {0}}}, "route 1 reads input 2, but the node has 2"},
      {{{0, Doubler(), {0}}, {1, Doubler(), {0, 1}}}, "route 1 publishes on output 1"},
      {{{0, Doubler(), {0}}}, "input 1 has no route"},
  };
  for (const Case& test : cases) {
    const Status status = prepare(test.routes);
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument) << test.error;
    EXPECT_TRUE(Contains(status, test.error)) << status;
  }
  // Sink routes publish nowhere, and several routes may share an input.
  EXPECT_TRUE(prepare({{0, Doubler(), {}}, {1, Doubler(), {0}}, {1, Doubler(), {0}}}).ok());
}

TEST(RoutingLoopTest, NothingToReceiveIsOk) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  RoutingProbe loop({{0, Doubler(), {0}}});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
  EXPECT_TRUE(loop.ProcessInput(context, 0).ok());
  EXPECT_EQ(context.invocations, 0u);
}

TEST(RoutingLoopTest, RunsEveryRouteOfTheInput) {
  SocketRig rig(HarnessConfig{.inputs = 2, .outputs = 2});
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  auto negate = MakeLogicHandler([](const int& value) { return std::pair{-value, Status::Ok()}; });
  RoutingProbe loop({{0, Doubler(), {0}}, {0, negate, {1}}, {1, Failing("other input"), {1}}});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());

  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(4)).ok());
  ASSERT_TRUE(rig.AwaitInput(0).ok());
  EXPECT_TRUE(loop.ProcessInput(context, 0).ok());
  EXPECT_EQ(context.invocations, 2u);  // the route of input 1 did not run
  EXPECT_TRUE(context.errors.empty());
  EXPECT_EQ(ReceiveInt(rig, 0), 8);
  EXPECT_EQ(ReceiveInt(rig, 1), -4);
}

TEST(RoutingLoopTest, OnlyTheLastRouteMayMoveThePayload) {
  auto consume = [] {
    return MakeLogicHandler([](Counted&& in) { return std::pair{std::move(in), Status::Ok()}; });
  };
  const auto value_of = [](SocketRig& rig, std::size_t output) {
    auto [message, status] = rig.harness.Receive(output);
    EXPECT_TRUE(status.ok()) << status;
    return status.ok() ? message->As<Counted>()->value : std::string();
  };
  {
    SocketRig rig;
    ASSERT_TRUE(rig.Open().ok());
    FakeLoopContext context(rig.sockets);
    RoutingProbe loop({{0, consume(), {0}}});
    ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
    Counted::copies = 0;
    ASSERT_TRUE(rig.harness.Send(0, MakeMessage(Counted("solo"))).ok());
    ASSERT_TRUE(rig.AwaitInput(0).ok());
    EXPECT_TRUE(loop.ProcessInput(context, 0).ok());
    EXPECT_EQ(Counted::copies, 0);  // the only route moves the payload out
    EXPECT_EQ(value_of(rig, 0), "solo");
  }
  {
    SocketRig rig(HarnessConfig{.outputs = 2});
    ASSERT_TRUE(rig.Open().ok());
    FakeLoopContext context(rig.sockets);
    RoutingProbe loop({{0, consume(), {0}}, {0, consume(), {1}}});
    ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
    Counted::copies = 0;
    ASSERT_TRUE(rig.harness.Send(0, MakeMessage(Counted("shared"))).ok());
    ASSERT_TRUE(rig.AwaitInput(0).ok());
    EXPECT_TRUE(loop.ProcessInput(context, 0).ok());
    EXPECT_EQ(Counted::copies, 1);  // the first route copies, the last one moves
    EXPECT_EQ(value_of(rig, 0), "shared");
    EXPECT_EQ(value_of(rig, 1), "shared");
  }
}

TEST(RoutingLoopTest, ReturnsTheFirstFailureAfterRunningEveryRoute) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  RoutingProbe loop({{0, Failing("first"), {0}}, {0, Doubler(), {0}}, {0, Failing("second"), {0}}});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(5)).ok());
  ASSERT_TRUE(rig.AwaitInput(0).ok());

  EXPECT_EQ(loop.ProcessInput(context, 0), Status::Error("first"));
  EXPECT_EQ(context.invocations, 3u);
  EXPECT_EQ(context.errors, (std::vector<Status>{Status::Error("first"), Status::Error("second")}));
  EXPECT_EQ(ReceiveInt(rig), 10);
}

TEST(RoutingLoopTest, EndOfStreamSkipsTheRemainingRoutes) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  auto finish =
      MakeLogicHandler([](const int& value) { return std::pair{value, Status::EndOfStream()}; });
  RoutingProbe loop({{0, finish, {0}}, {0, Doubler(), {0}}});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(5)).ok());
  ASSERT_TRUE(rig.AwaitInput(0).ok());

  EXPECT_EQ(loop.ProcessInput(context, 0).code(), StatusCode::kEndOfStream);
  EXPECT_EQ(context.invocations, 1u);
  EXPECT_EQ(rig.harness.Receive(0, kQuiet).second.code(), StatusCode::kTimeout);
}

TEST(RoutingLoopTest, ReceiveFailuresAreReportedAndReturned) {
  SocketManager closed(nullptr, Layout(1, 1));
  FakeLoopContext context(closed);
  RoutingProbe loop({{0, Doubler(), {0}}});
  ASSERT_TRUE(loop.Prepare(closed).ok());
  EXPECT_EQ(loop.ProcessInput(context, 0).code(), StatusCode::kClosed);
  ASSERT_EQ(context.errors.size(), 1u);
  EXPECT_EQ(context.errors[0].code(), StatusCode::kClosed);
  EXPECT_EQ(context.invocations, 0u);
}

// --- Draining -----------------------------------------------------------------------------------

TEST(DrainTest, FreeRunningLoopProcessesTheQueuedInputThenEnds) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  FreeRunningLoop loop({{0, Doubler(), {0}}});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
  for (int i : {1, 2, 3}) ASSERT_TRUE(rig.harness.Send(0, MakeMessage(i)).ok());

  context.finish = true;
  context.current_state = NodeState::kPaused;  // draining ignores a pause
  EXPECT_TRUE(loop.Drain(context).ok());
  EXPECT_EQ(context.invocations, 3u);
  for (int i : {1, 2, 3}) EXPECT_EQ(ReceiveInt(rig), 2 * i);
}

TEST(DrainTest, AStopCommandCutsTheDrainShort) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  FreeRunningLoop loop({{0, Doubler(), {0}}});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(1)).ok());
  ASSERT_TRUE(rig.harness.Command(ControlType::kStop).ok());

  context.finish = true;
  EXPECT_TRUE(loop.Drain(context).ok());
  EXPECT_EQ(context.invocations, 0u);  // control is applied before the input
  EXPECT_FALSE(context.finishing());
}

TEST(DrainTest, AStopDuringTheDrainEndsItAfterTheCurrentMessage) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  auto stop_after_one = MakeLogicHandler([&context](const int& value) {
    context.stop = true;  // as if the stop token fired meanwhile
    return std::pair{value, Status::Ok()};
  });
  FreeRunningLoop loop({{0, stop_after_one, {0}}});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
  for (int i : {1, 2, 3}) ASSERT_TRUE(rig.harness.Send(0, MakeMessage(i)).ok());

  context.finish = true;
  EXPECT_TRUE(loop.Drain(context).ok());
  EXPECT_EQ(context.invocations, 1u);
}

TEST(DrainTest, EndOfStreamFromLogicEndsTheDrain) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  auto until_zero = MakeLogicHandler([](const int& value) {
    return std::pair{value, value == 0 ? Status::EndOfStream() : Status::Ok()};
  });
  FreeRunningLoop loop({{0, until_zero, {0}}});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
  for (int i : {5, 0, 7}) ASSERT_TRUE(rig.harness.Send(0, MakeMessage(i)).ok());

  context.finish = true;
  EXPECT_EQ(loop.Drain(context).code(), StatusCode::kEndOfStream);
  EXPECT_EQ(context.invocations, 2u);
}

TEST(DrainTest, DataInputsAreDrainedForAtMostOneMinuteByDefault) {
  const FreeRunningLoopOptions defaults;
  EXPECT_EQ(defaults.drain_timeout, std::chrono::seconds(60));
  EXPECT_EQ(defaults.drain_quiet, Timeout(20));
  EXPECT_EQ(FreeRunningLoop({{0, Doubler(), {0}}}).options().drain_timeout,
            std::chrono::seconds(60));
}

TEST(DrainTest, TimeoutEndsADrainWhoseInputNeverGoesQuiet) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  // Every processed message brings the next one, like a producer that never stops.
  auto echo = MakeLogicHandler([&rig](const int& value) {
    (void)rig.harness.Send(0, MakeMessage(value + 1));
    return std::pair{value, Status::Ok()};
  });
  // Publishes nowhere: blocked outputs are bounded by send_timeout, not by drain_timeout.
  FreeRunningLoop loop({{0, echo, {}}},
                       FreeRunningLoopOptions{.drain_quiet = 20ms, .drain_timeout = 100ms});
  ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(0)).ok());

  context.finish = true;
  const auto start = std::chrono::steady_clock::now();
  const Status status = loop.Drain(context);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(status.code(), StatusCode::kTimeout);
  EXPECT_TRUE(Contains(status, "drain gave up after 100 ms")) << status;
  EXPECT_GE(elapsed, 100ms);
  EXPECT_LT(elapsed, kTestTimeout);
  EXPECT_GT(context.invocations, 1u);
}

TEST(DrainTest, QuietPeriodDecidesWhenTheInputsAreDrained) {
  {
    // Zero quiet: what is queued is processed, then the drain ends without waiting.
    SocketRig rig;
    ASSERT_TRUE(rig.Open().ok());
    FakeLoopContext context(rig.sockets);
    FreeRunningLoop loop({{0, Doubler(), {0}}}, FreeRunningLoopOptions{.drain_quiet = kNoWait});
    ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
    for (int i : {1, 2}) ASSERT_TRUE(rig.harness.Send(0, MakeMessage(i)).ok());
    context.finish = true;
    EXPECT_TRUE(loop.Drain(context).ok());
    EXPECT_EQ(context.invocations, 2u);
  }
  {
    // A message that arrives within the quiet period is still processed.
    SocketRig rig;
    ASSERT_TRUE(rig.Open().ok());
    FakeLoopContext context(rig.sockets);
    FreeRunningLoop loop({{0, Doubler(), {0}}}, FreeRunningLoopOptions{.drain_quiet = 500ms});
    ASSERT_TRUE(loop.Prepare(rig.sockets).ok());
    context.finish = true;
    std::jthread late([&rig] {
      std::this_thread::sleep_for(50ms);
      (void)rig.harness.Send(0, MakeMessage(4));
    });
    EXPECT_TRUE(loop.Drain(context).ok());
    late.join();
    EXPECT_EQ(context.invocations, 1u);
    EXPECT_EQ(ReceiveInt(rig), 8);
  }
}

TEST(DrainTest, OtherLoopsHaveNothingToDrain) {
  SocketRig rig;
  ASSERT_TRUE(rig.Open().ok());
  FakeLoopContext context(rig.sockets);
  context.finish = true;
  ASSERT_TRUE(rig.harness.Send(0, MakeMessage(1)).ok());

  TriggeredLoop triggered({{0, Doubler(), {0}}});
  ASSERT_TRUE(triggered.Prepare(rig.sockets).ok());
  EXPECT_TRUE(triggered.Drain(context).ok());  // triggered loops process input only on Trigger
  EXPECT_TRUE(FreeRunningSourceLoop(Constant(), {0}).Drain(context).ok());
  EXPECT_TRUE(TriggeredSourceLoop(Constant(), {0}).Drain(context).ok());
  EXPECT_EQ(context.invocations, 0u);
  EXPECT_TRUE(rig.sockets.Receive(0, kTestTimeout).second.ok());  // still queued
}

// --- Loop properties and validation -------------------------------------------------------------

TEST(LoopKindsTest, NamesAndTriggering) {
  FreeRunningLoop free_running({{0, Doubler(), {0}}});
  TriggeredLoop triggered({{0, Doubler(), {0}}});
  FreeRunningSourceLoop free_running_source(Constant(), {0});
  TriggeredSourceLoop triggered_source(Constant(), {0});
  EXPECT_EQ(free_running.name(), "FreeRunningLoop");
  EXPECT_EQ(triggered.name(), "TriggeredLoop");
  EXPECT_EQ(free_running_source.name(), "FreeRunningSourceLoop");
  EXPECT_EQ(triggered_source.name(), "TriggeredSourceLoop");
  EXPECT_FALSE(free_running.triggered());
  EXPECT_TRUE(triggered.triggered());
  EXPECT_FALSE(free_running_source.triggered());
  EXPECT_TRUE(triggered_source.triggered());
}

TEST(LoopKindsTest, TriggeredLoopNeedsControlAndValidRoutes) {
  SocketManager without_control(nullptr, Layout(1, 1, false));
  SocketManager with_control(nullptr, Layout(1, 1));
  EXPECT_EQ(TriggeredLoop({{0, Doubler(), {0}}}).Prepare(without_control).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(TriggeredLoop({{0, Doubler(), {3}}}).Prepare(with_control).code(),
            StatusCode::kInvalidArgument);
  EXPECT_TRUE(TriggeredLoop({{0, Doubler(), {0}}}).Prepare(with_control).ok());
  // Free-running loops work without control socket.
  EXPECT_TRUE(FreeRunningLoop({{0, Doubler(), {0}}}).Prepare(without_control).ok());
}

TEST(LoopKindsTest, SourceLoopsValidateTheirSetup) {
  SocketManager source(nullptr, Layout(0, 1));
  SocketManager source_without_control(nullptr, Layout(0, 1, false));
  SocketManager with_input(nullptr, Layout(1, 1));

  EXPECT_TRUE(FreeRunningSourceLoop(Constant(), {0}).Prepare(source_without_control).ok());
  EXPECT_EQ(FreeRunningSourceLoop(nullptr, {0}).Prepare(source).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(FreeRunningSourceLoop(Constant(), {0}).Prepare(with_input).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(FreeRunningSourceLoop(Constant(), {1}).Prepare(source).code(),
            StatusCode::kInvalidArgument);

  EXPECT_TRUE(TriggeredSourceLoop(Constant(), {0}).Prepare(source).ok());
  EXPECT_EQ(TriggeredSourceLoop(nullptr, {0}).Prepare(source).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(TriggeredSourceLoop(Constant(), {0}).Prepare(source_without_control).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(TriggeredSourceLoop(Constant(), {0}).Prepare(with_input).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(TriggeredSourceLoop(Constant(), {1}).Prepare(source).code(),
            StatusCode::kInvalidArgument);
}

// --- Loop exits ---------------------------------------------------------------------------------

/// One of every loop kind, prepared against `routing` or `source` sockets.
std::vector<std::unique_ptr<NodeLoop>> EveryLoop(const SocketManager& routing,
                                                 const SocketManager& source) {
  std::vector<std::unique_ptr<NodeLoop>> loops;
  loops.push_back(std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, Doubler(), {0}}}));
  loops.push_back(std::make_unique<TriggeredLoop>(std::vector<Route>{{0, Doubler(), {0}}}));
  for (const auto& loop : loops) EXPECT_TRUE(loop->Prepare(routing).ok()) << loop->name();
  loops.push_back(std::make_unique<FreeRunningSourceLoop>(Constant(), std::vector<std::size_t>{0}));
  loops.push_back(std::make_unique<TriggeredSourceLoop>(Constant(), std::vector<std::size_t>{0}));
  for (std::size_t i = 2; i < loops.size(); ++i) {
    EXPECT_TRUE(loops[i]->Prepare(source).ok()) << loops[i]->name();
  }
  return loops;
}

TEST(LoopExitTest, EveryLoopReturnsOkWhenAStopIsAlreadyRequested) {
  SocketManager routing(nullptr, Layout(1, 1));
  SocketManager source(nullptr, Layout(0, 1));
  for (const auto& loop : EveryLoop(routing, source)) {
    FakeLoopContext context(loop->name().find("Source") == std::string_view::npos ? routing
                                                                                  : source);
    context.stop = true;
    EXPECT_TRUE(loop->Run(context).ok()) << loop->name();  // the sockets are never touched
    EXPECT_EQ(context.invocations, 0u) << loop->name();
  }
}

TEST(LoopExitTest, EveryLoopEndsItsRunOnFinish) {
  SocketManager routing(nullptr, Layout(1, 1));
  SocketManager source(nullptr, Layout(0, 1));
  for (const auto& loop : EveryLoop(routing, source)) {
    FakeLoopContext context(loop->name().find("Source") == std::string_view::npos ? routing
                                                                                  : source);
    context.finish = true;
    EXPECT_TRUE(loop->Run(context).ok()) << loop->name();  // Drain() comes after Run()
  }
}

TEST(LoopExitTest, EveryLoopEndsWithTheTransportFailure) {
  // Unopened managers fail every socket operation with Closed.
  SocketManager routing(nullptr, Layout(1, 1));
  SocketManager source(nullptr, Layout(0, 1));
  for (const NodeState state : {NodeState::kRunning, NodeState::kPaused}) {
    for (const auto& loop : EveryLoop(routing, source)) {
      FakeLoopContext context(loop->name().find("Source") == std::string_view::npos ? routing
                                                                                    : source);
      context.current_state = state;
      EXPECT_EQ(loop->Run(context).code(), StatusCode::kClosed) << loop->name() << " " << state;
    }
  }
}

}  // namespace
}  // namespace mloom
