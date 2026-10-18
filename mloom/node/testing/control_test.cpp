#include "mloom/node/control.hpp"

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <utility>

#include "mloom/core/codec.hpp"
#include "mloom/core/message.hpp"

namespace mloom {
namespace {

template <typename T>
std::string Streamed(const T& value) {
  std::ostringstream os;
  os << value;
  return os.str();
}

std::pair<MessagePtr, Status> RoundTrip(const CodecRegistry& codecs, const MessagePtr& message) {
  Bytes frame;
  if (Status status = codecs.Encode(*message, frame); !status.ok()) return {nullptr, status};
  return codecs.Decode(frame);
}

// --- Names --------------------------------------------------------------------------------------

TEST(ControlTest, NodeStatesHaveNames) {
  EXPECT_EQ(ToString(NodeState::kCreated), "Created");
  EXPECT_EQ(ToString(NodeState::kRunning), "Running");
  EXPECT_EQ(ToString(NodeState::kPaused), "Paused");
  EXPECT_EQ(ToString(NodeState::kStopped), "Stopped");
  EXPECT_EQ(ToString(NodeState::kFailed), "Failed");
  EXPECT_EQ(ToString(static_cast<NodeState>(200)), "Unknown");
  EXPECT_EQ(Streamed(NodeState::kPaused), "Paused");
}

TEST(ControlTest, ControlTypesHaveNames) {
  EXPECT_EQ(ToString(ControlType::kStart), "Start");
  EXPECT_EQ(ToString(ControlType::kPause), "Pause");
  EXPECT_EQ(ToString(ControlType::kStop), "Stop");
  EXPECT_EQ(ToString(ControlType::kTrigger), "Trigger");
  EXPECT_EQ(ToString(ControlType::kQueryStatus), "QueryStatus");
  EXPECT_EQ(ToString(ControlType::kFinish), "Finish");
  EXPECT_EQ(ToString(static_cast<ControlType>(200)), "Unknown");
  EXPECT_EQ(Streamed(ControlType::kTrigger), "Trigger");
}

TEST(ControlTest, StatusEventsHaveNames) {
  EXPECT_EQ(ToString(StatusEvent::kStateChanged), "StateChanged");
  EXPECT_EQ(ToString(StatusEvent::kStepCompleted), "StepCompleted");
  EXPECT_EQ(ToString(StatusEvent::kError), "Error");
  EXPECT_EQ(ToString(StatusEvent::kStatusReply), "StatusReply");
  EXPECT_EQ(ToString(StatusEvent::kEndOfStream), "EndOfStream");
  EXPECT_EQ(ToString(static_cast<StatusEvent>(200)), "Unknown");
  EXPECT_EQ(Streamed(StatusEvent::kError), "Error");
}

TEST(ControlTest, DefaultsAreAHarmlessQueryAndAFreshNode) {
  const ControlCommand command;
  EXPECT_EQ(command.type, ControlType::kQueryStatus);
  EXPECT_TRUE(command.target.empty());
  EXPECT_EQ(command.sequence, 0u);

  const NodeStatus report;
  EXPECT_EQ(report.state, NodeState::kCreated);
  EXPECT_EQ(report.event, StatusEvent::kStatusReply);
  EXPECT_EQ(report.code, StatusCode::kOk);
  EXPECT_EQ(report.sequence, 0u);
  EXPECT_EQ(report.invocations, 0u);
  EXPECT_EQ(report.errors, 0u);
}

// --- Codecs -------------------------------------------------------------------------------------

TEST(ControlTest, ControlMessagesNeedRegisteredCodecs) {
  CodecRegistry codecs;
  EXPECT_EQ(RoundTrip(codecs, MakeMessage(ControlCommand{})).second.code(),
            StatusCode::kUnsupported);
  ASSERT_TRUE(RegisterControlCodecs(codecs).ok());
  EXPECT_TRUE(codecs.Contains<ControlCommand>());
  EXPECT_TRUE(codecs.Contains<NodeStatus>());
  EXPECT_TRUE(RegisterControlCodecs(codecs).ok());  // idempotent
}

class ControlCodecTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_TRUE(RegisterControlCodecs(codecs).ok()); }

  CodecRegistry codecs;
};

TEST_F(ControlCodecTest, ControlCommandRoundTrips) {
  auto [decoded, status] =
      RoundTrip(codecs, MakeMessage(ControlCommand{ControlType::kTrigger, "camera", 42}));
  ASSERT_TRUE(status.ok()) << status;
  const ControlCommand* command = decoded->As<ControlCommand>();
  ASSERT_NE(command, nullptr);
  EXPECT_EQ(command->type, ControlType::kTrigger);
  EXPECT_EQ(command->target, "camera");
  EXPECT_EQ(command->sequence, 42u);
}

TEST_F(ControlCodecTest, EveryControlTypeRoundTrips) {
  for (const ControlType type :
       {ControlType::kStart, ControlType::kPause, ControlType::kStop, ControlType::kTrigger,
        ControlType::kQueryStatus, ControlType::kFinish}) {
    auto [decoded, status] = RoundTrip(codecs, MakeMessage(ControlCommand{type, "", 1}));
    ASSERT_TRUE(status.ok()) << type << ": " << status;
    EXPECT_EQ(decoded->As<ControlCommand>()->type, type);
  }
}

TEST_F(ControlCodecTest, NodeStatusRoundTrips) {
  NodeStatus sent;
  sent.node = "filter";
  sent.state = NodeState::kPaused;
  sent.event = StatusEvent::kStepCompleted;
  sent.code = StatusCode::kTimeout;
  sent.detail = "no input within 30 ms";
  sent.sequence = 7;
  sent.invocations = 1234;
  sent.errors = 5;
  auto [decoded, status] = RoundTrip(codecs, MakeMessage(sent));
  ASSERT_TRUE(status.ok()) << status;
  const NodeStatus* report = decoded->As<NodeStatus>();
  ASSERT_NE(report, nullptr);
  EXPECT_EQ(report->node, sent.node);
  EXPECT_EQ(report->state, sent.state);
  EXPECT_EQ(report->event, sent.event);
  EXPECT_EQ(report->code, sent.code);
  EXPECT_EQ(report->detail, sent.detail);
  EXPECT_EQ(report->sequence, sent.sequence);
  EXPECT_EQ(report->invocations, sent.invocations);
  EXPECT_EQ(report->errors, sent.errors);
}

TEST_F(ControlCodecTest, RejectsUnknownControlTypes) {
  auto [decoded, status] =
      RoundTrip(codecs, MakeMessage(ControlCommand{static_cast<ControlType>(9), "", 1}));
  EXPECT_EQ(decoded, nullptr);
  EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
}

TEST_F(ControlCodecTest, RejectsMalformedNodeStatus) {
  NodeStatus bad_state;
  bad_state.state = static_cast<NodeState>(77);
  NodeStatus bad_event;
  bad_event.event = static_cast<StatusEvent>(77);
  NodeStatus bad_code;
  bad_code.code = static_cast<StatusCode>(77);
  for (const NodeStatus& report : {bad_state, bad_event, bad_code}) {
    auto [decoded, status] = RoundTrip(codecs, MakeMessage(report));
    EXPECT_EQ(decoded, nullptr);
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
  }
}

TEST_F(ControlCodecTest, RejectsTruncatedFrames) {
  Bytes frame;
  ASSERT_TRUE(
      codecs.Encode(*MakeMessage(ControlCommand{ControlType::kStop, "node", 3}), frame).ok());
  frame.pop_back();
  EXPECT_EQ(codecs.Decode(frame).second.code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mloom
