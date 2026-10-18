#include "mloom/orchestration/stage.hpp"

#include <gtest/gtest.h>

#include "mloom/node/free_running_loop.hpp"
#include "mloom/node/free_running_source_loop.hpp"
#include "mloom/transport/zmq/zmq_message_queue.hpp"

namespace mloom {
namespace {

std::function<std::unique_ptr<NodeLoop>()> SourceLoop() {
  return [] {
    auto source = MakeSourceHandler([] { return std::pair{1, Status::Ok()}; });
    return std::make_unique<FreeRunningSourceLoop>(source, std::vector<std::size_t>{0});
  };
}

std::function<std::unique_ptr<NodeLoop>()> ForwardLoop(std::size_t inputs = 1) {
  return [inputs] {
    auto forward = MakeLogicHandler(
        [](const MessagePtr& message) { return std::pair{message, Status::Ok()}; });
    std::vector<Route> routes;
    for (std::size_t i = 0; i < inputs; ++i) routes.push_back({i, forward, {0}});
    return std::make_unique<FreeRunningLoop>(std::move(routes));
  };
}

TEST(StageTest, RejectsBadNodes) {
  Stage stage("s");
  EXPECT_TRUE(stage.Add({.name = "a", .outputs = {"x"}, .loop = SourceLoop()}).ok());
  EXPECT_EQ(stage.Add({.name = "a", .outputs = {"y"}, .loop = SourceLoop()}).code(),
            StatusCode::kInvalidArgument);  // duplicate
  EXPECT_EQ(stage.Add({.name = "b,c", .loop = SourceLoop()}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(stage.Add({.name = "", .loop = SourceLoop()}).code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(stage.Add({.name = "d", .inputs = {"bad channel"}, .loop = ForwardLoop()}).code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(stage.Add({.name = "e"}).code(), StatusCode::kInvalidArgument);  // no loop
}

TEST(StageTest, ValidatesTheGraph) {
  {
    Stage stage("s");
    EXPECT_EQ(stage.Validate().code(), StatusCode::kInvalidArgument);  // empty
  }
  {
    Stage stage("s");
    ASSERT_TRUE(stage.Add({.name = "sink", .inputs = {"nowhere"}, .loop = ForwardLoop()}).ok());
    EXPECT_EQ(stage.Validate().code(), StatusCode::kInvalidArgument);
  }
  {
    Stage stage("s");
    ASSERT_TRUE(stage.Add({.name = "src", .outputs = {"x", "x"}, .loop = SourceLoop()}).ok());
    EXPECT_EQ(stage.Validate().code(), StatusCode::kInvalidArgument);
  }
}

TEST(StageTest, OrdersProducersBeforeConsumers) {
  Stage stage("s");
  ASSERT_TRUE(
      stage.Add({.name = "sink", .inputs = {"b"}, .outputs = {}, .loop = ForwardLoop()}).ok());
  ASSERT_TRUE(
      stage.Add({.name = "mid", .inputs = {"a"}, .outputs = {"b"}, .loop = ForwardLoop()}).ok());
  ASSERT_TRUE(stage.Add({.name = "src", .outputs = {"a"}, .loop = SourceLoop()}).ok());
  EXPECT_EQ(stage.TopologicalOrder(), (std::vector<std::string>{"src", "mid", "sink"}));
}

TEST(StageTest, CyclesKeepDeclarationOrder) {
  Stage stage("s");
  ASSERT_TRUE(
      stage.Add({.name = "a", .inputs = {"y"}, .outputs = {"x"}, .loop = ForwardLoop()}).ok());
  ASSERT_TRUE(
      stage.Add({.name = "b", .inputs = {"x"}, .outputs = {"y"}, .loop = ForwardLoop()}).ok());
  EXPECT_EQ(stage.TopologicalOrder(), (std::vector<std::string>{"a", "b"}));
}

TEST(StageTest, BuildGeneratesWiringAndBarriers) {
  Stage stage("plant", StageOptions{.start_paused = true});
  ASSERT_TRUE(stage.Add({.name = "left", .outputs = {"numbers"}, .loop = SourceLoop()}).ok());
  ASSERT_TRUE(stage.Add({.name = "right", .outputs = {"numbers"}, .loop = SourceLoop()}).ok());
  ASSERT_TRUE(
      stage.Add({.name = "sum", .inputs = {"numbers"}, .outputs = {"total"}, .loop = ForwardLoop()})
          .ok());
  ASSERT_TRUE(
      stage
          .Add({.name = "log",
                .inputs = {"numbers"},
                .loop =
                    [] {
                      auto sink = MakeLogicHandler([](const MessagePtr&) { return Status::Ok(); });
                      return std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, sink, {}}});
                    }})
          .ok());

  auto [nodes, status] = stage.Build(std::make_shared<zeromq::ZmqMessageQueue>());
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_EQ(nodes.size(), 4u);

  const NodeCore& left = nodes[0]->core();
  EXPECT_EQ(left.sockets().layout().outputs,
            std::vector<std::string>{"inproc://plant/left/out/numbers"});
  EXPECT_EQ(left.options().min_output_subscribers, std::vector<std::size_t>{2});  // sum and log
  EXPECT_EQ(left.options().min_status_subscribers, 1u);
  EXPECT_TRUE(left.options().start_paused);
  EXPECT_EQ(left.sockets().layout().control, "inproc://plant/control");
  EXPECT_EQ(left.sockets().layout().status, "inproc://plant/left/status");

  const NodeCore& sum = nodes[2]->core();
  EXPECT_EQ(
      sum.sockets().layout().inputs,
      std::vector<std::string>{"inproc://plant/left/out/numbers,inproc://plant/right/out/numbers"});
  EXPECT_EQ(sum.options().min_output_subscribers, std::vector<std::size_t>{0});  // no consumer
}

TEST(StageTest, UnorchestratedStagesHaveNoControlOrStatus) {
  Stage stage("free", StageOptions{.orchestrated = false});
  ASSERT_TRUE(stage.Add({.name = "src", .outputs = {"x"}, .loop = SourceLoop()}).ok());
  auto [nodes, status] = stage.Build(std::make_shared<zeromq::ZmqMessageQueue>());
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_FALSE(nodes[0]->core().sockets().has_control());
  EXPECT_FALSE(nodes[0]->core().sockets().has_status());
  EXPECT_EQ(nodes[0]->core().options().min_status_subscribers, 0u);
}

}  // namespace
}  // namespace mloom
