#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "mloom/node/free_running_loop.hpp"
#include "mloom/node/free_running_source_loop.hpp"
#include "mloom/node/triggered_loop.hpp"
#include "mloom/node/triggered_source_loop.hpp"
#include "mloom/orchestration/orchestrator.hpp"
#include "mloom/orchestration/stage.hpp"
#include "mloom/orchestration/thread_runner.hpp"
#include "mloom/transport/zmq/zmq_message_queue.hpp"

namespace mloom {
namespace {

using namespace std::chrono_literals;
constexpr Timeout kTimeout = 5s;

/// Thread-safe sink storage.
class Collector {
 public:
  void Add(int value) {
    std::lock_guard lock(mutex_);
    values_.push_back(value);
    changed_.notify_all();
  }
  std::vector<int> WaitFor(std::size_t count) {
    std::unique_lock lock(mutex_);
    changed_.wait_for(lock, kTimeout, [&] { return values_.size() >= count; });
    return values_;
  }
  std::vector<int> values() {
    std::lock_guard lock(mutex_);
    return values_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  std::vector<int> values_;
};

/// Which node fired with which value, recorded from the nodes' threads.
class FiringLog {
 public:
  using Entry = std::pair<std::string, int>;

  void Record(std::string node, int value) {
    std::lock_guard lock(mutex_);
    entries_.emplace_back(std::move(node), value);
  }
  std::vector<Entry> entries() {
    std::lock_guard lock(mutex_);
    return entries_;
  }

 private:
  std::mutex mutex_;
  std::vector<Entry> entries_;
};

/// A triggered node applying `step` to its input, logging every firing.
std::function<std::unique_ptr<NodeLoop>()> LoggedStep(std::string name, FiringLog& log,
                                                      std::function<int(int)> step) {
  return [name, &log, step] {
    auto handler = MakeLogicHandler([name, &log, step](const int& value) {
      const int result = step(value);
      log.Record(name, result);
      return std::pair{result, Status::Ok()};
    });
    return std::make_unique<TriggeredLoop>(std::vector<Route>{{0, handler, {0}}});
  };
}

std::shared_ptr<SourceHandlerBase> Counter(int count) {
  return MakeSourceHandler([next = 0, count]() mutable {
    const int value = next++;
    return std::pair{value, value >= count ? Status::EndOfStream() : Status::Ok()};
  });
}

std::shared_ptr<LogicHandlerBase> Doubler() {
  return MakeLogicHandler([](const int& value) { return std::pair{2 * value, Status::Ok()}; });
}

std::function<std::unique_ptr<NodeLoop>()> SinkLoop(Collector& collector) {
  return [&collector] {
    auto sink = MakeLogicHandler([&collector](const int& value) {
      collector.Add(value);
      return Status::Ok();
    });
    return std::make_unique<FreeRunningLoop>(std::vector<Route>{{0, sink, {}}});
  };
}

/// Builds `stage`, creates its orchestrator and runs every node on its own thread.
struct Deployment {
  explicit Deployment(const Stage& stage) {
    auto [built, build_status] = stage.Build(queue);
    EXPECT_TRUE(build_status.ok()) << build_status;
    auto [created, create_status] = Orchestrator::Create(queue, stage);
    EXPECT_TRUE(create_status.ok()) << create_status;
    orchestrator = std::move(created);
    EXPECT_TRUE(runner.LaunchAll(std::move(built)).ok());
  }

  std::shared_ptr<zeromq::ZmqMessageQueue> queue = std::make_shared<zeromq::ZmqMessageQueue>();
  std::unique_ptr<Orchestrator> orchestrator;
  ThreadRunner runner;
};

TEST(OrchestrationTest, FreeRunningPipelineLosesNothingAtStartup) {
  // source -> doubler -> sink, all free-running: no orchestrator command is needed.
  Collector collector;
  Stage stage("pipeline");
  ASSERT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"numbers"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningSourceLoop>(
                                  Counter(100), std::vector<std::size_t>{0});
                            }})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "doubler",
                        .inputs = {"numbers"},
                        .outputs = {"doubled"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningLoop>(
                                  std::vector<Route>{{0, Doubler(), {0}}});
                            }})
                  .ok());
  ASSERT_TRUE(stage.Add({.name = "sink", .inputs = {"doubled"}, .loop = SinkLoop(collector)}).ok());

  Deployment deployment(stage);
  Orchestrator& orchestrator = *deployment.orchestrator;
  const Status ready = orchestrator.AwaitReady(kTimeout);
  ASSERT_TRUE(ready.ok()) << ready;

  std::vector<int> expected;
  for (int i = 0; i < 100; ++i) expected.push_back(2 * i);
  EXPECT_EQ(collector.WaitFor(100), expected);  // complete and in order

  ASSERT_TRUE(orchestrator.AwaitState("source", NodeState::kStopped, kTimeout).ok());
  ASSERT_TRUE(orchestrator.Stop(kTimeout).ok());
  const auto results = deployment.runner.Wait();
  ASSERT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].node, "source");
  EXPECT_EQ(results[0].status.code(), StatusCode::kEndOfStream);
  EXPECT_TRUE(results[1].status.ok()) << results[1].status;
  EXPECT_TRUE(results[2].status.ok()) << results[2].status;
}

TEST(OrchestrationTest, SourceEndFinishesThePipelineWithoutLosingData) {
  // source -> slow -> increment -> sink, all free-running. The source ends its stream while the
  // slow node still has a backlog. The orchestrator reacts by finishing the other nodes in
  // topological order: each one drains its input before it stops, so every value arrives.
  constexpr int kCount = 200;
  Collector collector;
  Stage stage("draining");
  ASSERT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"numbers"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningSourceLoop>(
                                  Counter(kCount), std::vector<std::size_t>{0});
                            }})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "slow",
                        .inputs = {"numbers"},
                        .outputs = {"doubled"},
                        .loop =
                            [] {
                              auto slow_double = MakeLogicHandler([](const int& value) {
                                std::this_thread::sleep_for(2ms);  // much slower than the source
                                return std::pair{2 * value, Status::Ok()};
                              });
                              return std::make_unique<FreeRunningLoop>(
                                  std::vector<Route>{{0, slow_double, {0}}});
                            }})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "increment",
                        .inputs = {"doubled"},
                        .outputs = {"results"},
                        .loop =
                            [] {
                              auto increment = MakeLogicHandler([](const int& value) {
                                return std::pair{value + 1, Status::Ok()};
                              });
                              return std::make_unique<FreeRunningLoop>(
                                  std::vector<Route>{{0, increment, {0}}});
                            }})
                  .ok());
  ASSERT_TRUE(stage.Add({.name = "sink", .inputs = {"results"}, .loop = SinkLoop(collector)}).ok());

  Deployment deployment(stage);
  Orchestrator& orchestrator = *deployment.orchestrator;
  ASSERT_TRUE(orchestrator.AwaitReady(kTimeout).ok());

  // The source finishes on its own while most of its data is still on the way.
  ASSERT_TRUE(orchestrator.AwaitState("source", NodeState::kStopped, kTimeout).ok());
  EXPECT_LT(collector.values().size(), static_cast<std::size_t>(kCount));

  // The orchestrator reacts: it finishes every node, producers first.
  ASSERT_TRUE(orchestrator.Finish(stage.TopologicalOrder(), kTimeout).ok());
  for (const auto& [node, report] : orchestrator.Snapshot()) {
    EXPECT_EQ(report.state, NodeState::kStopped) << node;
  }
  std::vector<int> expected;
  for (int i = 0; i < kCount; ++i) expected.push_back(2 * i + 1);
  EXPECT_EQ(collector.values(), expected);  // complete and in order

  const auto results = deployment.runner.Wait();
  ASSERT_EQ(results.size(), 4u);
  EXPECT_EQ(results[0].status.code(), StatusCode::kEndOfStream);
  for (std::size_t i = 1; i < results.size(); ++i) {
    EXPECT_TRUE(results[i].status.ok()) << results[i].node << ": " << results[i].status;
  }
}

TEST(OrchestrationTest, FinishGivesUpOnANodeWhoseInputNeverStops) {
  // Finishing the sink while its source still runs breaks the topological order: input keeps
  // arriving, so the sink's drain ends at its timeout instead of hanging, and Finish says so.
  Collector collector;
  Stage stage("endless");
  ASSERT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"numbers"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningSourceLoop>(
                                  Counter(1'000'000), std::vector<std::size_t>{0},
                                  FreeRunningSourceOptions{.period = 1ms});
                            }})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "sink",
                        .inputs = {"numbers"},
                        .loop =
                            [&collector] {
                              auto sink = MakeLogicHandler([&collector](const int& value) {
                                collector.Add(value);
                                return Status::Ok();
                              });
                              return std::make_unique<FreeRunningLoop>(
                                  std::vector<Route>{{0, sink, {}}},
                                  FreeRunningLoopOptions{.drain_timeout = 200ms});
                            }})
                  .ok());

  Deployment deployment(stage);
  Orchestrator& orchestrator = *deployment.orchestrator;
  ASSERT_TRUE(orchestrator.AwaitReady(kTimeout).ok());
  EXPECT_GE(collector.WaitFor(5).size(), 5u);

  const Status finished = orchestrator.Finish({"sink"}, kTimeout);
  EXPECT_EQ(finished.code(), StatusCode::kTimeout) << finished;
  EXPECT_NE(finished.message().find("node 'sink' failed while finishing"), std::string::npos)
      << finished;
  EXPECT_EQ(orchestrator.Snapshot().at("sink").state, NodeState::kFailed);

  ASSERT_TRUE(orchestrator.Stop(kTimeout).ok());  // the source still runs
  const auto results = deployment.runner.Wait();
  ASSERT_EQ(results.size(), 2u);
  EXPECT_TRUE(results[0].status.ok()) << results[0].status;
  EXPECT_EQ(results[1].status.code(), StatusCode::kTimeout);
}

TEST(OrchestrationTest, CoordinatedStartPauseAndStop) {
  Collector collector;
  Stage stage("coordinated", StageOptions{.start_paused = true});
  ASSERT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"numbers"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningSourceLoop>(
                                  Counter(1'000'000), std::vector<std::size_t>{0},
                                  FreeRunningSourceOptions{.period = 1ms});
                            }})
                  .ok());
  ASSERT_TRUE(stage.Add({.name = "sink", .inputs = {"numbers"}, .loop = SinkLoop(collector)}).ok());

  Deployment deployment(stage);
  Orchestrator& orchestrator = *deployment.orchestrator;
  ASSERT_TRUE(orchestrator.AwaitReady(kTimeout).ok());
  EXPECT_EQ(orchestrator.Snapshot().at("source").state, NodeState::kPaused);
  std::this_thread::sleep_for(50ms);
  EXPECT_TRUE(collector.values().empty());

  ASSERT_TRUE(orchestrator.Start(kTimeout).ok());
  EXPECT_GE(collector.WaitFor(5).size(), 5u);

  ASSERT_TRUE(orchestrator.Pause(kTimeout, "source").ok());
  std::this_thread::sleep_for(20ms);  // let in-flight messages drain
  const std::size_t paused_count = collector.values().size();
  std::this_thread::sleep_for(50ms);
  EXPECT_EQ(collector.values().size(), paused_count);

  ASSERT_TRUE(orchestrator.Stop(kTimeout).ok());
  for (const NodeResult& result : deployment.runner.Wait()) {
    EXPECT_TRUE(result.status.ok()) << result.node << ": " << result.status;
  }
  // Values arrived in order, without gaps.
  const std::vector<int> values = collector.values();
  for (std::size_t i = 0; i < values.size(); ++i) EXPECT_EQ(values[i], static_cast<int>(i));
}

TEST(OrchestrationTest, OrchestratorStepsATriggeredPipeline) {
  Collector collector;
  Stage stage("stepped");
  ASSERT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"numbers"},
                        .loop =
                            [] {
                              return std::make_unique<TriggeredSourceLoop>(
                                  Counter(100), std::vector<std::size_t>{0});
                            }})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "doubler",
                        .inputs = {"numbers"},
                        .outputs = {"doubled"},
                        .loop =
                            [] {
                              return std::make_unique<TriggeredLoop>(
                                  std::vector<Route>{{0, Doubler(), {0}}});
                            }})
                  .ok());
  ASSERT_TRUE(stage.Add({.name = "sink", .inputs = {"doubled"}, .loop = SinkLoop(collector)}).ok());
  EXPECT_EQ(stage.TopologicalOrder(), (std::vector<std::string>{"source", "doubler", "sink"}));

  Deployment deployment(stage);
  Orchestrator& orchestrator = *deployment.orchestrator;
  ASSERT_TRUE(orchestrator.AwaitReady(kTimeout).ok());

  for (int step = 0; step < 3; ++step) {
    ASSERT_TRUE(orchestrator.StepAll({"source", "doubler"}, kTimeout).ok());
    EXPECT_EQ(collector.WaitFor(step + 1).size(), static_cast<std::size_t>(step + 1));
  }
  EXPECT_EQ(collector.values(), (std::vector<int>{0, 2, 4}));

  // A free-running node rejects triggers; the rejection is the step's result.
  EXPECT_EQ(orchestrator.Step("sink", kTimeout).code(), StatusCode::kUnsupported);
  const auto errors = orchestrator.TakeErrors();
  ASSERT_EQ(errors.size(), 1u);
  EXPECT_EQ(errors[0].node, "sink");

  ASSERT_TRUE(orchestrator.Stop(kTimeout).ok());
}

TEST(OrchestrationTest, ModelRunnerFiresAChainNodeByNode) {
  // sensor -> scale -> offset -> doubler -> sink, every node triggered. The orchestrator acts as
  // the external model runner: it fires one node and waits for that node's confirmation
  // (StepCompleted) before it fires the next one.
  FiringLog log;
  Stage stage("chain");
  ASSERT_TRUE(stage
                  .Add({.name = "sensor",
                        .outputs = {"raw"},
                        .loop =
                            [&log] {
                              auto source = MakeSourceHandler([&log, next = 1]() mutable {
                                const int value = next++;
                                log.Record("sensor", value);
                                return std::pair{value, Status::Ok()};
                              });
                              return std::make_unique<TriggeredSourceLoop>(
                                  source, std::vector<std::size_t>{0});
                            }})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "scale",
                        .inputs = {"raw"},
                        .outputs = {"scaled"},
                        .loop = LoggedStep("scale", log, [](int value) { return value * 10; })})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "offset",
                        .inputs = {"scaled"},
                        .outputs = {"shifted"},
                        .loop = LoggedStep("offset", log, [](int value) { return value + 1; })})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "doubler",
                        .inputs = {"shifted"},
                        .outputs = {"doubled"},
                        .loop = LoggedStep("doubler", log, [](int value) { return value * 2; })})
                  .ok());
  ASSERT_TRUE(stage
                  .Add({.name = "sink",
                        .inputs = {"doubled"},
                        .loop =
                            [&log] {
                              auto sink = MakeLogicHandler([&log](const int& value) {
                                log.Record("sink", value);
                                return Status::Ok();
                              });
                              return std::make_unique<TriggeredLoop>(
                                  std::vector<Route>{{0, sink, {}}});
                            }})
                  .ok());
  const std::vector<std::string> order = stage.TopologicalOrder();
  ASSERT_EQ(order, (std::vector<std::string>{"sensor", "scale", "offset", "doubler", "sink"}));

  Deployment deployment(stage);
  Orchestrator& model_runner = *deployment.orchestrator;
  ASSERT_TRUE(model_runner.AwaitReady(kTimeout).ok());
  EXPECT_TRUE(log.entries().empty());  // running, but nothing fires without a trigger

  for (std::uint64_t cycle = 1; cycle <= 3; ++cycle) {
    for (std::size_t i = 0; i < order.size(); ++i) {
      const std::string& node = order[i];
      ASSERT_TRUE(model_runner.Step(node, kTimeout).ok()) << node << " in cycle " << cycle;
      const NodeStatus& confirmation = model_runner.Snapshot().at(node);
      EXPECT_EQ(confirmation.event, StatusEvent::kStepCompleted);
      EXPECT_EQ(confirmation.invocations, cycle);
      // Exactly the confirmed node fired since the previous confirmation; nothing ran ahead.
      const std::vector<FiringLog::Entry> entries = log.entries();
      ASSERT_EQ(entries.size(), (cycle - 1) * order.size() + i + 1);
      EXPECT_EQ(entries.back().first, node);
    }
  }

  std::vector<FiringLog::Entry> expected;
  for (int value = 1; value <= 3; ++value) {
    const int doubled = (value * 10 + 1) * 2;
    expected.insert(expected.end(), {{"sensor", value},
                                     {"scale", value * 10},
                                     {"offset", value * 10 + 1},
                                     {"doubler", doubled},
                                     {"sink", doubled}});
  }
  EXPECT_EQ(log.entries(), expected);

  ASSERT_TRUE(model_runner.Stop(kTimeout).ok());
  for (const NodeResult& result : deployment.runner.Wait()) {
    EXPECT_TRUE(result.status.ok()) << result.node << ": " << result.status;
  }
}

TEST(OrchestrationTest, ChannelsFanIn) {
  Collector collector;
  Stage stage("fanin");
  for (const char* name : {"left", "right"}) {
    ASSERT_TRUE(stage
                    .Add({.name = name,
                          .outputs = {"numbers"},
                          .loop =
                              [] {
                                return std::make_unique<TriggeredSourceLoop>(
                                    Counter(100), std::vector<std::size_t>{0});
                              }})
                    .ok());
  }
  ASSERT_TRUE(stage.Add({.name = "sink", .inputs = {"numbers"}, .loop = SinkLoop(collector)}).ok());

  Deployment deployment(stage);
  Orchestrator& orchestrator = *deployment.orchestrator;
  ASSERT_TRUE(orchestrator.AwaitReady(kTimeout).ok());
  ASSERT_TRUE(orchestrator.StepAll({"left", "right", "left"}, kTimeout).ok());
  std::vector<int> values = collector.WaitFor(3);
  std::sort(values.begin(), values.end());
  EXPECT_EQ(values, (std::vector<int>{0, 0, 1}));
}

TEST(OrchestrationTest, UnorchestratedStageRunsEventDriven) {
  Collector collector;
  Stage stage("events", StageOptions{.orchestrated = false});
  ASSERT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"numbers"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningSourceLoop>(
                                  Counter(10), std::vector<std::size_t>{0});
                            }})
                  .ok());
  ASSERT_TRUE(stage.Add({.name = "sink", .inputs = {"numbers"}, .loop = SinkLoop(collector)}).ok());
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  auto [nodes, status] = stage.Build(queue);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(Orchestrator::Create(queue, stage).second.code(), StatusCode::kInvalidArgument);

  ThreadRunner runner;
  ASSERT_TRUE(runner.LaunchAll(std::move(nodes)).ok());
  EXPECT_EQ(collector.WaitFor(10).size(), 10u);
  runner.RequestStop();
  const auto results = runner.Wait();
  EXPECT_EQ(results[0].status.code(), StatusCode::kEndOfStream);
  EXPECT_TRUE(results[1].status.ok());
}

TEST(OrchestrationTest, StartupFailureIsVisibleToTheOrchestrator) {
  Stage stage("broken");
  // The loop publishes on an output the node does not have, so the node cannot start.
  ASSERT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"numbers"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningSourceLoop>(
                                  Counter(1), std::vector<std::size_t>{5});
                            }})
                  .ok());
  Deployment deployment(stage);
  const Status ready = deployment.orchestrator->AwaitReady(kTimeout);
  EXPECT_FALSE(ready.ok());
  EXPECT_EQ(deployment.orchestrator->Snapshot().at("source").state, NodeState::kFailed);
  EXPECT_EQ(deployment.runner.Wait()[0].status.code(), StatusCode::kInvalidArgument);
}

}  // namespace
}  // namespace mloom
