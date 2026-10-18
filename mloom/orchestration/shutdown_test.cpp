#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "mloom/node/free_running_loop.hpp"
#include "mloom/node/free_running_source_loop.hpp"
#include "mloom/orchestration/orchestrator.hpp"
#include "mloom/orchestration/stage.hpp"
#include "mloom/orchestration/thread_runner.hpp"
#include "mloom/transport/zmq/zmq_message_queue.hpp"

// Teardown across the layers: node threads, the orchestrator and the ZeroMQ context underneath
// must shut down promptly, in any order and under load, without leaks or data races. Meant to be
// run with --config=tsan and --config=asan as well.

namespace mloom {
namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

constexpr Timeout kTimeout = 5s;
/// Poll interval 20 ms and linger 100 ms; generous for sanitizer builds.
constexpr auto kPromptShutdown = 2s;

std::shared_ptr<SourceHandlerBase> Endless() {
  return MakeSourceHandler([next = 0]() mutable { return std::pair{next++, Status::Ok()}; });
}

/// Waits until `count` reaches `at_least`.
bool AwaitCount(const std::atomic<int>& count, int at_least) {
  const auto deadline = Clock::now() + kTimeout;
  while (count.load() < at_least && Clock::now() < deadline) std::this_thread::sleep_for(1ms);
  return count.load() >= at_least;
}

/// source (as fast as it can) -> sink that needs `sink_delay` per message and counts them.
Stage BusyPipeline(const std::string& name, StageOptions options, Timeout sink_delay,
                   std::atomic<int>& received) {
  Stage stage(name, options);
  EXPECT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"numbers"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningSourceLoop>(
                                  Endless(), std::vector<std::size_t>{0});
                            }})
                  .ok());
  EXPECT_TRUE(stage
                  .Add({.name = "sink",
                        .inputs = {"numbers"},
                        .loop =
                            [sink_delay, &received] {
                              auto count = MakeLogicHandler([sink_delay, &received](const int&) {
                                if (sink_delay > Timeout::zero()) {
                                  std::this_thread::sleep_for(sink_delay);
                                }
                                ++received;
                                return Status::Ok();
                              });
                              return std::make_unique<FreeRunningLoop>(
                                  std::vector<Route>{{0, count, {}}});
                            }})
                  .ok());
  return stage;
}

void ExpectAllOk(const std::vector<NodeResult>& results) {
  for (const NodeResult& result : results) {
    EXPECT_TRUE(result.status.ok()) << result.node << ": " << result.status;
  }
}

TEST(ShutdownTest, DestroyingTheRunnerStopsABusyPipelinePromptly) {
  std::atomic<int> received{0};
  const Stage stage =
      BusyPipeline("busy", StageOptions{.orchestrated = false}, Timeout(1), received);
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  auto [nodes, built] = stage.Build(queue);
  ASSERT_TRUE(built.ok()) << built;
  auto runner = std::make_unique<ThreadRunner>();
  ASSERT_TRUE(runner->LaunchAll(std::move(nodes)).ok());
  ASSERT_TRUE(AwaitCount(received, 50));  // the sink lags behind: its input queue fills up

  // Nothing is stopped explicitly: destroying the runner and the queue tears everything down,
  // including the messages that are still queued.
  const auto start = Clock::now();
  runner.reset();
  queue.reset();
  EXPECT_LT(Clock::now() - start, kPromptShutdown);
}

TEST(ShutdownTest, NodesStopPromptlyWhileWaitingAtTheStartupBarrier) {
  // Orchestrated nodes wait for the orchestrator's status subscription, which never comes.
  std::atomic<int> received{0};
  const Stage stage = BusyPipeline("waiting", StageOptions{}, Timeout::zero(), received);
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  auto [nodes, built] = stage.Build(queue);
  ASSERT_TRUE(built.ok()) << built;
  ThreadRunner runner;
  ASSERT_TRUE(runner.LaunchAll(std::move(nodes)).ok());
  std::this_thread::sleep_for(50ms);  // inside their barriers by now

  const auto start = Clock::now();
  runner.RequestStop();
  ExpectAllOk(runner.Wait());
  EXPECT_LT(Clock::now() - start, kPromptShutdown);  // far below the 5 s startup timeout
  EXPECT_EQ(received.load(), 0);
}

TEST(ShutdownTest, OrchestratorAndQueueMayGoAwayBeforeTheNodes) {
  std::atomic<int> received{0};
  const Stage stage = BusyPipeline("orphaned", StageOptions{}, Timeout::zero(), received);
  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  auto [nodes, built] = stage.Build(queue);
  ASSERT_TRUE(built.ok()) << built;
  auto [orchestrator, created] = Orchestrator::Create(queue, stage);
  ASSERT_TRUE(created.ok()) << created;
  ThreadRunner runner;
  ASSERT_TRUE(runner.LaunchAll(std::move(nodes)).ok());
  ASSERT_TRUE(orchestrator->AwaitReady(kTimeout).ok());
  ASSERT_TRUE(AwaitCount(received, 50));

  // The nodes keep the context alive; without the orchestrator they report into the void.
  queue.reset();
  orchestrator.reset();
  const int before = received.load();
  ASSERT_TRUE(AwaitCount(received, before + 50));  // still processing

  const auto start = Clock::now();
  runner.RequestStop();
  ExpectAllOk(runner.Wait());
  EXPECT_LT(Clock::now() - start, kPromptShutdown);
}

/// Thread-safe record of the values that reached the sink.
class Values {
 public:
  void Add(int value) {
    std::lock_guard lock(mutex_);
    values_.push_back(value);
  }
  std::vector<int> Get() {
    std::lock_guard lock(mutex_);
    return values_;
  }

 private:
  std::mutex mutex_;
  std::vector<int> values_;
};

TEST(ShutdownTest, BusyChainSurvivesManyCommandsAndFinishesWithoutLoss) {
  // Five free-running nodes with data flowing while the orchestrator pauses and restarts them
  // over and over; Finish then drains the chain. Every value must arrive once and in order.
  Values values;
  Stage stage("restless");
  ASSERT_TRUE(stage
                  .Add({.name = "source",
                        .outputs = {"c0"},
                        .loop =
                            [] {
                              return std::make_unique<FreeRunningSourceLoop>(
                                  Endless(), std::vector<std::size_t>{0},
                                  FreeRunningSourceOptions{.period = 200us});
                            }})
                  .ok());
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(stage
                    .Add({.name = "relay" + std::to_string(i),
                          .inputs = {"c" + std::to_string(i)},
                          .outputs = {"c" + std::to_string(i + 1)},
                          .loop =
                              [] {
                                auto increment = MakeLogicHandler([](const int& value) {
                                  return std::pair{value + 1, Status::Ok()};
                                });
                                return std::make_unique<FreeRunningLoop>(
                                    std::vector<Route>{{0, increment, {0}}});
                              }})
                    .ok());
  }
  ASSERT_TRUE(stage
                  .Add({.name = "sink",
                        .inputs = {"c3"},
                        .loop =
                            [&values] {
                              auto record = MakeLogicHandler([&values](const int& value) {
                                values.Add(value);
                                return Status::Ok();
                              });
                              return std::make_unique<FreeRunningLoop>(
                                  std::vector<Route>{{0, record, {}}});
                            }})
                  .ok());

  auto queue = std::make_shared<zeromq::ZmqMessageQueue>();
  auto [nodes, built] = stage.Build(queue);
  ASSERT_TRUE(built.ok()) << built;
  auto [orchestrator, created] = Orchestrator::Create(queue, stage);
  ASSERT_TRUE(created.ok()) << created;
  ThreadRunner runner;
  ASSERT_TRUE(runner.LaunchAll(std::move(nodes)).ok());
  ASSERT_TRUE(orchestrator->AwaitReady(kTimeout).ok());

  for (int round = 0; round < 20; ++round) {
    ASSERT_TRUE(orchestrator->Pause(kTimeout).ok()) << "round " << round;
    ASSERT_TRUE(orchestrator->Send(ControlType::kQueryStatus).second.ok());
    ASSERT_TRUE(orchestrator->Start(kTimeout).ok()) << "round " << round;
    std::this_thread::sleep_for(2ms);
  }
  ASSERT_TRUE(orchestrator->Finish(stage.TopologicalOrder(), kTimeout).ok());
  ExpectAllOk(runner.Wait());

  const std::uint64_t produced = orchestrator->Snapshot().at("source").invocations;
  const std::vector<int> received = values.Get();
  ASSERT_EQ(received.size(), produced);
  for (std::size_t i = 0; i < received.size(); ++i) {
    ASSERT_EQ(received[i], static_cast<int>(i) + 3) << "at " << i;
  }
}

// --- Backpressure -------------------------------------------------------------------------------

/// inproc pipes hold both high-water marks: 8 messages between two nodes.
zeromq::ZmqOptions SmallQueues() {
  return zeromq::ZmqOptions{.send_high_water_mark = 4, .receive_high_water_mark = 4};
}

/// source (as fast as it can) -> sink that needs 2 ms per message, on small queues: the source
/// spends most of its time blocked by backpressure.
class BlockedPipeline {
 public:
  explicit BlockedPipeline(const std::string& name, NodeCoreOptions source_options = {})
      : stage_(name) {
    EXPECT_TRUE(stage_
                    .Add({.name = "source",
                          .outputs = {"numbers"},
                          .loop =
                              [this] {
                                auto produce = MakeSourceHandler([this, next = 0]() mutable {
                                  ++produced;
                                  return std::pair{next++, Status::Ok()};
                                });
                                return std::make_unique<FreeRunningSourceLoop>(
                                    produce, std::vector<std::size_t>{0});
                              },
                          .options = std::move(source_options)})
                    .ok());
    EXPECT_TRUE(stage_
                    .Add({.name = "sink",
                          .inputs = {"numbers"},
                          .loop =
                              [this] {
                                auto slow = MakeLogicHandler([this](const int& value) {
                                  std::this_thread::sleep_for(2ms);
                                  received.Add(value);
                                  return Status::Ok();
                                });
                                return std::make_unique<FreeRunningLoop>(
                                    std::vector<Route>{{0, slow, {}}});
                              }})
                    .ok());
  }

  /// Builds and launches the nodes and waits until the orchestrator sees them all.
  Status Deploy() {
    queue = std::make_shared<zeromq::ZmqMessageQueue>(SmallQueues());
    auto [nodes, built] = stage_.Build(queue);
    if (!built.ok()) return built;
    auto [created, created_status] = Orchestrator::Create(queue, stage_);
    if (!created_status.ok()) return created_status;
    orchestrator = std::move(created);
    runner = std::make_unique<ThreadRunner>();
    if (Status status = runner->LaunchAll(std::move(nodes)); !status.ok()) return status;
    return orchestrator->AwaitReady(kTimeout);
  }

  bool AwaitReceived(std::size_t count) {
    const auto deadline = Clock::now() + kTimeout;
    while (received.Get().size() < count && Clock::now() < deadline) {
      std::this_thread::sleep_for(1ms);
    }
    return received.Get().size() >= count;
  }

  const Stage& stage() const { return stage_; }

  std::atomic<int> produced{0};
  Values received;
  std::shared_ptr<zeromq::ZmqMessageQueue> queue;
  std::unique_ptr<Orchestrator> orchestrator;
  std::unique_ptr<ThreadRunner> runner;  // declared last: its nodes use the members above

 private:
  Stage stage_;
};

enum class Shutdown { kStopToken, kStopCommand, kFinish, kDestroyRunner };

std::string ToString(Shutdown way) {
  switch (way) {
    case Shutdown::kStopToken:
      return "stop token";
    case Shutdown::kStopCommand:
      return "Stop command";
    case Shutdown::kFinish:
      return "Finish";
    case Shutdown::kDestroyRunner:
      return "runner destroyed";
  }
  return "?";
}

TEST(ShutdownTest, SourceBlockedByBackpressureShutsDownPromptlyEveryWay) {
  int deployment = 0;
  for (const Shutdown way : {Shutdown::kStopToken, Shutdown::kStopCommand, Shutdown::kFinish,
                             Shutdown::kDestroyRunner}) {
    for (int round = 0; round < 3; ++round) {
      SCOPED_TRACE(ToString(way) + ", round " + std::to_string(round));
      BlockedPipeline pipeline("blocked" + std::to_string(deployment++));
      const Status deployed = pipeline.Deploy();
      ASSERT_TRUE(deployed.ok()) << deployed;
      ASSERT_TRUE(pipeline.AwaitReceived(10));
      // Backpressure, not loss: the slow sink holds the source back to what the queue holds.
      const int produced = pipeline.produced.load();
      EXPECT_LE(produced - static_cast<int>(pipeline.received.Get().size()), 20);

      const auto start = Clock::now();
      std::vector<NodeResult> results;
      switch (way) {
        case Shutdown::kStopToken:
          pipeline.runner->RequestStop();
          results = pipeline.runner->Wait();
          break;
        case Shutdown::kStopCommand:
          ASSERT_TRUE(pipeline.orchestrator->Stop(kTimeout).ok());
          results = pipeline.runner->Wait();
          break;
        case Shutdown::kFinish:
          ASSERT_TRUE(
              pipeline.orchestrator->Finish(pipeline.stage().TopologicalOrder(), kTimeout).ok());
          results = pipeline.runner->Wait();
          break;
        case Shutdown::kDestroyRunner:
          pipeline.runner.reset();
          break;
      }
      EXPECT_LT(Clock::now() - start, kPromptShutdown);
      ExpectAllOk(results);

      // Whatever arrived arrived once and in order; Finish delivered everything.
      const std::vector<int> values = pipeline.received.Get();
      for (std::size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(values[i], static_cast<int>(i));
      }
      if (way == Shutdown::kFinish) {
        EXPECT_EQ(static_cast<int>(values.size()), pipeline.produced.load());
      }
    }
  }
}

TEST(ShutdownTest, PausedConsumerCannotDeadlockFinish) {
  // The sink is paused, so the source can never get rid of its message. Its backpressure
  // handler resolves that: after the send timeout the message is skipped, the source finishes,
  // and the paused sink still drains what it had queued.
  std::atomic<int> skipped{0};
  NodeCoreOptions source_options;
  source_options.send_timeout = 100ms;
  source_options.on_backpressure = [&skipped](const BackpressureEvent&) {
    ++skipped;
    return BackpressureAction::kSkip;
  };
  BlockedPipeline pipeline("paused-consumer", source_options);
  const Status deployed = pipeline.Deploy();
  ASSERT_TRUE(deployed.ok()) << deployed;
  ASSERT_TRUE(pipeline.AwaitReceived(10));
  ASSERT_TRUE(pipeline.orchestrator->Pause(kTimeout, "sink").ok());
  std::this_thread::sleep_for(50ms);  // the source is blocked by now

  const auto start = Clock::now();
  ASSERT_TRUE(pipeline.orchestrator->Finish(pipeline.stage().TopologicalOrder(), kTimeout).ok());
  EXPECT_LT(Clock::now() - start, kPromptShutdown);
  ExpectAllOk(pipeline.runner->Wait());

  EXPECT_GE(skipped.load(), 1);
  const std::vector<int> values = pipeline.received.Get();
  EXPECT_EQ(static_cast<int>(values.size()), pipeline.produced.load() - skipped.load());
  for (std::size_t i = 1; i < values.size(); ++i) ASSERT_LT(values[i - 1], values[i]);
}

}  // namespace
}  // namespace mloom
