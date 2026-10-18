#include "mloom/transport/zmq/zmq_message_queue.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

namespace mloom_test {
struct Reading {
  int sensor = 0;
  double value = 0.0;
};

/// Counts destructions, to prove that dropped frames release their message.
struct Tracked {
  explicit Tracked(std::atomic<int>* counter) : destroyed(counter) {}
  Tracked(Tracked&& other) noexcept : destroyed(other.destroyed) { other.destroyed = nullptr; }
  ~Tracked() {
    if (destroyed != nullptr) ++*destroyed;
  }
  std::atomic<int>* destroyed;
};
}  // namespace mloom_test

namespace mloom::zeromq {
namespace {

using mloom_test::Reading;
using mloom_test::Tracked;
using namespace std::chrono_literals;

constexpr Timeout kTestTimeout = 5s;

std::string UniqueName(const std::string& base) {
  static std::atomic<int> counter{0};
  return base + "-" + std::to_string(::getpid()) + "-" + std::to_string(counter++);
}

std::unique_ptr<PubSocket> Publisher(MessageQueue& queue, const std::string& spec) {
  auto [socket, status] = queue.CreatePublisher(spec);
  EXPECT_TRUE(status.ok()) << status;
  return std::move(socket);
}

std::unique_ptr<SubSocket> Subscriber(MessageQueue& queue, const std::string& spec) {
  auto [socket, status] = queue.CreateSubscriber(spec);
  EXPECT_TRUE(status.ok()) << status;
  return std::move(socket);
}

TEST(ZmqMessageQueueTest, InprocPassesTheSameMessageObject) {
  ZmqMessageQueue queue;
  const std::string address = "inproc://" + UniqueName("readings");
  auto pub = Publisher(queue, address);
  auto sub = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());

  MessagePtr sent = MakeMessage(Reading{3, 1.5});
  ASSERT_TRUE(pub->Send(sent).ok());
  auto [received, status] = sub->Receive(kTestTimeout);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(received.get(), sent.get());  // zero copy: same object
  ASSERT_TRUE(received->IsType<Reading>());
  EXPECT_EQ(received->As<Reading>()->sensor, 3);
}

TEST(ZmqMessageQueueTest, FanOutSharesOneMessage) {
  ZmqMessageQueue queue;
  const std::string address = "inproc://" + UniqueName("fanout");
  auto pub = Publisher(queue, address);
  auto first = Subscriber(queue, address);
  auto second = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(2, kTestTimeout).ok());

  ASSERT_TRUE(pub->Send(MakeMessage(Reading{1, 2.0})).ok());
  auto [a, status_a] = first->Receive(kTestTimeout);
  auto [b, status_b] = second->Receive(kTestTimeout);
  ASSERT_TRUE(status_a.ok() && status_b.ok());
  EXPECT_EQ(a.get(), b.get());
  EXPECT_EQ(a.use_count(), 2);
}

TEST(ZmqMessageQueueTest, ReceiveTimesOut) {
  ZmqMessageQueue queue;
  auto sub = Subscriber(queue, "inproc://" + UniqueName("silent"));
  EXPECT_EQ(sub->Receive(kNoWait).second.code(), StatusCode::kTimeout);
  EXPECT_EQ(sub->Receive(10ms).second.code(), StatusCode::kTimeout);
}

TEST(ZmqMessageQueueTest, WaitForSubscribersTimesOutAndTracksLeaving) {
  ZmqMessageQueue queue;
  const std::string address = "inproc://" + UniqueName("lonely");
  auto pub = Publisher(queue, address);
  EXPECT_EQ(pub->WaitForSubscribers(1, 20ms).code(), StatusCode::kTimeout);
  {
    auto sub = Subscriber(queue, address);
    ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
  }
  // The subscriber left; its unsubscription must eventually be noticed.
  const auto deadline = std::chrono::steady_clock::now() + kTestTimeout;
  Status status;
  while (std::chrono::steady_clock::now() < deadline) {
    status = pub->WaitForSubscribers(1, 10ms);
    if (status.code() == StatusCode::kTimeout) break;
  }
  EXPECT_EQ(status.code(), StatusCode::kTimeout);
}

TEST(ZmqMessageQueueTest, DroppedAndUnreadFramesReleaseTheirMessage) {
  std::atomic<int> destroyed{0};
  {
    ZmqMessageQueue queue;
    const std::string address = "inproc://" + UniqueName("drop");
    auto pub = Publisher(queue, address);
    // No subscriber yet: the frame is dropped immediately.
    ASSERT_TRUE(pub->Send(MakeMessage(Tracked(&destroyed))).ok());
    EXPECT_EQ(destroyed.load(), 1);

    // A subscriber that never reads: released when the sockets and context go away.
    auto sub = Subscriber(queue, address);
    ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
    ASSERT_TRUE(pub->Send(MakeMessage(Tracked(&destroyed))).ok());
    EXPECT_EQ(destroyed.load(), 1);
  }
  EXPECT_EQ(destroyed.load(), 2);
}

TEST(ZmqMessageQueueTest, ReceivedMessageOutlivesItsFrame) {
  std::atomic<int> destroyed{0};
  ZmqMessageQueue queue;
  const std::string address = "inproc://" + UniqueName("owned");
  auto pub = Publisher(queue, address);
  auto sub = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
  ASSERT_TRUE(pub->Send(MakeMessage(Tracked(&destroyed))).ok());
  auto [message, status] = sub->Receive(kTestTimeout);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(destroyed.load(), 0);
  EXPECT_EQ(message.use_count(), 1);  // the frame's reference is gone, ours is the only one
  message.reset();
  EXPECT_EQ(destroyed.load(), 1);
}

TEST(ZmqMessageQueueTest, TcpSerializesThroughCodecs) {
  // Two queues = two ZeroMQ contexts, as in two processes: only tcp can connect them.
  ZmqMessageQueue sender;
  ZmqMessageQueue receiver;
  ASSERT_TRUE(sender.codecs().RegisterTrivial<Reading>().ok());
  ASSERT_TRUE(receiver.codecs().RegisterTrivial<Reading>().ok());

  auto pub = Publisher(sender, "tcp://127.0.0.1:*");
  const std::string address = pub->endpoints().front().address;
  EXPECT_NE(address.find("127.0.0.1:"), std::string::npos);
  EXPECT_EQ(address.find('*'), std::string::npos);  // wildcard port resolved
  auto sub = Subscriber(receiver, address);
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());

  MessagePtr sent = MakeMessage(Reading{9, -0.5});
  ASSERT_TRUE(pub->Send(sent).ok());
  auto [received, status] = sub->Receive(kTestTimeout);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_NE(received.get(), sent.get());  // a decoded copy
  ASSERT_TRUE(received->IsType<Reading>());
  EXPECT_EQ(received->As<Reading>()->sensor, 9);
  EXPECT_EQ(received->As<Reading>()->value, -0.5);
}

TEST(ZmqMessageQueueTest, IpcSerializesThroughCodecs) {
  ZmqMessageQueue queue;
  ASSERT_TRUE(queue.codecs().RegisterTrivial<Reading>().ok());
  const std::string address = "ipc:///tmp/" + UniqueName("mloom");
  auto pub = Publisher(queue, address);
  auto sub = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
  ASSERT_TRUE(pub->Send(MakeMessage(Reading{4, 4.0})).ok());
  auto [received, status] = sub->Receive(kTestTimeout);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(received->As<Reading>()->sensor, 4);
}

TEST(ZmqMessageQueueTest, RemoteSendWithoutCodecIsUnsupported) {
  ZmqMessageQueue queue;
  auto pub = Publisher(queue, "tcp://127.0.0.1:*");
  EXPECT_EQ(pub->Send(MakeMessage(Reading{})).code(), StatusCode::kUnsupported);
}

TEST(ZmqMessageQueueTest, SharedCodecRegistry) {
  auto codecs = std::make_shared<CodecRegistry>();
  ZmqMessageQueue first({.codecs = codecs});
  ZmqMessageQueue second({.codecs = codecs});
  ASSERT_TRUE(first.codecs().RegisterTrivial<Reading>().ok());
  EXPECT_TRUE(second.codecs().Contains<Reading>());
}

TEST(ZmqMessageQueueTest, RejectsInvalidSocketConfigurations) {
  ZmqMessageQueue queue;
  EXPECT_EQ(queue.CreatePublisher("inproc://a,tcp://127.0.0.1:*").second.code(),
            StatusCode::kInvalidArgument);
  EXPECT_EQ(queue.CreateSubscriber("udp://127.0.0.1:5000").second.code(), StatusCode::kUnsupported);
  EXPECT_EQ(queue.CreatePublisher("bogus").second.code(), StatusCode::kInvalidArgument);
  const std::string address = "inproc://" + UniqueName("taken");
  auto pub = Publisher(queue, address);
  EXPECT_EQ(queue.CreatePublisher(address).second.code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(pub->Send(nullptr).code(), StatusCode::kInvalidArgument);
}

TEST(ZmqMessageQueueTest, SubscriberCanBindAndPublisherConnect) {
  ZmqMessageQueue queue;
  const std::string address = "inproc://" + UniqueName("reverse");
  auto sub = Subscriber(queue, "@" + address);
  auto pub = Publisher(queue, ">" + address);
  // inproc has no I/O thread: a binding subscriber completes the join only while it is used.
  Status joined = Status::Timeout();
  for (int i = 0; i < 500 && !joined.ok(); ++i) {
    EXPECT_EQ(sub->Receive(kNoWait).second.code(), StatusCode::kTimeout);
    joined = pub->WaitForSubscribers(1, 10ms);
  }
  ASSERT_TRUE(joined.ok()) << joined;
  ASSERT_TRUE(pub->Send(MakeMessage(1)).ok());
  auto [message, status] = sub->Receive(kTestTimeout);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(*message->As<int>(), 1);
}

TEST(ZmqMessageQueueTest, SubscriberFansInFromSeveralPublishers) {
  ZmqMessageQueue queue;
  const std::string a = "inproc://" + UniqueName("left");
  const std::string b = "inproc://" + UniqueName("right");
  auto pub_a = Publisher(queue, a);
  auto pub_b = Publisher(queue, b);
  auto sub = Subscriber(queue, a + "," + b);
  ASSERT_TRUE(pub_a->WaitForSubscribers(1, kTestTimeout).ok());
  ASSERT_TRUE(pub_b->WaitForSubscribers(1, kTestTimeout).ok());
  ASSERT_TRUE(pub_a->Send(MakeMessage(1)).ok());
  ASSERT_TRUE(pub_b->Send(MakeMessage(2)).ok());
  int sum = 0;
  for (int i = 0; i < 2; ++i) {
    auto [message, status] = sub->Receive(kTestTimeout);
    ASSERT_TRUE(status.ok()) << status;
    sum += *message->As<int>();
  }
  EXPECT_EQ(sum, 3);
}

TEST(ZmqPollerTest, ReportsReadableSockets) {
  ZmqMessageQueue queue;
  const std::string a = "inproc://" + UniqueName("poll-a");
  const std::string b = "inproc://" + UniqueName("poll-b");
  auto pub_a = Publisher(queue, a);
  auto pub_b = Publisher(queue, b);
  auto sub_a = Subscriber(queue, a);
  auto sub_b = Subscriber(queue, b);
  ASSERT_TRUE(pub_a->WaitForSubscribers(1, kTestTimeout).ok());
  ASSERT_TRUE(pub_b->WaitForSubscribers(1, kTestTimeout).ok());

  auto poller = queue.CreatePoller();
  EXPECT_EQ(poller->Add(*sub_a).first, 0u);
  EXPECT_EQ(poller->Add(*sub_b).first, 1u);

  std::vector<std::size_t> ready;
  EXPECT_EQ(poller->Wait(10ms, ready).code(), StatusCode::kTimeout);
  EXPECT_TRUE(ready.empty());

  ASSERT_TRUE(pub_b->Send(MakeMessage(7)).ok());
  ASSERT_TRUE(poller->Wait(kTestTimeout, ready).ok());
  EXPECT_EQ(ready, std::vector<std::size_t>{1});
}

TEST(ZmqPollerTest, ServiceOnlySocketsAcceptPublishersButAreNeverReported) {
  ZmqMessageQueue queue;
  const std::string address = "inproc://" + UniqueName("service");
  auto sub = Subscriber(queue, "@" + address);
  auto pub = Publisher(queue, ">" + address);  // connects after the bind: needs servicing
  auto poller = queue.CreatePoller();
  ASSERT_TRUE(poller->Add(*sub, PollInterest::kServiceOnly).second.ok());

  std::vector<std::size_t> ready;
  Status joined = Status::Timeout();
  for (int i = 0; i < 500 && !joined.ok(); ++i) {
    EXPECT_EQ(poller->Wait(1ms, ready).code(), StatusCode::kTimeout);
    joined = pub->WaitForSubscribers(1, 10ms);
  }
  ASSERT_TRUE(joined.ok()) << joined;

  ASSERT_TRUE(pub->Send(MakeMessage(3)).ok());
  EXPECT_EQ(poller->Wait(20ms, ready).code(), StatusCode::kTimeout);  // never reported
  auto [message, status] = sub->Receive(kTestTimeout);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(*message->As<int>(), 3);
}

TEST(ZmqPollerTest, RejectsForeignSockets) {
  class FakeSubSocket final : public SubSocket {
   public:
    std::pair<MessagePtr, Status> Receive(Timeout) override { return {nullptr, Status::Ok()}; }
    const std::vector<Endpoint>& endpoints() const noexcept override { return endpoints_; }

   private:
    std::vector<Endpoint> endpoints_;
  };
  ZmqMessageQueue queue;
  FakeSubSocket fake;
  auto poller = queue.CreatePoller();
  EXPECT_EQ(poller->Add(fake).second.code(), StatusCode::kInvalidArgument);
  std::vector<std::size_t> ready;
  EXPECT_EQ(poller->Wait(kWaitForever, ready).code(), StatusCode::kInvalidArgument);
}

TEST(ZmqMessageQueueTest, SocketsMayOutliveTheirQueue) {
  std::unique_ptr<PubSocket> pub;
  std::unique_ptr<SubSocket> sub;
  {
    ZmqMessageQueue queue;
    const std::string address = "inproc://" + UniqueName("orphan");
    pub = Publisher(queue, address);
    sub = Subscriber(queue, address);
  }
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
  ASSERT_TRUE(pub->Send(MakeMessage(5)).ok());
  EXPECT_TRUE(sub->Receive(kTestTimeout).second.ok());
}

// --- Backpressure -------------------------------------------------------------------------------

ZmqOptions SmallQueues() {
  return ZmqOptions{.send_high_water_mark = 2, .receive_high_water_mark = 2};
}

/// Publishes numbered messages without waiting until a subscriber is full; returns how many
/// went out.
int FillUp(PubSocket& pub) {
  int sent = 0;
  while (sent < 1000 && pub.Send(MakeMessage(sent)).ok()) ++sent;
  return sent;
}

TEST(ZmqBackpressureTest, FullSubscriberMakesSendFailInsteadOfDropping) {
  ZmqMessageQueue queue(SmallQueues());
  const std::string address = "inproc://" + UniqueName("full");
  auto pub = Publisher(queue, address);
  auto sub = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());

  const int capacity = FillUp(*pub);
  EXPECT_GT(capacity, 0);
  EXPECT_LE(capacity, 8);  // the high-water marks, not more
  EXPECT_EQ(pub->Send(MakeMessage(-1)).code(), StatusCode::kTimeout);
  EXPECT_EQ(pub->Send(MakeMessage(-1), 20ms).code(), StatusCode::kTimeout);

  // Everything accepted arrives in order; the refused messages never do.
  for (int i = 0; i < capacity; ++i) {
    auto [message, status] = sub->Receive(kTestTimeout);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(*message->As<int>(), i);
  }
  EXPECT_EQ(sub->Receive(20ms).second.code(), StatusCode::kTimeout);
}

TEST(ZmqBackpressureTest, SendWaitsUntilTheSubscriberMakesRoom) {
  ZmqMessageQueue queue(SmallQueues());
  const std::string address = "inproc://" + UniqueName("wait");
  auto pub = Publisher(queue, address);
  auto sub = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
  const int capacity = FillUp(*pub);

  std::jthread reader([&sub, capacity] {  // from here on only this thread uses `sub`
    std::this_thread::sleep_for(50ms);
    for (int i = 0; i <= capacity; ++i) EXPECT_TRUE(sub->Receive(kTestTimeout).second.ok());
  });
  const auto start = std::chrono::steady_clock::now();
  EXPECT_TRUE(pub->Send(MakeMessage(capacity), kWaitForever).ok());
  EXPECT_GE(std::chrono::steady_clock::now() - start, 40ms);  // it really waited
  reader.join();
}

TEST(ZmqBackpressureTest, SlowestSubscriberHoldsBackEveryone) {
  ZmqMessageQueue queue(SmallQueues());
  const std::string address = "inproc://" + UniqueName("slowest");
  auto pub = Publisher(queue, address);
  auto fast = Subscriber(queue, address);
  auto slow = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(2, kTestTimeout).ok());
  const int capacity = FillUp(*pub);
  for (int i = 0; i < capacity; ++i) EXPECT_EQ(*fast->Receive(kTestTimeout).first->As<int>(), i);

  // The slow subscriber is still full, so nobody gets the next message.
  EXPECT_EQ(pub->Send(MakeMessage(capacity)).code(), StatusCode::kTimeout);
  EXPECT_EQ(fast->Receive(20ms).second.code(), StatusCode::kTimeout);

  // Once it caught up, both get it.
  for (int i = 0; i < capacity; ++i) EXPECT_EQ(*slow->Receive(kTestTimeout).first->As<int>(), i);
  ASSERT_TRUE(pub->Send(MakeMessage(capacity), kTestTimeout).ok());
  EXPECT_EQ(*fast->Receive(kTestTimeout).first->As<int>(), capacity);
  EXPECT_EQ(*slow->Receive(kTestTimeout).first->As<int>(), capacity);
}

TEST(ZmqBackpressureTest, RefusedMessagesAreReleased) {
  std::atomic<int> destroyed{0};
  ZmqMessageQueue queue(SmallQueues());
  const std::string address = "inproc://" + UniqueName("refused");
  auto pub = Publisher(queue, address);
  auto sub = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
  FillUp(*pub);
  EXPECT_EQ(pub->Send(MakeMessage(Tracked(&destroyed)), 10ms).code(), StatusCode::kTimeout);
  EXPECT_EQ(destroyed.load(), 1);
}

// --- Shutdown -----------------------------------------------------------------------------------

/// Upper bound for tearing sockets and contexts down; linger is 100 ms, sanitizers are slow.
constexpr auto kPromptShutdown = 2s;

/// How long `body` takes, including the destruction of everything it owns.
template <typename F>
std::chrono::steady_clock::duration Timed(F&& body) {
  const auto start = std::chrono::steady_clock::now();
  body();
  return std::chrono::steady_clock::now() - start;
}

TEST(ZmqShutdownTest, UnreadTcpBacklogDoesNotBlockShutdown) {
  const auto elapsed = Timed([] {
    ZmqMessageQueue sender;
    ZmqMessageQueue receiver;
    ASSERT_TRUE(sender.codecs().RegisterTrivial<Reading>().ok());
    ASSERT_TRUE(receiver.codecs().RegisterTrivial<Reading>().ok());
    auto pub = Publisher(sender, "tcp://127.0.0.1:*");
    auto sub = Subscriber(receiver, pub->endpoints().front().address);
    ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
    // More than both high-water marks hold; nothing is ever read.
    for (int i = 0; i < 5000; ++i) (void)pub->Send(MakeMessage(Reading{i, 0.0}));
  });
  EXPECT_LT(elapsed, kPromptShutdown);
}

TEST(ZmqShutdownTest, VanishedPeerWithQueuedDataDoesNotBlockShutdown) {
  const auto elapsed = Timed([] {
    ZmqMessageQueue sender;
    ASSERT_TRUE(sender.codecs().RegisterTrivial<Reading>().ok());
    auto pub = Publisher(sender, "tcp://127.0.0.1:*");
    {
      ZmqMessageQueue receiver;  // another "process"
      ASSERT_TRUE(receiver.codecs().RegisterTrivial<Reading>().ok());
      auto sub = Subscriber(receiver, pub->endpoints().front().address);
      ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
      for (int i = 0; i < 5000; ++i) (void)pub->Send(MakeMessage(Reading{i, 0.0}));
    }  // gone, with its unread data
    // Publishing to the vanished peer neither blocks nor fails.
    for (int i = 0; i < 1000; ++i) EXPECT_TRUE(pub->Send(MakeMessage(Reading{i, 0.0})).ok());
  });
  EXPECT_LT(elapsed, kPromptShutdown);
}

TEST(ZmqShutdownTest, SocketsMayBeUsedAndClosedOnAnotherThread) {
  ZmqMessageQueue queue;
  const std::string address = "inproc://" + UniqueName("moving");
  auto pub = Publisher(queue, address);
  auto sub = Subscriber(queue, address);
  ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
  // Starting a thread is the full memory barrier ZeroMQ needs to hand sockets over.
  std::jthread worker([pub = std::move(pub), sub = std::move(sub)]() mutable {
    EXPECT_TRUE(pub->Send(MakeMessage(1)).ok());
    EXPECT_TRUE(sub->Receive(kTestTimeout).second.ok());
    pub.reset();  // closed on the worker thread
    sub.reset();
  });
  worker.join();
}

TEST(ZmqShutdownTest, RepeatedSetupAndTeardownStaysPrompt) {
  // Each round leaves an unread message and a connection to an inproc endpoint that is never
  // bound, which libzmq only resolves while it terminates the context.
  const auto elapsed = Timed([] {
    for (int round = 0; round < 100; ++round) {
      ZmqMessageQueue queue;
      const std::string address = "inproc://" + UniqueName("churn");
      auto pub = Publisher(queue, address);
      auto sub = Subscriber(queue, address);
      auto dangling = Subscriber(queue, "inproc://" + UniqueName("never-bound"));
      ASSERT_TRUE(pub->WaitForSubscribers(1, kTestTimeout).ok());
      ASSERT_TRUE(pub->Send(MakeMessage(round)).ok());
    }
  });
  EXPECT_LT(elapsed, 5 * kPromptShutdown);
}

}  // namespace
}  // namespace mloom::zeromq
