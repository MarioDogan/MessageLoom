# MessageLoom

A message passing, event based framework in C++20 (namespace `mloom`). Nodes exchange
type-erased messages over pub/sub sockets. Business logic stays plain C++ that knows nothing
about the framework, and the same nodes run either purely event driven or stepped by an
external orchestrator. The first transport is ZeroMQ.

```
bazel test //...              # GCC by default
bazel test --config=clang //...
bazel run //examples:pipeline
```

## Architecture

```
Node                       deployment unit
└─ NodeCore                runs the loop, applies control commands, reports status
   ├─ SocketManager        data inputs/outputs, one control and one status socket
   └─ NodeLoop             decides when the business logic runs
      └─ LogicHandler(s)   wraps 3rd-party business logic

Stage                      which node reads and writes which channel
NodeRunner                 where nodes run (ThreadRunner: one thread per node)
Orchestrator               drives nodes through their control and status sockets
```

| Layer | Package | Contents |
|---|---|---|
| Core | `//mloom/core` | `Status`, `Message` / `MessagePtr`, `TypeId`, `CodecRegistry`, `ByteWriter`/`ByteReader` |
| Transport | `//mloom/transport` | `MessageQueue`, `PubSocket`, `SubSocket`, `Poller`, endpoint syntax |
| ZeroMQ | `//mloom/transport/zmq` | `ZmqMessageQueue` |
| Node | `//mloom/node` | `LogicHandler`, `SocketManager`, the four `NodeLoop`s, `NodeCore`, `Node` |
| Orchestration | `//mloom/orchestration` | `Stage`, `NodeRunner` / `ThreadRunner`, `Orchestrator` |
| Everything | `//mloom` | `#include "mloom/mloom.hpp"` |

### Messages

A `Message` is type-erased; in-process it travels as `std::shared_ptr<Message>`. Connected nodes
know which type arrives on which socket, and a handler recovers it with
`message->IsType<T>()` / `As<T>()`. `IsType` compares a 64-bit hash of the compiler's type name,
which is stable across processes and is what goes on the wire. It then confirms the match with
`typeid`, so a hash collision cannot cause a wrong downcast.

### MessageQueue and sockets

`MessageQueue` creates pub and sub sockets and hides the transport. Endpoints are plain
addresses with an optional prefix: `@` binds, `>` connects, and without a prefix publishers
bind and subscribers connect. Lists are comma separated (`"inproc://a,inproc://b"`).

`ZmqMessageQueue`:
- **inproc**: the `MessagePtr` itself is passed. No copy and no serialization, and every
  subscriber shares the same `Message`. Frames that are dropped or never read release their
  message through ZeroMQ's free callback, so nothing leaks.
- **tcp / ipc**: messages are serialized through the `CodecRegistry` (`Register<T>(encode, decode)`,
  or `RegisterTrivial<T>()`). Both sides must register the type.
- Publishers are XPUB sockets, so `WaitForSubscribers(n)` can synchronize startup and avoid
  pub/sub's slow-joiner message loss.
- Publishers never drop messages silently. When a subscriber's queue is full
  (`send_high_water_mark`), `Send(message, timeout)` waits for room and then fails with
  `Timeout`, and no subscriber receives the message.
- UDP is not available: ZeroMQ supports it only with the draft RADIO/DISH API, which the BCR
  libzmq build does not enable.

### Nodes

```cpp
auto handler = mloom::MakeLogicHandler([](const Image& image) {      // or (Image&& image)
  return std::pair{Detect(image), mloom::Status::Ok()};             // NeedMoreData, Error, EndOfStream...
});
auto loop = std::make_unique<mloom::FreeRunningLoop>(
    std::vector<mloom::Route>{{/*input*/ 0, handler, /*outputs*/ {0}}});
auto sockets = std::make_unique<mloom::SocketManager>(queue, mloom::SocketLayout{
    .inputs = {"inproc://images"}, .outputs = {"inproc://detections"},
    .control = "inproc://control", .status = "inproc://detector/status"});
mloom::Node node("detector", std::make_unique<mloom::NodeCore>(std::move(sockets), std::move(loop)));
node.Run(stop_token);  // on the thread of your choice
```

- **Logic handlers** take `std::pair<Out, Status> f(const In&)` (borrow) or `f(In&&)` (consume).
  A consumed payload is moved out when the node holds the only reference to the message and
  copied otherwise, so other subscribers never see a moved-from value. A sink returns a plain
  `Status`. `In = MessagePtr` receives the raw message, and `Out = MessagePtr` forwards one.
- **Status codes** steer the loop: `Ok` publishes the result, `NeedMoreData` publishes nothing,
  `EndOfStream` ends the node, and any other code is reported on the status socket while the
  node keeps running.
- **Loops**

  | Loop | Inputs | Runs the logic |
  |---|---|---|
  | `FreeRunningSourceLoop` | none | continuously (optional `period`), checks control without blocking |
  | `FreeRunningLoop` | ≥ 1 | for every arriving message, checks control without blocking |
  | `TriggeredSourceLoop` | none | once per `Trigger` command |
  | `TriggeredLoop` | ≥ 1 | once per `Trigger`, on the next input (optional `input_timeout`) |

- **Control** commands: `Start`, `Pause`, `Stop`, `Trigger`, `QueryStatus`, `Finish`. Each one
  can target a single node by name. `Stop` ends a node right away; `Finish` lets a free-running
  node first process the input that is still queued, for at most
  `FreeRunningLoopOptions::drain_timeout` (60 s by default). **Status** reports carry the
  node's state, the event (`StateChanged`, `StepCompleted`, `Error`, `StatusReply`,
  `EndOfStream`), the echoed command sequence and its counters.
- **Startup barrier**: `NodeCoreOptions::min_output_subscribers` and `min_status_subscribers`
  make a node wait for its consumers before it runs.
- **Backpressure**: a node's data outputs wait for room at a slow consumer for up to
  `NodeCoreOptions::send_timeout` (60 s by default), and keep serving control commands
  meanwhile. Then the injected `on_backpressure` handler decides what happens to the message:
  `kSkip` drops it, `kReportError` drops it and reports an Error (the default without a
  handler), and `kRetry` waits once more.

### Orchestration and staging

```cpp
mloom::Stage stage("plant");
stage.Add({.name = "camera",   .outputs = {"frames"},                         .loop = MakeCameraLoop});
stage.Add({.name = "detector", .inputs = {"frames"}, .outputs = {"detections"}, .loop = MakeDetectorLoop});

auto queue = std::make_shared<mloom::zeromq::ZmqMessageQueue>();
auto [nodes, built] = stage.Build(queue);
auto [orchestrator, created] = mloom::Orchestrator::Create(queue, stage);
mloom::ThreadRunner runner;
runner.LaunchAll(std::move(nodes));
orchestrator->AwaitReady(5s);
orchestrator->StepAll(stage.TopologicalOrder(), 5s);   // or Start / Pause / Stop
orchestrator->Finish(stage.TopologicalOrder(), 5s);    // drain and stop, producers first
```

- `Stage` connects nodes through **named channels**, so no addresses are configured. Each
  producer binds its own endpoint and consumers connect to every producer of a channel, which
  gives fan-in and fan-out. The startup barriers come from the graph, so nothing is lost at
  startup.
- `NodeRunner` decides where nodes run. `ThreadRunner` gives each node a thread.
- `Orchestrator` controls the nodes over the control/status sockets and tracks each node's
  latest status.

See [examples/pipeline.cpp](examples/pipeline.cpp) for the whole flow, run event driven and
then orchestrated.

## Development

| Command | Purpose |
|---|---|
| `bazel test --config=strict //...` | warnings in MessageLoom sources are errors |
| `bazel test --config=asan //...` | AddressSanitizer + LeakSanitizer |
| `bazel test --config=tsan //...` | ThreadSanitizer |
| `tools/compile_commands.py` | writes `compile_commands.json` for clangd |

Bazel 9 (pinned in `.bazelversion`), with dependencies from the Bazel Central Registry: libzmq,
cppzmq and googletest.

## Notes and limitations

- There is no typed port system. Sockets carry type-erased messages, and the logic handler checks
  and recovers the type.
- A subscriber that *binds*, with the publisher connecting to it, only accepts publishers while
  it is being polled. Nodes keep their inputs serviced even while paused, but your own binding
  subscribers need `Poller::Add(socket, PollInterest::kServiceOnly)` or regular reads.
- Type hashes derive from compiler type names. Processes that exchange messages over tcp/ipc
  should be built with the same toolchain.
- `Stage` is single-process (inproc). Multi-process runners and tcp stage endpoints are the
  next step.
