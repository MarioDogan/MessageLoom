// A camera -> detector -> printer pipeline, run twice:
//   1. event driven: every node reacts to data as it arrives;
//   2. orchestrated: an external orchestrator steps triggered nodes one frame at a time.
//
// bazel run //examples:pipeline

#include <cstdio>
#include <functional>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "mloom/mloom.hpp"

namespace {

using namespace std::chrono_literals;

// --- 3rd-party business logic: plain types and functions, unaware of MessageLoom ------------

struct Frame {
  int id = 0;
  std::vector<int> pixels;
};

struct Detection {
  int frame_id = 0;
  int bright_pixels = 0;
};

class Camera {
 public:
  explicit Camera(int frames) : frames_(frames) {}
  std::pair<Frame, mloom::Status> Grab() {
    if (next_ >= frames_) return {Frame{}, mloom::Status::EndOfStream()};
    Frame frame{next_, std::vector<int>(64)};
    std::iota(frame.pixels.begin(), frame.pixels.end(), next_);
    ++next_;
    return {std::move(frame), mloom::Status::Ok()};
  }

 private:
  int frames_;
  int next_ = 0;
};

// Consumes its input (Frame&&): the pixels are moved, not copied, when no one else shares them.
std::pair<Detection, mloom::Status> Detect(Frame&& frame) {
  Detection detection{frame.id, 0};
  for (const int pixel : frame.pixels) detection.bright_pixels += pixel > 40 ? 1 : 0;
  return {detection, mloom::Status::Ok()};
}

mloom::Status Print(const Detection& detection) {
  std::printf("  frame %2d: %2d bright pixels\n", detection.frame_id, detection.bright_pixels);
  return mloom::Status::Ok();
}

// --- Integration: register the logic with handlers, describe nodes and channels ---------------

mloom::Stage MakeStage(const std::string& name, bool triggered, int frames) {
  mloom::Stage stage(name);
  auto check = [](mloom::Status status) {
    if (!status.ok()) std::fprintf(stderr, "stage: %s\n", status.ToString().c_str());
  };
  check(stage.Add({.name = "camera", .outputs = {"frames"}, .loop = [triggered, frames] {
                     auto camera = std::make_shared<Camera>(frames);
                     auto grab = mloom::MakeSourceHandler([camera] { return camera->Grab(); });
                     std::vector<std::size_t> outputs{0};
                     return triggered
                                ? std::unique_ptr<mloom::NodeLoop>(
                                      std::make_unique<mloom::TriggeredSourceLoop>(grab, outputs))
                                : std::make_unique<mloom::FreeRunningSourceLoop>(
                                      grab, outputs,
                                      mloom::FreeRunningSourceOptions{.period = 5ms});
                   }}));
  check(stage.Add(
      {.name = "detector", .inputs = {"frames"}, .outputs = {"detections"}, .loop = [triggered] {
         std::vector<mloom::Route> routes{{0, mloom::MakeLogicHandler(&Detect), {0}}};
         return triggered ? std::unique_ptr<mloom::NodeLoop>(
                                std::make_unique<mloom::TriggeredLoop>(std::move(routes)))
                          : std::make_unique<mloom::FreeRunningLoop>(std::move(routes));
       }}));
  check(stage.Add({.name = "printer", .inputs = {"detections"}, .loop = [] {
                     return std::make_unique<mloom::FreeRunningLoop>(
                         std::vector<mloom::Route>{{0, mloom::MakeLogicHandler(&Print), {}}});
                   }}));
  return stage;
}

// --- Deployment ----------------------------------------------------------------------------

int Run(const mloom::Stage& stage,
        const std::function<mloom::Status(mloom::Orchestrator&)>& drive) {
  auto queue = std::make_shared<mloom::zeromq::ZmqMessageQueue>();
  auto [nodes, built] = stage.Build(queue);
  auto [orchestrator, created] = mloom::Orchestrator::Create(queue, stage);
  if (!built.ok() || !created.ok()) {
    std::fprintf(stderr, "setup failed: %s %s\n", built.ToString().c_str(),
                 created.ToString().c_str());
    return 1;
  }
  mloom::ThreadRunner runner;
  (void)runner.LaunchAll(std::move(nodes));

  mloom::Status status = orchestrator->AwaitReady(5s);
  if (status.ok()) status = drive(*orchestrator);
  (void)orchestrator->Stop(5s);
  for (const mloom::NodeResult& result : runner.Wait()) {
    std::printf("  %-8s -> %s\n", result.node.c_str(), result.status.ToString().c_str());
  }
  if (!status.ok()) std::fprintf(stderr, "failed: %s\n", status.ToString().c_str());
  return status.ok() ? 0 : 1;
}

}  // namespace

int main() {
  std::printf("event driven: the camera runs freely until it runs out of frames\n");
  int failures = Run(MakeStage("event-driven", false, 8), [](mloom::Orchestrator& orchestrator) {
    return orchestrator.AwaitState("camera", mloom::NodeState::kStopped, 5s);
  });

  std::printf("\norchestrated: one Step per frame, in topological order\n");
  failures += Run(MakeStage("stepped", true, 100), [](mloom::Orchestrator& orchestrator) {
    for (int frame = 0; frame < 4; ++frame) {
      if (auto status = orchestrator.StepAll({"camera", "detector"}, 5s); !status.ok()) {
        return status;
      }
    }
    std::this_thread::sleep_for(50ms);  // let the free-running printer flush
    return mloom::Status::Ok();
  });
  return failures;
}
