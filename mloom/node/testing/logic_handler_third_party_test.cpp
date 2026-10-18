#include <gtest/gtest.h>

#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "mloom/node/logic_handler.hpp"
#include "mloom/node/testing/acme_integration.hpp"
#include "mloom/node/testing/third_party/acme_sensors.hpp"

// The acme library stands in for third-party code that already exists and cannot be changed.
// Every test integrates part of it into a logic or source handler with a lambda. The lambda
// adapts acme's calling and error conventions to LogicResult<Out>(const In&) or (In&&), and
// captures whatever acme state the logic needs.

namespace mloom {
namespace {

using testing::DecodeHandler;
using testing::FahrenheitHandler;
using testing::InvertHandler;
using testing::LimitsHandler;
using testing::MakeReading;
using testing::ParseHandler;
using testing::Payload;
using Lines = std::vector<std::string>;

/// Runs `handler` on `input` and returns the produced payload, or a default T if there is none.
template <typename T>
T Output(LogicHandlerBase& handler, MessagePtr input) {
  auto [output, status] = handler.Handle(std::move(input));
  EXPECT_TRUE(status.ok()) << status;
  const T* value = output ? output->As<T>() : nullptr;
  return value ? *value : T{};
}

template <typename T>
T Produced(SourceHandlerBase& source) {
  auto [output, status] = source.Produce();
  EXPECT_TRUE(status.ok()) << status;
  const T* value = output ? output->As<T>() : nullptr;
  return value ? *value : T{};
}

// --- Stateless integrations (acme_integration.hpp) -----------------------------------------------

TEST(ThirdPartyLogicTest, PlainFunction) {
  auto to_fahrenheit = FahrenheitHandler();
  static_assert(
      std::is_same_v<decltype(to_fahrenheit), std::shared_ptr<LogicHandler<double, double>>>);
  EXPECT_EQ(Output<double>(*to_fahrenheit, MakeMessage(100.0)), 212.0);
  EXPECT_EQ(Output<double>(*to_fahrenheit, MakeMessage(-40.0)), -40.0);
}

TEST(ThirdPartyLogicTest, BoolResultWithOutParameter) {
  auto parse = ParseHandler();
  static_assert(
      std::is_same_v<decltype(parse), std::shared_ptr<LogicHandler<std::string, acme::Reading>>>);
  const acme::Reading reading =
      Output<acme::Reading>(*parse, MakeMessage(std::string("boiler=71.5")));
  EXPECT_EQ(reading.sensor, "boiler");
  EXPECT_EQ(reading.celsius, 71.5);

  auto [output, status] = parse->Handle(MakeMessage(std::string("boiler:hot")));
  EXPECT_EQ(output, nullptr);
  EXPECT_EQ(status, Status::InvalidArgument("cannot parse 'boiler:hot'"));
}

TEST(ThirdPartyLogicTest, OptionalResultFiltersMessages) {
  auto limits = LimitsHandler(80.0, 95.0);
  auto [quiet, quiet_status] = limits->Handle(MakeMessage(MakeReading("boiler", 60.0)));
  EXPECT_EQ(quiet, nullptr);
  EXPECT_EQ(quiet_status.code(), StatusCode::kNeedMoreData);

  const acme::Alarm warning =
      Output<acme::Alarm>(*limits, MakeMessage(MakeReading("boiler", 85.0)));
  EXPECT_EQ(warning.level, acme::Level::kWarning);
  const acme::Alarm critical =
      Output<acme::Alarm>(*limits, MakeMessage(MakeReading("boiler", 97.0)));
  EXPECT_EQ(critical.level, acme::Level::kCritical);
  EXPECT_EQ(critical.sensor, "boiler");
  EXPECT_EQ(critical.celsius, 97.0);
}

TEST(ThirdPartyLogicTest, ExceptionsBecomeStatuses) {
  auto decode = DecodeHandler();
  EXPECT_EQ(Output<Payload>(*decode, MakeMessage(acme::Frame{1, {1, 2, 3, 6}})),
            (Payload{1, 2, 3}));

  auto [corrupt, corrupt_status] = decode->Handle(MakeMessage(acme::Frame{2, {1, 2, 3, 7}}));
  EXPECT_EQ(corrupt, nullptr);
  EXPECT_EQ(corrupt_status, Status::InvalidArgument("frame 2 has a bad checksum"));
  EXPECT_EQ(decode->Handle(MakeMessage(acme::Frame{3, {}})).second,
            Status::InvalidArgument("frame 3 is empty"));
}

TEST(ThirdPartyLogicTest, TakingApiConsumesThePayload) {
  auto invert = InvertHandler();
  EXPECT_TRUE(invert->consumes_input());

  acme::Frame frame{7, {0x00, 0x0f, 0xff}};
  const std::uint8_t* storage = frame.bytes.data();
  auto [output, status] = invert->Handle(MakeMessage(std::move(frame)));
  ASSERT_TRUE(status.ok()) << status;
  const acme::Frame& inverted = *output->As<acme::Frame>();
  EXPECT_EQ(inverted.id, 7u);
  EXPECT_EQ(inverted.bytes, (Payload{0xff, 0xf0, 0x00}));
  EXPECT_EQ(inverted.bytes.data(), storage);  // moved end to end, never copied

  // A message that other nodes hold as well is copied first; they never see the inversion.
  MessagePtr shared = MakeMessage(acme::Frame{8, {0x01}});
  auto [copy, copy_status] = invert->Handle(shared);
  ASSERT_TRUE(copy_status.ok()) << copy_status;
  EXPECT_EQ(copy->As<acme::Frame>()->bytes, Payload{0xfe});
  EXPECT_EQ(shared->As<acme::Frame>()->bytes, Payload{0x01});
}

// --- Stateful integrations ----------------------------------------------------------------------

TEST(ThirdPartyStateTest, CapturedByReferenceTheCallerKeepsTheState) {
  // `average` must outlive the handler. In a running node it is used on the node's thread, so
  // nobody else may touch it meanwhile.
  acme::MovingAverage average(3);
  auto smooth = MakeLogicHandler([&average](const double& value) {
    average.Add(value);
    if (!average.full()) return std::pair{0.0, Status::NeedMoreData()};
    return std::pair{average.average(), Status::Ok()};
  });
  EXPECT_EQ(smooth->Handle(MakeMessage(1.0)).second.code(), StatusCode::kNeedMoreData);
  EXPECT_EQ(smooth->Handle(MakeMessage(2.0)).second.code(), StatusCode::kNeedMoreData);
  EXPECT_EQ(Output<double>(*smooth, MakeMessage(3.0)), 2.0);
  EXPECT_EQ(Output<double>(*smooth, MakeMessage(7.0)), 4.0);  // (2 + 3 + 7) / 3
  EXPECT_EQ(average.count(), 4u);                             // the caller sees the updates
}

TEST(ThirdPartyStateTest, CapturedByValueTheHandlerOwnsACopy) {
  acme::MovingAverage warmed_up(2);
  warmed_up.Add(10.0);
  auto smooth = MakeLogicHandler([average = warmed_up](const double& value) mutable {
    average.Add(value);
    return std::pair{average.average(), Status::Ok()};
  });
  EXPECT_EQ(Output<double>(*smooth, MakeMessage(20.0)), 15.0);  // continues from the copy
  EXPECT_EQ(Output<double>(*smooth, MakeMessage(30.0)), 25.0);
  EXPECT_EQ(warmed_up.count(), 1u);  // the original is untouched
}

TEST(ThirdPartyStateTest, StateCreatedInTheCapture) {
  auto split =
      MakeLogicHandler([splitter = acme::LineSplitter()](const std::string& chunk) mutable {
        splitter.Feed(chunk);
        Lines lines = splitter.TakeLines();
        const Status status = lines.empty() ? Status::NeedMoreData() : Status::Ok();
        return std::pair{std::move(lines), status};
      });
  // Partial lines stay buffered inside the handler until a later chunk completes them.
  EXPECT_EQ(split->Handle(MakeMessage(std::string("boi"))).second.code(),
            StatusCode::kNeedMoreData);
  EXPECT_EQ(Output<Lines>(*split, MakeMessage(std::string("ler=71.5\nmix"))), Lines{"boiler=71.5"});
  EXPECT_EQ(Output<Lines>(*split, MakeMessage(std::string("er=20\npump=5\n"))),
            (Lines{"mixer=20", "pump=5"}));
}

TEST(ThirdPartyStateTest, SharedPtrStateCanBeReconfiguredFromOutside) {
  auto calibrator = std::make_shared<acme::Calibrator>();
  auto calibrate = MakeLogicHandler([calibrator](acme::Reading&& reading) {
    reading.celsius = calibrator->Apply(reading.celsius);
    return std::pair{std::move(reading), Status::Ok()};
  });
  EXPECT_EQ(Output<acme::Reading>(*calibrate, MakeMessage(MakeReading("boiler", 20.0))).celsius,
            20.0);
  // Single-threaded here; a running node would need the change synchronized with its thread.
  calibrator->Configure(2.0, -1.0);
  EXPECT_EQ(Output<acme::Reading>(*calibrate, MakeMessage(MakeReading("boiler", 20.0))).celsius,
            39.0);

  const std::weak_ptr<acme::Calibrator> watch = calibrator;
  calibrator.reset();
  EXPECT_FALSE(watch.expired());  // the handler shares ownership
  calibrate.reset();
  EXPECT_TRUE(watch.expired());
}

TEST(ThirdPartyStateTest, NonCopyableObjectsAreHeldThroughSharedPtr) {
  // std::function copies its callable, so a lambda cannot own a non-copyable object directly.
  static_assert(!std::is_copy_constructible_v<acme::Recorder>);
  auto recorder = std::make_shared<acme::Recorder>(2);
  auto record = MakeLogicHandler([recorder](acme::Reading&& reading) {
    if (!recorder->Write(std::move(reading))) return Status::Error("recorder is full");
    return Status::Ok();
  });
  static_assert(
      std::is_same_v<decltype(record), std::shared_ptr<LogicHandler<acme::Reading, void>>>);
  EXPECT_TRUE(record->Handle(MakeMessage(MakeReading("boiler", 70.0))).second.ok());
  EXPECT_TRUE(record->Handle(MakeMessage(MakeReading("mixer", 40.0))).second.ok());
  EXPECT_EQ(record->Handle(MakeMessage(MakeReading("pump", 30.0))).second,
            Status::Error("recorder is full"));
  ASSERT_EQ(recorder->records().size(), 2u);
  EXPECT_EQ(recorder->records()[0].sensor, "boiler");
  EXPECT_EQ(recorder->records()[1].sensor, "mixer");
}

TEST(ThirdPartyStateTest, TwoHandlersShareOneObject) {
  // E.g. two routes of one node: parsed readings on one input, text lines on another.
  auto recorder = std::make_shared<acme::Recorder>(10);
  auto record_reading = MakeLogicHandler([recorder](acme::Reading&& reading) {
    return recorder->Write(std::move(reading)) ? Status::Ok() : Status::Error("recorder is full");
  });
  auto record_line = MakeLogicHandler([recorder](const std::string& line) {
    acme::Reading reading;
    if (!acme::ParseReading(line, &reading)) {
      return Status::InvalidArgument("cannot parse '" + line + "'");
    }
    return recorder->Write(std::move(reading)) ? Status::Ok() : Status::Error("recorder is full");
  });
  EXPECT_TRUE(record_reading->Handle(MakeMessage(MakeReading("boiler", 70.0))).second.ok());
  EXPECT_TRUE(record_line->Handle(MakeMessage(std::string("mixer=40"))).second.ok());
  EXPECT_EQ(record_line->Handle(MakeMessage(std::string("mixer"))).second.code(),
            StatusCode::kInvalidArgument);
  ASSERT_EQ(recorder->records().size(), 2u);
  EXPECT_EQ(recorder->records()[1].celsius, 40.0);
}

TEST(ThirdPartyStateTest, CallbackResultsAreCollectedInSharedStorage) {
  // The detector reports through a callback during Process(). The callback must not point into
  // the lambda, because std::function copies the lambda; storage behind a shared_ptr survives
  // the copy.
  auto detector = std::make_shared<acme::JumpDetector>(5.0);
  auto jumps = std::make_shared<std::vector<acme::Jump>>();
  detector->OnJump([jumps](const acme::Jump& jump) { jumps->push_back(jump); });
  auto detect = MakeLogicHandler([detector, jumps](const acme::Reading& reading) {
    jumps->clear();
    detector->Process(reading);
    const Status status = jumps->empty() ? Status::NeedMoreData() : Status::Ok();
    return std::pair{std::move(*jumps), status};
  });

  EXPECT_EQ(detect->Handle(MakeMessage(MakeReading("boiler", 20.0))).second.code(),
            StatusCode::kNeedMoreData);  // first value of the sensor
  EXPECT_EQ(detect->Handle(MakeMessage(MakeReading("boiler", 22.0))).second.code(),
            StatusCode::kNeedMoreData);  // below the threshold
  const auto found =
      Output<std::vector<acme::Jump>>(*detect, MakeMessage(MakeReading("boiler", 30.0)));
  ASSERT_EQ(found.size(), 1u);
  EXPECT_EQ(found[0].sensor, "boiler");
  EXPECT_EQ(found[0].from, 22.0);
  EXPECT_EQ(found[0].to, 30.0);
  EXPECT_EQ(detect->Handle(MakeMessage(MakeReading("mixer", 50.0))).second.code(),
            StatusCode::kNeedMoreData);  // sensors are tracked separately
}

TEST(ThirdPartyStateTest, CopiedHandlersCopyValueCapturesButShareSharedPtrs) {
  LogicHandler<double, double> owning(
      [average = acme::MovingAverage(10)](const double& value) mutable {
        average.Add(value);
        return std::pair{average.average(), Status::Ok()};
      });
  auto common = std::make_shared<acme::MovingAverage>(10);
  LogicHandler<double, double> sharing([common](const double& value) {
    common->Add(value);
    return std::pair{common->average(), Status::Ok()};
  });
  EXPECT_EQ(Output<double>(owning, MakeMessage(10.0)), 10.0);
  EXPECT_EQ(Output<double>(sharing, MakeMessage(10.0)), 10.0);

  LogicHandler<double, double> owning_copy(owning);
  LogicHandler<double, double> sharing_copy(sharing);
  EXPECT_EQ(Output<double>(owning_copy, MakeMessage(30.0)), 20.0);   // (10 + 30) / 2
  EXPECT_EQ(Output<double>(owning, MakeMessage(50.0)), 30.0);        // (10 + 50) / 2
  EXPECT_EQ(Output<double>(sharing_copy, MakeMessage(30.0)), 20.0);  // (10 + 30) / 2
  EXPECT_EQ(Output<double>(sharing, MakeMessage(50.0)), 30.0);       // (10 + 30 + 50) / 3
}

// --- Composition --------------------------------------------------------------------------------

/// Feeds `input` through `stages` the way connected nodes would pass it on. Stops at the first
/// stage that produces nothing and returns that stage's result.
std::pair<MessagePtr, Status> RunChain(const std::vector<std::shared_ptr<LogicHandlerBase>>& stages,
                                       MessagePtr input) {
  std::pair<MessagePtr, Status> result{std::move(input), Status::Ok()};
  for (const auto& stage : stages) {
    result = stage->Handle(std::move(result.first));
    if (!result.first) break;
  }
  return result;
}

TEST(ThirdPartyChainTest, IntegratedStepsChainLikeConnectedNodes) {
  auto calibrator = std::make_shared<acme::Calibrator>();
  calibrator->Configure(1.0, 2.0);
  std::vector<acme::Alarm> alarms;  // state of the final sink, kept by the test
  const std::vector<std::shared_ptr<LogicHandlerBase>> chain = {
      ParseHandler(),
      MakeLogicHandler([calibrator](acme::Reading&& reading) {
        reading.celsius = calibrator->Apply(reading.celsius);
        return std::pair{std::move(reading), Status::Ok()};
      }),
      LimitsHandler(80.0, 95.0),
      MakeLogicHandler([&alarms](acme::Alarm&& alarm) {
        alarms.push_back(std::move(alarm));
        return Status::Ok();
      }),
  };

  EXPECT_EQ(RunChain(chain, MakeMessage(std::string("boiler=70"))).second.code(),
            StatusCode::kNeedMoreData);  // 72 °C raises no alarm
  EXPECT_TRUE(RunChain(chain, MakeMessage(std::string("boiler=93"))).second.ok());
  EXPECT_EQ(RunChain(chain, MakeMessage(std::string("garbage"))).second.code(),
            StatusCode::kInvalidArgument);
  EXPECT_TRUE(RunChain(chain, MakeMessage(std::string("mixer=80"))).second.ok());

  ASSERT_EQ(alarms.size(), 2u);
  EXPECT_EQ(alarms[0].sensor, "boiler");
  EXPECT_EQ(alarms[0].celsius, 95.0);
  EXPECT_EQ(alarms[0].level, acme::Level::kCritical);
  EXPECT_EQ(alarms[1].sensor, "mixer");
  EXPECT_EQ(alarms[1].level, acme::Level::kWarning);
}

// --- Sources ------------------------------------------------------------------------------------

TEST(ThirdPartySourceTest, GeneratorOwnedByTheLambda) {
  auto generate =
      MakeSourceHandler([generator = acme::ReadingGenerator("boiler", 20.0, 0.5, 3)]() mutable {
        if (!generator.HasNext()) return std::pair{acme::Reading{}, Status::EndOfStream()};
        return std::pair{generator.Next(), Status::Ok()};
      });
  static_assert(std::is_same_v<decltype(generate), std::shared_ptr<SourceHandler<acme::Reading>>>);
  for (const double expected : {20.0, 20.5, 21.0}) {
    EXPECT_EQ(Produced<acme::Reading>(*generate).celsius, expected);
  }
  EXPECT_EQ(generate->Produce().second.code(), StatusCode::kEndOfStream);
}

TEST(ThirdPartySourceTest, PolledDeviceSharedWithTheCaller) {
  auto sensor = std::make_shared<acme::PollingSensor>("pump", 2);  // ready on every second poll
  auto poll = MakeSourceHandler([sensor] {
    try {
      std::optional<acme::Reading> reading = sensor->Poll();
      if (!reading) return std::pair{acme::Reading{}, Status::NeedMoreData()};
      return std::pair{std::move(*reading), Status::Ok()};
    } catch (const std::exception& error) {
      return std::pair{acme::Reading{}, Status::Error(error.what())};
    }
  });
  EXPECT_EQ(poll->Produce().second.code(), StatusCode::kNeedMoreData);
  const acme::Reading first = Produced<acme::Reading>(*poll);
  EXPECT_EQ(first.sensor, "pump");
  EXPECT_EQ(first.celsius, 20.0);
  EXPECT_EQ(poll->Produce().second.code(), StatusCode::kNeedMoreData);
  EXPECT_EQ(Produced<acme::Reading>(*poll).celsius, 21.0);

  sensor->Disconnect();
  auto [nothing, failure] = poll->Produce();
  EXPECT_EQ(nothing, nullptr);
  EXPECT_EQ(failure, Status::Error("sensor 'pump' disconnected"));
}

}  // namespace
}  // namespace mloom
