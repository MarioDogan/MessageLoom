#include "mloom/node/logic_handler.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <type_traits>
#include <utility>

namespace mloom_test {
/// Counts copies, to tell moved payloads from copied ones.
struct Counted {
  static inline int copies = 0;
  Counted() = default;
  explicit Counted(std::string v) : value(std::move(v)) {}
  Counted(const Counted& other) : value(other.value) { ++copies; }
  Counted(Counted&&) noexcept = default;
  Counted& operator=(const Counted&) = delete;
  Counted& operator=(Counted&&) = delete;
  std::string value;
};

/// Function object with a const call operator.
struct Scale {
  int factor = 1;
  std::pair<int, mloom::Status> operator()(const int& value) const {
    return {value * factor, mloom::Status::Ok()};
  }
};

/// Accepts its input both borrowed and consumed.
struct BorrowOrConsume {
  std::pair<std::string, mloom::Status> operator()(const Counted&) const {
    return {"borrowed", mloom::Status::Ok()};
  }
  std::pair<std::string, mloom::Status> operator()(Counted&&) const {
    return {"consumed", mloom::Status::Ok()};
  }
};
}  // namespace mloom_test

namespace mloom {
namespace {

using mloom_test::BorrowOrConsume;
using mloom_test::Counted;
using mloom_test::Scale;

static_assert(std::is_same_v<LogicResult<void>, Status>);
static_assert(std::is_same_v<LogicResult<int>, std::pair<int, Status>>);

std::pair<int, Status> Twice(const int& value) { return {value * 2, Status::Ok()}; }
std::pair<int, Status> Thrice(const int& value) noexcept { return {value * 3, Status::Ok()}; }
std::pair<int, Status> FortyTwo() { return {42, Status::Ok()}; }

int IntOf(const std::pair<MessagePtr, Status>& result) {
  EXPECT_TRUE(result.second.ok()) << result.second;
  if (!result.first || !result.first->IsType<int>()) return -1;
  return *result.first->As<int>();
}

// --- Deduction ----------------------------------------------------------------------------------

TEST(LogicHandlerTest, BorrowingLogicReceivesThePayload) {
  auto handler =
      MakeLogicHandler([](const int& value) { return std::pair{value + 1, Status::Ok()}; });
  static_assert(std::is_same_v<decltype(handler), std::shared_ptr<LogicHandler<int, int>>>);
  EXPECT_FALSE(handler->consumes_input());
  EXPECT_EQ(handler->input_type(), TypeId::Of<int>());

  auto [output, status] = handler->Handle(MakeMessage(41));
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_TRUE(output->IsType<int>());
  EXPECT_EQ(*output->As<int>(), 42);
}

TEST(LogicHandlerTest, FunctionPointersAreDeduced) {
  auto handler = MakeLogicHandler(&Twice);
  EXPECT_EQ(*handler->Handle(MakeMessage(4)).first->As<int>(), 8);
}

TEST(LogicHandlerTest, EveryCallableKindIsDeduced) {
  auto function = MakeLogicHandler(Twice);
  auto noexcept_pointer = MakeLogicHandler(&Thrice);
  auto functor = MakeLogicHandler(Scale{4});
  auto noexcept_lambda = MakeLogicHandler(
      [](const int& value) noexcept { return std::pair{value + 1, Status::Ok()}; });
  auto mutable_lambda = MakeLogicHandler([sum = 0](const int& value) mutable {
    sum += value;
    return std::pair{sum, Status::Ok()};
  });
  auto mutable_noexcept_lambda = MakeLogicHandler([calls = 0](const int& value) mutable noexcept {
    return std::pair{value + ++calls, Status::Ok()};
  });
  using IntHandler = std::shared_ptr<LogicHandler<int, int>>;
  static_assert(std::is_same_v<decltype(function), IntHandler>);
  static_assert(std::is_same_v<decltype(noexcept_pointer), IntHandler>);
  static_assert(std::is_same_v<decltype(functor), IntHandler>);
  static_assert(std::is_same_v<decltype(noexcept_lambda), IntHandler>);
  static_assert(std::is_same_v<decltype(mutable_lambda), IntHandler>);
  static_assert(std::is_same_v<decltype(mutable_noexcept_lambda), IntHandler>);

  EXPECT_EQ(IntOf(function->Handle(MakeMessage(5))), 10);
  EXPECT_EQ(IntOf(noexcept_pointer->Handle(MakeMessage(5))), 15);
  EXPECT_EQ(IntOf(functor->Handle(MakeMessage(5))), 20);
  EXPECT_EQ(IntOf(noexcept_lambda->Handle(MakeMessage(5))), 6);
  // Mutable logic keeps its state from one message to the next.
  EXPECT_EQ(IntOf(mutable_lambda->Handle(MakeMessage(1))), 1);
  EXPECT_EQ(IntOf(mutable_lambda->Handle(MakeMessage(2))), 3);
  EXPECT_EQ(IntOf(mutable_lambda->Handle(MakeMessage(3))), 6);
  EXPECT_EQ(IntOf(mutable_noexcept_lambda->Handle(MakeMessage(10))), 11);
  EXPECT_EQ(IntOf(mutable_noexcept_lambda->Handle(MakeMessage(10))), 12);
}

TEST(LogicHandlerTest, WorksThroughTheBaseInterface) {
  std::shared_ptr<LogicHandlerBase> base =
      MakeLogicHandler([](const std::string& text) { return std::pair{text + "!", Status::Ok()}; });
  EXPECT_EQ(base->input_type(), TypeId::Of<std::string>());
  auto [output, status] = base->Handle(MakeMessage(std::string("hi")));
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(*output->As<std::string>(), "hi!");
}

// --- Type checks --------------------------------------------------------------------------------

TEST(LogicHandlerTest, RejectsWrongPayloadType) {
  auto handler = MakeLogicHandler(&Twice);
  auto [output, status] = handler->Handle(MakeMessage(std::string("four")));
  EXPECT_EQ(output, nullptr);
  EXPECT_EQ(status.code(), StatusCode::kTypeMismatch);
  EXPECT_NE(status.message().find("int"), std::string::npos);
  EXPECT_EQ(handler->Handle(nullptr).second.code(), StatusCode::kInvalidArgument);
}

TEST(LogicHandlerTest, PayloadTypesMustMatchExactly) {
  auto ints = MakeLogicHandler(&Twice);
  EXPECT_EQ(ints->Handle(MakeMessage(4L)).second.code(), StatusCode::kTypeMismatch);
  EXPECT_EQ(ints->Handle(MakeMessage(4u)).second.code(), StatusCode::kTypeMismatch);

  auto strings = MakeLogicHandler(
      [](const std::string& text) { return std::pair{text.size(), Status::Ok()}; });
  const char* literal = "text";
  auto [output, status] = strings->Handle(MakeMessage(literal));
  EXPECT_EQ(output, nullptr);
  EXPECT_EQ(status.code(), StatusCode::kTypeMismatch);
  // The message names what was expected and what arrived.
  EXPECT_NE(status.message().find(TypeName<std::string>()), std::string::npos) << status;
  EXPECT_NE(status.message().find(TypeName<const char*>()), std::string::npos) << status;
}

// --- Results ------------------------------------------------------------------------------------

TEST(LogicHandlerTest, NonOkResultsProduceNoMessage) {
  auto handler = MakeLogicHandler([](const int& value) {
    return std::pair{value, value < 0 ? Status::Error("negative") : Status::NeedMoreData()};
  });
  auto [more, more_status] = handler->Handle(MakeMessage(1));
  EXPECT_EQ(more, nullptr);
  EXPECT_EQ(more_status.code(), StatusCode::kNeedMoreData);
  auto [error, error_status] = handler->Handle(MakeMessage(-1));
  EXPECT_EQ(error, nullptr);
  EXPECT_EQ(error_status, Status::Error("negative"));
}

TEST(LogicHandlerTest, EndOfStreamDropsTheFinalValue) {
  auto handler = MakeLogicHandler(
      [](const int& value) { return std::pair{value, Status::EndOfStream("done")}; });
  auto [output, status] = handler->Handle(MakeMessage(7));
  EXPECT_EQ(output, nullptr);
  EXPECT_EQ(status, Status::EndOfStream("done"));
}

TEST(LogicHandlerTest, ExplicitTypesConvertTheResult) {
  auto handler = MakeLogicHandler<int, double>(
      [](const int& value) { return std::pair{value, Status::Ok()}; });
  auto [output, status] = handler->Handle(MakeMessage(3));
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_TRUE(output->IsType<double>());
  EXPECT_EQ(*output->As<double>(), 3.0);
}

// --- Borrowing and consuming --------------------------------------------------------------------

TEST(LogicHandlerTest, ConsumingLogicMovesFromUniquelyOwnedMessages) {
  Counted::copies = 0;
  auto handler = MakeLogicHandler([](Counted&& in) {
    Counted out(std::move(in));
    out.value += "!";
    return std::pair{std::move(out), Status::Ok()};
  });
  EXPECT_TRUE(handler->consumes_input());
  auto [output, status] = handler->Handle(MakeMessage(Counted("hi")));
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(output->As<Counted>()->value, "hi!");
  EXPECT_EQ(Counted::copies, 0);
}

TEST(LogicHandlerTest, ConsumingLogicCopiesSharedMessages) {
  Counted::copies = 0;
  auto handler = MakeLogicHandler([](Counted&& in) {
    Counted out(std::move(in));
    return std::pair{std::move(out), Status::Ok()};
  });
  MessagePtr shared = MakeMessage(Counted("keep"));
  auto [output, status] = handler->Handle(shared);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(Counted::copies, 1);
  EXPECT_EQ(shared->As<Counted>()->value, "keep");  // other owners still see the value
  EXPECT_EQ(output->As<Counted>()->value, "keep");
}

TEST(LogicHandlerTest, ConsumingASharedMessageReleasesItBeforeTheLogicRuns) {
  MessagePtr shared = MakeMessage(Counted("keep"));
  long owners_seen_by_logic = 0;
  auto handler = MakeLogicHandler([&](Counted&& in) {
    owners_seen_by_logic = shared.use_count();
    return std::pair{std::move(in), Status::Ok()};
  });
  ASSERT_TRUE(handler->Handle(shared).second.ok());
  EXPECT_EQ(owners_seen_by_logic, 1);  // only the test still holds the message
}

TEST(LogicHandlerTest, MoveOnlyPayloadsAreConsumedOnlyWhenUnique) {
  auto handler =
      MakeLogicHandler([](std::unique_ptr<int>&& in) { return std::pair{*in * 10, Status::Ok()}; });
  EXPECT_EQ(*handler->Handle(MakeMessage(std::make_unique<int>(3))).first->As<int>(), 30);

  MessagePtr shared = MakeMessage(std::make_unique<int>(3));
  auto [output, status] = handler->Handle(shared);
  EXPECT_EQ(output, nullptr);
  EXPECT_EQ(status.code(), StatusCode::kUnsupported);
  EXPECT_NE(status.message().find(TypeName<std::unique_ptr<int>>()), std::string::npos) << status;
  EXPECT_EQ(**shared->As<std::unique_ptr<int>>(), 3);  // left untouched
}

TEST(LogicHandlerTest, BorrowingIsPreferredWhenLogicAcceptsBoth) {
  LogicHandler<Counted, std::string> handler(BorrowOrConsume{});
  EXPECT_FALSE(handler.consumes_input());
  auto [output, status] = handler.Handle(MakeMessage(Counted("x")));
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(*output->As<std::string>(), "borrowed");
}

TEST(LogicHandlerTest, ByValueParametersBorrowAndCopyThePayload) {
  Counted::copies = 0;
  auto handler =
      MakeLogicHandler([](Counted in) { return std::pair{in.value.size(), Status::Ok()}; });
  static_assert(
      std::is_same_v<decltype(handler), std::shared_ptr<LogicHandler<Counted, std::size_t>>>);
  EXPECT_FALSE(handler->consumes_input());
  MessagePtr message = MakeMessage(Counted("abc"));
  EXPECT_EQ(*handler->Handle(message).first->As<std::size_t>(), 3u);
  EXPECT_EQ(Counted::copies, 1);  // the parameter is copied from the borrowed payload
  EXPECT_EQ(message->As<Counted>()->value, "abc");
}

// --- Sinks and raw messages ---------------------------------------------------------------------

TEST(LogicHandlerTest, SinksReturnABareStatus) {
  int seen = 0;
  auto sink = MakeLogicHandler([&seen](const int& value) {
    seen = value;
    return Status::Ok();
  });
  static_assert(std::is_same_v<decltype(sink), std::shared_ptr<LogicHandler<int, void>>>);
  auto [output, status] = sink->Handle(MakeMessage(5));
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(output, nullptr);
  EXPECT_EQ(seen, 5);
}

TEST(LogicHandlerTest, ConsumingSinksTakeThePayloadAndReportFailures) {
  std::string stored;
  auto sink = MakeLogicHandler([&stored](std::string&& text) {
    if (text.empty()) return Status::InvalidArgument("empty");
    stored = std::move(text);
    return Status::Ok();
  });
  static_assert(std::is_same_v<decltype(sink), std::shared_ptr<LogicHandler<std::string, void>>>);
  EXPECT_TRUE(sink->consumes_input());

  auto [output, status] = sink->Handle(MakeMessage(std::string("payload")));
  EXPECT_TRUE(status.ok()) << status;
  EXPECT_EQ(output, nullptr);
  EXPECT_EQ(stored, "payload");

  auto [failed_output, failure] = sink->Handle(MakeMessage(std::string()));
  EXPECT_EQ(failed_output, nullptr);
  EXPECT_EQ(failure, Status::InvalidArgument("empty"));
}

TEST(LogicHandlerTest, RawMessagesPassThroughWithoutTypeCheck) {
  auto forward =
      MakeLogicHandler([](const MessagePtr& message) { return std::pair{message, Status::Ok()}; });
  static_assert(
      std::is_same_v<decltype(forward), std::shared_ptr<LogicHandler<MessagePtr, MessagePtr>>>);
  MessagePtr input = MakeMessage(std::string("any"));
  auto [output, status] = forward->Handle(input);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(output.get(), input.get());
}

TEST(LogicHandlerTest, RawMessagesCanBeConsumed) {
  auto unwrap = MakeLogicHandler([](MessagePtr&& message) {
    const int* value = message->As<int>();
    return std::pair{value ? *value : -1, Status::Ok()};
  });
  static_assert(std::is_same_v<decltype(unwrap), std::shared_ptr<LogicHandler<MessagePtr, int>>>);
  EXPECT_TRUE(unwrap->consumes_input());
  EXPECT_EQ(IntOf(unwrap->Handle(MakeMessage(9))), 9);
  EXPECT_EQ(IntOf(unwrap->Handle(MakeMessage(std::string("x")))), -1);  // no type check
  EXPECT_EQ(unwrap->Handle(nullptr).second.code(), StatusCode::kInvalidArgument);
}

TEST(LogicHandlerTest, RawOutputMayBeEmpty) {
  auto ints_only = MakeLogicHandler([](const MessagePtr& message) {
    return std::pair{message->IsType<int>() ? message : MessagePtr{}, Status::Ok()};
  });
  EXPECT_EQ(IntOf(ints_only->Handle(MakeMessage(1))), 1);
  auto [output, status] = ints_only->Handle(MakeMessage(std::string("drop")));
  EXPECT_TRUE(status.ok()) << status;
  EXPECT_EQ(output, nullptr);
}

// --- Registration -------------------------------------------------------------------------------

TEST(LogicHandlerTest, RegisterAfterConstructionAndExplicitTypes) {
  LogicHandler<int, std::string> handler;
  EXPECT_FALSE(handler.registered());
  EXPECT_EQ(handler.Handle(MakeMessage(1)).second.code(), StatusCode::kError);

  handler.Register(
      [](const auto& value) { return std::pair{std::to_string(value), Status::Ok()}; });
  EXPECT_TRUE(handler.registered());
  EXPECT_EQ(*handler.Handle(MakeMessage(12)).first->As<std::string>(), "12");

  auto generic =
      MakeLogicHandler<int, int>([](auto&& value) { return std::pair{value, Status::Ok()}; });
  EXPECT_EQ(*generic->Handle(MakeMessage(3)).first->As<int>(), 3);
}

TEST(LogicHandlerTest, UnregisteredHandlerNamesItsInputType) {
  LogicHandler<std::string, int> handler;
  // Registration is checked before the input, so even a null message reports it.
  auto [output, status] = handler.Handle(nullptr);
  EXPECT_EQ(output, nullptr);
  EXPECT_EQ(status.code(), StatusCode::kError);
  EXPECT_NE(status.message().find(TypeName<std::string>()), std::string::npos) << status;
}

TEST(LogicHandlerTest, RegisterReplacesTheLogic) {
  LogicHandler<int, int> handler(
      [](const int& value) { return std::pair{value + 1, Status::Ok()}; });
  EXPECT_TRUE(handler.registered());
  EXPECT_FALSE(handler.consumes_input());
  EXPECT_EQ(IntOf(handler.Handle(MakeMessage(2))), 3);

  handler.Register([](int&& value) { return std::pair{value * 100, Status::Ok()}; });
  EXPECT_TRUE(handler.consumes_input());
  EXPECT_EQ(IntOf(handler.Handle(MakeMessage(2))), 200);
}

TEST(LogicHandlerTest, CopiesAndMovesKeepTheLogic) {
  LogicHandler<int, int> original(
      [](const int& value) { return std::pair{value + 1, Status::Ok()}; });
  // Copying from a non-const lvalue must pick the copy constructor, not register `original`.
  LogicHandler<int, int> copy(original);
  LogicHandler<int, int> moved(std::move(copy));
  EXPECT_TRUE(original.registered());
  EXPECT_TRUE(moved.registered());
  EXPECT_EQ(IntOf(original.Handle(MakeMessage(1))), 2);
  EXPECT_EQ(IntOf(moved.Handle(MakeMessage(1))), 2);
}

// --- SourceHandler ------------------------------------------------------------------------------

TEST(SourceHandlerTest, ProducesMessages) {
  int next = 0;
  auto source = MakeSourceHandler([&next] {
    const int value = next++;
    return std::pair{value, value >= 2 ? Status::EndOfStream() : Status::Ok()};
  });
  static_assert(std::is_same_v<decltype(source), std::shared_ptr<SourceHandler<int>>>);
  EXPECT_EQ(*source->Produce().first->As<int>(), 0);
  EXPECT_EQ(*source->Produce().first->As<int>(), 1);
  auto [last, status] = source->Produce();
  EXPECT_EQ(last, nullptr);
  EXPECT_EQ(status.code(), StatusCode::kEndOfStream);

  SourceHandler<double> empty;
  EXPECT_EQ(empty.Produce().second.code(), StatusCode::kError);
}

TEST(SourceHandlerTest, FunctionsAreDeduced) {
  auto by_reference = MakeSourceHandler(FortyTwo);
  auto by_pointer = MakeSourceHandler(&FortyTwo);
  static_assert(std::is_same_v<decltype(by_reference), std::shared_ptr<SourceHandler<int>>>);
  static_assert(std::is_same_v<decltype(by_pointer), std::shared_ptr<SourceHandler<int>>>);
  EXPECT_EQ(IntOf(by_reference->Produce()), 42);
  EXPECT_EQ(IntOf(by_pointer->Produce()), 42);
}

TEST(SourceHandlerTest, ExplicitOutputTypeConvertsTheResult) {
  auto source = MakeSourceHandler<std::string>([] { return std::pair{"text", Status::Ok()}; });
  static_assert(std::is_same_v<decltype(source), std::shared_ptr<SourceHandler<std::string>>>);
  auto [output, status] = source->Produce();
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(*output->As<std::string>(), "text");
}

TEST(SourceHandlerTest, RawMessagesArePublishedAsIs) {
  MessagePtr made = MakeMessage(7);
  auto source = MakeSourceHandler([made] { return std::pair{made, Status::Ok()}; });
  static_assert(std::is_same_v<decltype(source), std::shared_ptr<SourceHandler<MessagePtr>>>);
  EXPECT_EQ(source->Produce().first.get(), made.get());
}

TEST(SourceHandlerTest, NonOkResultsProduceNoMessage) {
  auto source = MakeSourceHandler([calls = 0]() mutable {
    ++calls;
    return std::pair{calls, calls == 1 ? Status::NeedMoreData() : Status::Error("broken")};
  });
  auto [waiting, waiting_status] = source->Produce();
  EXPECT_EQ(waiting, nullptr);
  EXPECT_EQ(waiting_status.code(), StatusCode::kNeedMoreData);
  auto [failed, failure] = source->Produce();
  EXPECT_EQ(failed, nullptr);
  EXPECT_EQ(failure, Status::Error("broken"));
}

TEST(SourceHandlerTest, RegisterLaterReplaceAndCopy) {
  SourceHandler<int> source;
  EXPECT_FALSE(source.registered());
  source.Register([] { return std::pair{1, Status::Ok()}; });
  EXPECT_TRUE(source.registered());
  EXPECT_EQ(IntOf(source.Produce()), 1);

  source.Register(&FortyTwo);
  EXPECT_EQ(IntOf(source.Produce()), 42);

  SourceHandler<int> copy(source);  // copy constructor, not a registration of `source`
  EXPECT_EQ(IntOf(copy.Produce()), 42);
}

TEST(SourceHandlerTest, WorksThroughTheBaseInterface) {
  std::shared_ptr<SourceHandlerBase> base = MakeSourceHandler(&FortyTwo);
  EXPECT_EQ(IntOf(base->Produce()), 42);
}

}  // namespace
}  // namespace mloom
