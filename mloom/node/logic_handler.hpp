#pragma once

#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#include "mloom/core/message.hpp"
#include "mloom/core/status.hpp"
#include "mloom/core/type_id.hpp"

namespace mloom {

/// Type-erased business logic that node loops run on every input message.
class LogicHandlerBase {
 public:
  virtual ~LogicHandlerBase() = default;

  /// Runs the logic on `input`. Returns the produced message, which is null when nothing was
  /// produced (sinks, NeedMoreData, failures), and the logic's status.
  virtual std::pair<MessagePtr, Status> Handle(MessagePtr input) = 0;

  /// The payload type the logic expects.
  virtual TypeId input_type() const noexcept = 0;
};

/// Type-erased producer logic that source loops run.
class SourceHandlerBase {
 public:
  virtual ~SourceHandlerBase() = default;

  /// Produces the next message. Status semantics as for LogicHandlerBase::Handle; EndOfStream
  /// ends the node loop.
  virtual std::pair<MessagePtr, Status> Produce() = 0;
};

/// What business logic returns: `std::pair<Out, Status>`, or a bare Status for sinks (void Out).
template <typename Out>
using LogicResult = std::conditional_t<std::is_void_v<Out>, Status, std::pair<Out, Status>>;

namespace detail {

template <typename>
inline constexpr bool kAlwaysFalse = false;

/// Converts a logic result into a message; only Ok results are wrapped.
template <typename Out>
std::pair<MessagePtr, Status> ToMessage(LogicResult<Out>&& result) {
  if constexpr (std::is_void_v<Out>) {
    return {nullptr, std::move(result)};
  } else {
    if (!result.second.ok()) return {nullptr, std::move(result.second)};
    if constexpr (std::is_same_v<Out, MessagePtr>) {
      return {std::move(result.first), Status::Ok()};
    } else {
      return {MakeMessage(std::move(result.first)), Status::Ok()};
    }
  }
}

template <typename Result>
struct ResultTraits {
  static_assert(kAlwaysFalse<Result>, "logic must return std::pair<Out, mloom::Status> or Status");
};
template <>
struct ResultTraits<Status> {
  using Out = void;
};
template <typename T>
struct ResultTraits<std::pair<T, Status>> {
  using Out = T;
};

/// Signature of a non-generic callable: Return and Args.
template <typename F>
struct CallableTraits : CallableTraits<decltype(&F::operator())> {};
template <typename R, typename... A>
struct CallableTraits<R(A...)> {
  using Return = R;
  using Args = std::tuple<A...>;
};
template <typename R, typename... A>
struct CallableTraits<R (*)(A...)> : CallableTraits<R(A...)> {};
template <typename R, typename... A>
struct CallableTraits<R (*)(A...) noexcept> : CallableTraits<R(A...)> {};
template <typename C, typename R, typename... A>
struct CallableTraits<R (C::*)(A...)> : CallableTraits<R(A...)> {};
template <typename C, typename R, typename... A>
struct CallableTraits<R (C::*)(A...) const> : CallableTraits<R(A...)> {};
template <typename C, typename R, typename... A>
struct CallableTraits<R (C::*)(A...) noexcept> : CallableTraits<R(A...)> {};
template <typename C, typename R, typename... A>
struct CallableTraits<R (C::*)(A...) const noexcept> : CallableTraits<R(A...)> {};

}  // namespace detail

/// Adapts typed 3rd-party logic to LogicHandlerBase. Register one of:
///
///   LogicResult<Out> f(const In&)  borrows the payload, which may be shared with other nodes.
///   LogicResult<Out> f(In&&)       consumes the payload. It is moved out when this handler holds
///                                  the only reference to the message, and copied otherwise, so
///                                  other subscribers never see a moved-from value.
///
/// A message whose payload is not an In is rejected with TypeMismatch. In = MessagePtr receives
/// the raw message without a type check; Out = MessagePtr publishes the returned message as is;
/// Out = void makes a sink returning a bare Status.
///
/// Logic reports failures through its Status. Exceptions are not caught: one that escapes ends
/// Node::Run with that exception, which terminates the process when the node runs on a
/// ThreadRunner thread.
template <typename In, typename Out>
class LogicHandler final : public LogicHandlerBase {
  static_assert(std::is_same_v<In, std::remove_cvref_t<In>>, "In must be a plain type");

 public:
  using Result = LogicResult<Out>;
  using BorrowFn = std::function<Result(const In&)>;
  using ConsumeFn = std::function<Result(In&&)>;

  LogicHandler() = default;

  template <typename F>
    requires(!std::is_same_v<std::remove_cvref_t<F>, LogicHandler>)
  explicit LogicHandler(F&& logic) {
    Register(std::forward<F>(logic));
  }

  /// Installs the business logic, replacing any previous one.
  template <typename F>
  void Register(F&& logic) {
    using Fn = std::decay_t<F>;
    if constexpr (std::is_invocable_r_v<Result, Fn&, const In&>) {
      logic_.template emplace<BorrowFn>(std::forward<F>(logic));
    } else if constexpr (std::is_invocable_r_v<Result, Fn&, In&&>) {
      logic_.template emplace<ConsumeFn>(std::forward<F>(logic));
    } else {
      static_assert(detail::kAlwaysFalse<F>,
                    "logic must be callable as LogicResult<Out>(const In&) or (In&&)");
    }
  }

  bool registered() const noexcept { return logic_.index() != 0; }
  bool consumes_input() const noexcept { return std::holds_alternative<ConsumeFn>(logic_); }
  TypeId input_type() const noexcept override { return TypeId::Of<In>(); }

  std::pair<MessagePtr, Status> Handle(MessagePtr input) override {
    if (!registered()) {
      return {nullptr, Status::Error("no logic registered for '" + std::string(TypeName<In>()) +
                                     "' handler")};
    }
    if (!input) return {nullptr, Status::InvalidArgument("null input message")};
    if constexpr (std::is_same_v<In, MessagePtr>) {
      if (auto* borrow = std::get_if<BorrowFn>(&logic_)) {
        return detail::ToMessage<Out>((*borrow)(input));
      }
      return detail::ToMessage<Out>(std::get<ConsumeFn>(logic_)(std::move(input)));
    } else {
      if (!input->IsType<In>()) {
        return {nullptr,
                Status::TypeMismatch("expected '" + std::string(TypeName<In>()) +
                                     "' but received '" + std::string(input->type().name()) + "'")};
      }
      auto& typed = static_cast<TypedMessage<In>&>(*input);
      if (auto* borrow = std::get_if<BorrowFn>(&logic_)) {
        return detail::ToMessage<Out>((*borrow)(typed.value()));
      }
      ConsumeFn& consume = std::get<ConsumeFn>(logic_);
      if (input.use_count() == 1) {
        return detail::ToMessage<Out>(consume(std::move(typed.mutable_value())));
      }
      if constexpr (std::is_copy_constructible_v<In>) {
        In copy(typed.value());
        input.reset();
        return detail::ToMessage<Out>(consume(std::move(copy)));
      } else {
        return {nullptr, Status::Unsupported("cannot consume a shared message of move-only type '" +
                                             std::string(TypeName<In>()) + "'")};
      }
    }
  }

 private:
  std::variant<std::monostate, BorrowFn, ConsumeFn> logic_;
};

/// Adapts typed 3rd-party producer logic `std::pair<Out, Status> f()` to SourceHandlerBase.
/// Exceptions are not caught, as for LogicHandler.
template <typename Out>
class SourceHandler final : public SourceHandlerBase {
  static_assert(!std::is_void_v<Out>, "a source must produce a value");

 public:
  using Result = std::pair<Out, Status>;

  SourceHandler() = default;

  template <typename F>
    requires(!std::is_same_v<std::remove_cvref_t<F>, SourceHandler>)
  explicit SourceHandler(F&& logic) {
    Register(std::forward<F>(logic));
  }

  template <typename F>
  void Register(F&& logic) {
    static_assert(std::is_invocable_r_v<Result, std::decay_t<F>&>,
                  "source logic must be callable as std::pair<Out, Status>()");
    logic_ = std::forward<F>(logic);
  }

  bool registered() const noexcept { return static_cast<bool>(logic_); }

  std::pair<MessagePtr, Status> Produce() override {
    if (!logic_) return {nullptr, Status::Error("no logic registered with source handler")};
    return detail::ToMessage<Out>(logic_());
  }

 private:
  std::function<Result()> logic_;
};

/// Creates a LogicHandler with In and Out deduced from a non-generic callable.
template <typename F>
auto MakeLogicHandler(F&& logic) {
  using Traits = detail::CallableTraits<std::remove_cvref_t<F>>;
  static_assert(std::tuple_size_v<typename Traits::Args> == 1,
                "logic handlers take exactly one argument");
  using In = std::remove_cvref_t<std::tuple_element_t<0, typename Traits::Args>>;
  using Out = typename detail::ResultTraits<std::remove_cvref_t<typename Traits::Return>>::Out;
  return std::make_shared<LogicHandler<In, Out>>(std::forward<F>(logic));
}

/// Creates a LogicHandler with explicit types, e.g. for generic lambdas.
template <typename In, typename Out, typename F>
std::shared_ptr<LogicHandler<In, Out>> MakeLogicHandler(F&& logic) {
  return std::make_shared<LogicHandler<In, Out>>(std::forward<F>(logic));
}

/// Creates a SourceHandler with Out deduced from the callable's return type.
template <typename F>
auto MakeSourceHandler(F&& logic) {
  using Result = std::remove_cvref_t<std::invoke_result_t<std::decay_t<F>&>>;
  using Out = typename detail::ResultTraits<Result>::Out;
  return std::make_shared<SourceHandler<Out>>(std::forward<F>(logic));
}

template <typename Out, typename F>
std::shared_ptr<SourceHandler<Out>> MakeSourceHandler(F&& logic) {
  return std::make_shared<SourceHandler<Out>>(std::forward<F>(logic));
}

}  // namespace mloom
