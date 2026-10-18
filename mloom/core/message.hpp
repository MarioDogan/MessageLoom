#pragma once

#include <memory>
#include <type_traits>
#include <utility>

#include "mloom/core/type_id.hpp"

namespace mloom {

/// Type-erased unit of data passed between nodes.
///
/// Inside a process messages travel as MessagePtr: a published message is shared by every
/// subscriber, so receivers must treat it as immutable. Connected nodes know which payload type
/// arrives on which socket and recover it with IsType<T>() / As<T>().
class Message {
 public:
  virtual ~Message() = default;
  Message(const Message&) = delete;
  Message& operator=(const Message&) = delete;

  const TypeId& type() const noexcept { return type_; }

  /// True if the payload is a T. The stable type hash is compared first and confirmed with
  /// typeid, so a hash collision can never cause a wrong downcast.
  template <typename T>
  bool IsType() const noexcept {
    using U = std::remove_cvref_t<T>;
    return type_.hash() == TypeHash<U>() && type_.info() == typeid(U);
  }

  /// The payload if it is a T, otherwise nullptr.
  template <typename T>
  const std::remove_cvref_t<T>* As() const noexcept;

 protected:
  explicit Message(TypeId type) noexcept : type_(type) {}

 private:
  TypeId type_;
};

using MessagePtr = std::shared_ptr<Message>;

/// Message carrying a payload of type T.
template <typename T>
class TypedMessage final : public Message {
  static_assert(std::is_same_v<T, std::remove_cvref_t<T>>, "payload type must not be cv/ref");
  static_assert(!std::is_same_v<T, MessagePtr>, "a message cannot carry another MessagePtr");

 public:
  template <typename... Args>
  explicit TypedMessage(std::in_place_t, Args&&... args)
      : Message(TypeId::Of<T>()), value_(std::forward<Args>(args)...) {}

  const T& value() const noexcept { return value_; }

  /// Mutable access; only legitimate for the sole owner of the message.
  T& mutable_value() noexcept { return value_; }

 private:
  T value_;
};

template <typename T>
const std::remove_cvref_t<T>* Message::As() const noexcept {
  using U = std::remove_cvref_t<T>;
  return IsType<U>() ? &static_cast<const TypedMessage<U>*>(this)->value() : nullptr;
}

/// Wraps `value` into a new message.
template <typename T>
MessagePtr MakeMessage(T&& value) {
  using U = std::remove_cvref_t<T>;
  return std::make_shared<TypedMessage<U>>(std::in_place, std::forward<T>(value));
}

/// Constructs a T payload in place from `args`.
template <typename T, typename... Args>
MessagePtr EmplaceMessage(Args&&... args) {
  return std::make_shared<TypedMessage<T>>(std::in_place, std::forward<Args>(args)...);
}

}  // namespace mloom
