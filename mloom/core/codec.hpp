#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "mloom/core/byte_io.hpp"
#include "mloom/core/message.hpp"
#include "mloom/core/status.hpp"
#include "mloom/core/type_id.hpp"

namespace mloom {

/// Serializes messages of one payload type. Created through CodecRegistry::Register.
struct Codec {
  TypeId type;
  std::function<Status(const Message& message, ByteWriter& writer)> encode;
  std::function<std::pair<MessagePtr, Status>(ByteReader& reader)> decode;
};

/// Maps type hashes to codecs, so messages can leave the process (tcp, ipc).
///
/// Frame layout: [u8 frame version][u64 type hash][payload]. Both peers must register the type.
/// Type hashes derive from compiler type names, so communicating processes should be built with
/// the same toolchain. Thread-safe.
class CodecRegistry {
 public:
  static constexpr std::uint8_t kFrameVersion = 1;

  /// Registers T with `encode: void(const T&, ByteWriter&)` and
  /// `decode: std::pair<T, Status>(ByteReader&)`. Registering T again replaces its codec.
  template <typename T, typename EncodeFn, typename DecodeFn>
  Status Register(EncodeFn encode, DecodeFn decode);

  /// Registers a trivially copyable T as raw bytes. Only for peers with the same ABI/endianness.
  template <typename T>
  Status RegisterTrivial();

  std::shared_ptr<const Codec> Find(std::uint64_t type_hash) const;

  template <typename T>
  bool Contains() const {
    return Find(TypeHash<T>()) != nullptr;
  }

  /// Serializes `message` into a self-describing frame, replacing the contents of `frame`.
  Status Encode(const Message& message, Bytes& frame) const;

  /// Rebuilds a message from a frame produced by Encode.
  std::pair<MessagePtr, Status> Decode(ByteSpan frame) const;

 private:
  Status Insert(Codec codec);

  mutable std::shared_mutex mutex_;
  std::unordered_map<std::uint64_t, std::shared_ptr<const Codec>> codecs_;
};

template <typename T, typename EncodeFn, typename DecodeFn>
Status CodecRegistry::Register(EncodeFn encode, DecodeFn decode) {
  static_assert(std::is_same_v<T, std::remove_cvref_t<T>>, "register the plain payload type");
  static_assert(std::is_invocable_v<EncodeFn&, const T&, ByteWriter&>,
                "encode must be callable as void(const T&, ByteWriter&)");
  static_assert(std::is_invocable_r_v<std::pair<T, Status>, DecodeFn&, ByteReader&>,
                "decode must be callable as std::pair<T, Status>(ByteReader&)");
  Codec codec{
      TypeId::Of<T>(),
      [encode = std::move(encode)](const Message& message, ByteWriter& writer) mutable -> Status {
        const T* value = message.As<T>();
        if (value == nullptr) {
          return Status::TypeMismatch("codec for '" + std::string(TypeName<T>()) +
                                      "' cannot encode '" + std::string(message.type().name()) +
                                      "'");
        }
        encode(*value, writer);
        return Status::Ok();
      },
      [decode = std::move(decode)](ByteReader& reader) mutable -> std::pair<MessagePtr, Status> {
        std::pair<T, Status> decoded = decode(reader);
        if (!decoded.second.ok()) return {nullptr, std::move(decoded.second)};
        if (!reader.ok()) {
          return {nullptr, Status::InvalidArgument("truncated '" + std::string(TypeName<T>()) +
                                                   "' payload")};
        }
        return {MakeMessage(std::move(decoded.first)), Status::Ok()};
      }};
  return Insert(std::move(codec));
}

template <typename T>
Status CodecRegistry::RegisterTrivial() {
  static_assert(std::is_trivially_copyable_v<T> && std::is_default_constructible_v<T>,
                "RegisterTrivial needs a trivially copyable, default constructible type");
  return Register<T>([](const T& value, ByteWriter& writer) { writer.WriteRaw(&value, sizeof(T)); },
                     [](ByteReader& reader) {
                       T value{};
                       reader.ReadRaw(&value, sizeof(T));
                       return std::pair<T, Status>(value, Status::Ok());
                     });
}

}  // namespace mloom
