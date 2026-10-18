#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <typeinfo>

namespace mloom {
namespace detail {

template <typename T>
constexpr const char* RawTypeSignature() noexcept {
#if defined(__clang__) || defined(__GNUC__)
  return __PRETTY_FUNCTION__;
#else
#error "MessageLoom derives type names from __PRETTY_FUNCTION__ and needs GCC or Clang"
#endif
}

/// Extracts the type from "... [with T = <type>]" (GCC) or "... [T = <type>]" (Clang).
constexpr std::string_view ExtractTypeName(std::string_view signature) noexcept {
  constexpr std::string_view kMarker = "T = ";
  const std::size_t begin = signature.find(kMarker);
  const std::size_t end = signature.rfind(']');
  if (begin == std::string_view::npos || end == std::string_view::npos || end < begin) {
    return signature;
  }
  return signature.substr(begin + kMarker.size(), end - begin - kMarker.size());
}

constexpr std::uint64_t Fnv1a64(std::string_view text) noexcept {
  std::uint64_t hash = 0xcbf29ce484222325ULL;
  for (const char c : text) {
    hash ^= static_cast<std::uint8_t>(c);
    hash *= 0x100000001b3ULL;
  }
  return hash;
}

}  // namespace detail

/// Compiler-provided name of T without cv/ref qualifiers, e.g. "mloom::ControlCommand".
/// It is stable for a given toolchain, which makes it usable to identify types across processes.
template <typename T>
constexpr std::string_view TypeName() noexcept {
  return detail::ExtractTypeName(detail::RawTypeSignature<std::remove_cvref_t<T>>());
}

/// 64-bit FNV-1a hash of TypeName<T>(). This is the type identity that travels on the wire.
template <typename T>
inline constexpr std::uint64_t kTypeHash = detail::Fnv1a64(TypeName<T>());

template <typename T>
constexpr std::uint64_t TypeHash() noexcept {
  return kTypeHash<std::remove_cvref_t<T>>;
}

/// Runtime identity of a payload type: the stable name hash plus the in-process typeid.
class TypeId {
 public:
  template <typename T>
  static TypeId Of() noexcept {
    using U = std::remove_cvref_t<T>;
    return TypeId(TypeHash<U>(), TypeName<U>(), typeid(U));
  }

  std::uint64_t hash() const noexcept { return hash_; }
  std::string_view name() const noexcept { return name_; }
  const std::type_info& info() const noexcept { return *info_; }

  friend bool operator==(const TypeId& a, const TypeId& b) noexcept {
    return a.hash_ == b.hash_ && *a.info_ == *b.info_;
  }

 private:
  TypeId(std::uint64_t hash, std::string_view name, const std::type_info& info) noexcept
      : hash_(hash), name_(name), info_(&info) {}

  std::uint64_t hash_;
  std::string_view name_;
  const std::type_info* info_;
};

}  // namespace mloom
