#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace mloom {

using Bytes = std::vector<std::byte>;
using ByteSpan = std::span<const std::byte>;

namespace detail {

template <std::size_t N>
struct UnsignedOfSize;
template <>
struct UnsignedOfSize<1> {
  using type = std::uint8_t;
};
template <>
struct UnsignedOfSize<2> {
  using type = std::uint16_t;
};
template <>
struct UnsignedOfSize<4> {
  using type = std::uint32_t;
};
template <>
struct UnsignedOfSize<8> {
  using type = std::uint64_t;
};

template <typename T>
concept WireScalar = std::is_arithmetic_v<T> || std::is_enum_v<T>;

}  // namespace detail

/// Appends values to a byte buffer. Scalars are written little-endian regardless of the host, so
/// frames written with ByteWriter can be read on any machine.
class ByteWriter {
 public:
  explicit ByteWriter(Bytes& out) noexcept : out_(out) {}

  template <detail::WireScalar T>
  void Write(T value) {
    using U = typename detail::UnsignedOfSize<sizeof(T)>::type;
    const U bits = std::bit_cast<U>(value);
    for (std::size_t i = 0; i < sizeof(T); ++i) {
      out_.push_back(static_cast<std::byte>(bits >> (8 * i)));
    }
  }

  /// Length-prefixed (u32) string.
  void WriteString(std::string_view text) {
    Write(static_cast<std::uint32_t>(text.size()));
    WriteRaw(text.data(), text.size());
  }

  /// Length-prefixed (u32) byte block.
  void WriteBytes(ByteSpan bytes) {
    Write(static_cast<std::uint32_t>(bytes.size()));
    WriteRaw(bytes.data(), bytes.size());
  }

  /// Raw bytes without a length prefix.
  void WriteRaw(const void* data, std::size_t size) {
    const auto* begin = static_cast<const std::byte*>(data);
    out_.insert(out_.end(), begin, begin + size);
  }

 private:
  Bytes& out_;
};

/// Reads values written by ByteWriter. Every read is bounds checked; after the first failure the
/// reader stays failed and ok() returns false.
class ByteReader {
 public:
  explicit ByteReader(ByteSpan data) noexcept : data_(data) {}

  template <detail::WireScalar T>
  bool Read(T& value) {
    using U = typename detail::UnsignedOfSize<sizeof(T)>::type;
    if (!Require(sizeof(T))) return false;
    U bits = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
      bits = static_cast<U>(bits | static_cast<U>(std::to_integer<U>(data_[pos_ + i]) << (8 * i)));
    }
    value = std::bit_cast<T>(bits);
    pos_ += sizeof(T);
    return true;
  }

  bool ReadString(std::string& text) {
    std::uint32_t size = 0;
    if (!Read(size) || !Require(size)) return false;
    text.assign(reinterpret_cast<const char*>(data_.data() + pos_), size);
    pos_ += size;
    return true;
  }

  bool ReadBytes(Bytes& bytes) {
    std::uint32_t size = 0;
    if (!Read(size) || !Require(size)) return false;
    bytes.assign(data_.begin() + static_cast<std::ptrdiff_t>(pos_),
                 data_.begin() + static_cast<std::ptrdiff_t>(pos_ + size));
    pos_ += size;
    return true;
  }

  bool ReadRaw(void* data, std::size_t size) {
    if (!Require(size)) return false;
    std::memcpy(data, data_.data() + pos_, size);
    pos_ += size;
    return true;
  }

  bool ok() const noexcept { return ok_; }
  bool empty() const noexcept { return pos_ == data_.size(); }
  ByteSpan remaining() const noexcept { return data_.subspan(pos_); }

 private:
  bool Require(std::size_t size) noexcept {
    if (!ok_ || data_.size() - pos_ < size) {
      ok_ = false;
      return false;
    }
    return true;
  }

  ByteSpan data_;
  std::size_t pos_ = 0;
  bool ok_ = true;
};

}  // namespace mloom
