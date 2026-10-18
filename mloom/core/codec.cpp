#include "mloom/core/codec.hpp"

#include <charconv>
#include <mutex>

namespace mloom {
namespace {

std::string ToHex(std::uint64_t value) {
  char buffer[2 + 16] = {'0', 'x'};
  const auto result = std::to_chars(buffer + 2, buffer + sizeof(buffer), value, 16);
  return std::string(buffer, result.ptr);
}

}  // namespace

Status CodecRegistry::Insert(Codec codec) {
  std::unique_lock lock(mutex_);
  const auto it = codecs_.find(codec.type.hash());
  if (it != codecs_.end() && it->second->type.info() != codec.type.info()) {
    return Status::InvalidArgument("type hash collision between '" +
                                   std::string(it->second->type.name()) + "' and '" +
                                   std::string(codec.type.name()) + "'");
  }
  codecs_[codec.type.hash()] = std::make_shared<const Codec>(std::move(codec));
  return Status::Ok();
}

std::shared_ptr<const Codec> CodecRegistry::Find(std::uint64_t type_hash) const {
  std::shared_lock lock(mutex_);
  const auto it = codecs_.find(type_hash);
  return it == codecs_.end() ? nullptr : it->second;
}

Status CodecRegistry::Encode(const Message& message, Bytes& frame) const {
  const auto codec = Find(message.type().hash());
  if (!codec) {
    return Status::Unsupported("no codec registered for '" + std::string(message.type().name()) +
                               "'");
  }
  frame.clear();
  ByteWriter writer(frame);
  writer.Write(kFrameVersion);
  writer.Write(message.type().hash());
  return codec->encode(message, writer);
}

std::pair<MessagePtr, Status> CodecRegistry::Decode(ByteSpan frame) const {
  ByteReader reader(frame);
  std::uint8_t version = 0;
  std::uint64_t type_hash = 0;
  if (!reader.Read(version) || !reader.Read(type_hash)) {
    return {nullptr, Status::InvalidArgument("frame shorter than its header")};
  }
  if (version != kFrameVersion) {
    return {nullptr,
            Status::Unsupported("unsupported frame version " + std::to_string(int{version}))};
  }
  const auto codec = Find(type_hash);
  if (!codec) {
    return {nullptr, Status::Unsupported("no codec registered for type hash " + ToHex(type_hash))};
  }
  auto [message, status] = codec->decode(reader);
  if (!status.ok()) return {nullptr, std::move(status)};
  if (!reader.empty()) {
    return {nullptr, Status::InvalidArgument("trailing bytes after '" +
                                             std::string(codec->type.name()) + "' payload")};
  }
  return {std::move(message), Status::Ok()};
}

}  // namespace mloom
