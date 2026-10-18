#include "mloom/core/codec.hpp"

#include <gtest/gtest.h>

#include <limits>
#include <string>

#include "mloom/core/byte_io.hpp"

namespace mloom_test {
struct Sample {
  double value = 0.0;
  std::int32_t channel = 0;
};
struct Label {
  std::string text;
  std::uint16_t id = 0;
};
}  // namespace mloom_test

namespace mloom {
namespace {

using mloom_test::Label;
using mloom_test::Sample;

Status RegisterLabel(CodecRegistry& registry) {
  return registry.Register<Label>(
      [](const Label& label, ByteWriter& writer) {
        writer.WriteString(label.text);
        writer.Write(label.id);
      },
      [](ByteReader& reader) {
        Label label;
        reader.ReadString(label.text);
        reader.Read(label.id);
        return std::pair{std::move(label), Status::Ok()};
      });
}

TEST(ByteIoTest, RoundTripsScalarsAndStrings) {
  enum class Color : std::uint8_t { kRed = 7 };
  Bytes buffer;
  ByteWriter writer(buffer);
  writer.Write(std::uint64_t{0x0102030405060708});
  writer.Write(std::int16_t{-2});
  writer.Write(3.5);
  writer.Write(true);
  writer.Write(Color::kRed);
  writer.WriteString("loom");
  EXPECT_EQ(buffer[0], std::byte{0x08});  // little-endian on every host

  ByteReader reader(buffer);
  std::uint64_t u = 0;
  std::int16_t i = 0;
  double d = 0;
  bool b = false;
  Color c{};
  std::string s;
  EXPECT_TRUE(reader.Read(u) && reader.Read(i) && reader.Read(d) && reader.Read(b) &&
              reader.Read(c) && reader.ReadString(s));
  EXPECT_EQ(u, 0x0102030405060708u);
  EXPECT_EQ(i, -2);
  EXPECT_EQ(d, 3.5);
  EXPECT_TRUE(b);
  EXPECT_EQ(c, Color::kRed);
  EXPECT_EQ(s, "loom");
  EXPECT_TRUE(reader.empty());
}

TEST(ByteIoTest, ReaderFailsOnUnderflowAndStaysFailed) {
  Bytes buffer;
  ByteWriter writer(buffer);
  writer.Write(std::uint32_t{1000});  // claims a 1000 byte string
  ByteReader reader(buffer);
  std::string text;
  EXPECT_FALSE(reader.ReadString(text));
  EXPECT_FALSE(reader.ok());
  std::uint8_t byte = 0;
  EXPECT_FALSE(reader.Read(byte));
}

TEST(CodecRegistryTest, RoundTripsCustomCodec) {
  CodecRegistry registry;
  ASSERT_TRUE(RegisterLabel(registry).ok());
  EXPECT_TRUE(registry.Contains<Label>());

  Bytes frame;
  ASSERT_TRUE(registry.Encode(*MakeMessage(Label{"camera", 42}), frame).ok());
  auto [message, status] = registry.Decode(frame);
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_TRUE(message->IsType<Label>());
  EXPECT_EQ(message->As<Label>()->text, "camera");
  EXPECT_EQ(message->As<Label>()->id, 42);
}

TEST(CodecRegistryTest, RoundTripsTrivialCodec) {
  CodecRegistry registry;
  ASSERT_TRUE(registry.RegisterTrivial<Sample>().ok());
  Bytes frame;
  ASSERT_TRUE(registry.Encode(*MakeMessage(Sample{0.25, -3}), frame).ok());
  auto [message, status] = registry.Decode(frame);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(message->As<Sample>()->value, 0.25);
  EXPECT_EQ(message->As<Sample>()->channel, -3);
}

TEST(CodecRegistryTest, ReportsMissingCodec) {
  CodecRegistry registry;
  Bytes frame;
  EXPECT_EQ(registry.Encode(*MakeMessage(Sample{}), frame).code(), StatusCode::kUnsupported);

  CodecRegistry sender;
  ASSERT_TRUE(sender.RegisterTrivial<Sample>().ok());
  ASSERT_TRUE(sender.Encode(*MakeMessage(Sample{}), frame).ok());
  EXPECT_EQ(registry.Decode(frame).second.code(), StatusCode::kUnsupported);
}

TEST(CodecRegistryTest, RejectsMalformedFrames) {
  CodecRegistry registry;
  ASSERT_TRUE(RegisterLabel(registry).ok());
  Bytes frame;
  ASSERT_TRUE(registry.Encode(*MakeMessage(Label{"abc", 1}), frame).ok());

  Bytes truncated(frame.begin(), frame.end() - 1);
  EXPECT_EQ(registry.Decode(truncated).second.code(), StatusCode::kInvalidArgument);

  Bytes trailing = frame;
  trailing.push_back(std::byte{0});
  EXPECT_EQ(registry.Decode(trailing).second.code(), StatusCode::kInvalidArgument);

  Bytes wrong_version = frame;
  wrong_version[0] = std::byte{99};
  EXPECT_EQ(registry.Decode(wrong_version).second.code(), StatusCode::kUnsupported);

  EXPECT_EQ(registry.Decode(Bytes{std::byte{1}}).second.code(), StatusCode::kInvalidArgument);
}

TEST(CodecRegistryTest, ReRegistrationReplacesCodec) {
  CodecRegistry registry;
  ASSERT_TRUE(RegisterLabel(registry).ok());
  ASSERT_TRUE(RegisterLabel(registry).ok());
  EXPECT_TRUE(registry.Contains<Label>());
}

TEST(CodecRegistryTest, DecoderErrorsPropagate) {
  CodecRegistry registry;
  ASSERT_TRUE(registry
                  .Register<Label>([](const Label&, ByteWriter&) {},
                                   [](ByteReader&) {
                                     return std::pair{Label{}, Status::InvalidArgument("bad id")};
                                   })
                  .ok());
  Bytes frame;
  ASSERT_TRUE(registry.Encode(*MakeMessage(Label{}), frame).ok());
  EXPECT_EQ(registry.Decode(frame).second, Status::InvalidArgument("bad id"));
}

}  // namespace
}  // namespace mloom
