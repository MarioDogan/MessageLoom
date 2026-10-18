#include "mloom/core/message.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "mloom/core/status.hpp"
#include "mloom/core/type_id.hpp"

namespace mloom_test {
struct Point {
  int x = 0;
  int y = 0;
};
struct Other {
  int x = 0;
};
}  // namespace mloom_test

namespace mloom {
namespace {

using mloom_test::Other;
using mloom_test::Point;

static_assert(TypeName<int>() == "int");
static_assert(TypeName<Point>() == "mloom_test::Point");
static_assert(TypeName<const Point&>() == "mloom_test::Point");
static_assert(TypeHash<Point>() == TypeHash<const Point&>());
static_assert(TypeHash<Point>() != TypeHash<Other>());

TEST(TypeIdTest, ComparesHashAndTypeid) {
  EXPECT_EQ(TypeId::Of<Point>(), TypeId::Of<const Point>());
  EXPECT_FALSE(TypeId::Of<Point>() == TypeId::Of<Other>());
  EXPECT_EQ(TypeId::Of<std::vector<int>>().name(), TypeName<std::vector<int>>());
  EXPECT_EQ(TypeId::Of<Point>().info(), typeid(Point));
}

TEST(MessageTest, IsTypeRecoversPayloadType) {
  MessagePtr message = MakeMessage(Point{1, 2});
  EXPECT_TRUE(message->IsType<Point>());
  EXPECT_TRUE(message->IsType<const Point&>());
  EXPECT_FALSE(message->IsType<Other>());
  EXPECT_FALSE(message->IsType<int>());
  EXPECT_EQ(message->type().name(), "mloom_test::Point");
}

TEST(MessageTest, AsReturnsPayloadOrNull) {
  MessagePtr message = MakeMessage(Point{3, 4});
  const Point* point = message->As<Point>();
  ASSERT_NE(point, nullptr);
  EXPECT_EQ(point->x, 3);
  EXPECT_EQ(point->y, 4);
  EXPECT_EQ(message->As<Other>(), nullptr);
}

TEST(MessageTest, EmplaceConstructsInPlace) {
  MessagePtr message = EmplaceMessage<std::string>(3, 'a');
  ASSERT_TRUE(message->IsType<std::string>());
  EXPECT_EQ(*message->As<std::string>(), "aaa");
}

TEST(MessageTest, MakeMessageMovesFromRvalues) {
  std::string text(64, 'x');
  MessagePtr message = MakeMessage(std::move(text));
  EXPECT_EQ(message->As<std::string>()->size(), 64u);
}

TEST(StatusTest, DefaultIsOk) {
  Status status;
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status, Status::Ok());
  EXPECT_EQ(status.ToString(), "Ok");
}

TEST(StatusTest, CarriesCodeAndMessage) {
  Status status = Status::NeedMoreData("waiting for second frame");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), StatusCode::kNeedMoreData);
  EXPECT_EQ(status.message(), "waiting for second frame");
  EXPECT_EQ(status.ToString(), "NeedMoreData: waiting for second frame");
  EXPECT_EQ(Status::Error("x"), Status(StatusCode::kError, "x"));
  EXPECT_NE(Status::Error("x"), Status::Error("y"));
}

}  // namespace
}  // namespace mloom
