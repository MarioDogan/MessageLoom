#include "mloom/transport/endpoint.hpp"

#include <gtest/gtest.h>

namespace mloom {
namespace {

TEST(EndpointTest, ParsesSingleAddress) {
  auto [endpoints, status] = ParseEndpoints("inproc://camera");
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_EQ(endpoints.size(), 1u);
  EXPECT_EQ(endpoints[0].mode, EndpointMode::kDefault);
  EXPECT_EQ(endpoints[0].transport, Transport::kInproc);
  EXPECT_EQ(endpoints[0].address, "inproc://camera");
  EXPECT_TRUE(endpoints[0].is_local());
}

TEST(EndpointTest, ParsesModesAndLists) {
  auto [endpoints, status] = ParseEndpoints(" @tcp://*:5555 , >ipc:///tmp/loom,inproc://x");
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_EQ(endpoints.size(), 3u);
  EXPECT_EQ(endpoints[0].mode, EndpointMode::kBind);
  EXPECT_EQ(endpoints[0].transport, Transport::kTcp);
  EXPECT_EQ(endpoints[0].address, "tcp://*:5555");
  EXPECT_FALSE(endpoints[0].is_local());
  EXPECT_EQ(endpoints[1].mode, EndpointMode::kConnect);
  EXPECT_EQ(endpoints[1].transport, Transport::kIpc);
  EXPECT_EQ(endpoints[1].address, "ipc:///tmp/loom");
  EXPECT_EQ(endpoints[2].ToString(), "inproc://x");
  EXPECT_EQ(endpoints[0].ToString(), "@tcp://*:5555");
}

TEST(EndpointTest, ResolvesDefaultMode) {
  Endpoint endpoint;
  EXPECT_EQ(endpoint.ResolvedMode(EndpointMode::kBind), EndpointMode::kBind);
  endpoint.mode = EndpointMode::kConnect;
  EXPECT_EQ(endpoint.ResolvedMode(EndpointMode::kBind), EndpointMode::kConnect);
}

TEST(EndpointTest, RejectsMalformedSpecs) {
  for (const char* spec :
       {"", "  ", "camera", "://x", "foo://bar", "tcp://", "inproc://a,,b", "inproc://a,"}) {
    EXPECT_EQ(ParseEndpoints(spec).second.code(), StatusCode::kInvalidArgument) << spec;
  }
}

}  // namespace
}  // namespace mloom
