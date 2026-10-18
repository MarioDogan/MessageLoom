#pragma once

// Test support: integrations of stateless acme functions into logic handlers, shared by the
// handler-level and the node-level third-party tests. Each lambda adapts one acme calling and
// error convention to LogicResult<Out>(const In&) or (In&&).

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "mloom/core/status.hpp"
#include "mloom/node/logic_handler.hpp"
#include "mloom/node/testing/third_party/acme_sensors.hpp"

namespace mloom::testing {

/// Payload bytes of an acme::Frame.
using Payload = std::vector<std::uint8_t>;

inline acme::Reading MakeReading(std::string sensor, double celsius) {
  return acme::Reading{std::move(sensor), celsius};
}

/// A plain function: wrap its value into an Ok result.
inline auto FahrenheitHandler() {
  return MakeLogicHandler([](const double& celsius) {
    return std::pair{acme::CelsiusToFahrenheit(celsius), Status::Ok()};
  });
}

/// bool result plus out parameter: false becomes InvalidArgument.
inline auto ParseHandler() {
  return MakeLogicHandler([](const std::string& line) {
    acme::Reading reading;
    if (!acme::ParseReading(line, &reading)) {
      return std::pair{acme::Reading{}, Status::InvalidArgument("cannot parse '" + line + "'")};
    }
    return std::pair{std::move(reading), Status::Ok()};
  });
}

/// std::optional result: no value means "accepted, nothing to publish", i.e. NeedMoreData. The
/// limits are configuration, captured by value.
inline auto LimitsHandler(double warning, double critical) {
  return MakeLogicHandler([warning, critical](const acme::Reading& reading) {
    std::optional<acme::Alarm> alarm = acme::CheckLimits(reading, warning, critical);
    if (!alarm) return std::pair{acme::Alarm{}, Status::NeedMoreData()};
    return std::pair{std::move(*alarm), Status::Ok()};
  });
}

/// Exceptions must not escape into the node loop, so they become statuses.
inline auto DecodeHandler() {
  return MakeLogicHandler([](const acme::Frame& frame) {
    try {
      return std::pair{acme::DecodeFrame(frame), Status::Ok()};
    } catch (const acme::DecodeError& error) {
      return std::pair{Payload{}, Status::InvalidArgument(error.what())};
    }
  });
}

/// An API that takes its argument over: consume the payload instead of borrowing it.
inline auto InvertHandler() {
  return MakeLogicHandler([](acme::Frame&& frame) {
    return std::pair{acme::InvertFrame(std::move(frame)), Status::Ok()};
  });
}

}  // namespace mloom::testing
