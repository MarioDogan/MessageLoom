#pragma once

#include <chrono>

namespace mloom {

/// Duration of a blocking wait. Zero polls without blocking, a negative value waits forever.
using Timeout = std::chrono::milliseconds;

inline constexpr Timeout kNoWait{0};
inline constexpr Timeout kWaitForever{-1};

}  // namespace mloom
