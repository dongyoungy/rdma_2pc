#include "think_time_generator.h"

namespace rdma {
namespace proto {

ThinkTimeGenerator::ThinkTimeGenerator(std::string type, int duration) {
  if (type == "zero") {
    type_ = ZERO;
    duration_ = 0;
  } else if (type == "simple") {
    type_ = SIMPLE;
    duration_ = duration;
  } else if (type == "normal") {
    type_ = NORMAL;
    normal_dist_ = std::normal_distribution<float>(100.0f, 50.0f);
  } else {
    type_ = UNKNOWN;
  }
  default_rng_.seed(
      std::chrono::system_clock::now().time_since_epoch().count());
}

int ThinkTimeGenerator::GetTime() {
  switch (type_) {
    case ZERO:
    case SIMPLE: {
      return duration_;
      break;
    }
    case NORMAL: {
      int time = normal_dist_(default_rng_);
      return (time > 0) ? time : 0;
      break;
    }
    case UNKNOWN:
    default: {
      return -1;
      break;
    }
  }
}

}  // namespace proto
}  // namespace rdma
