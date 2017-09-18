#ifndef RDMA_PROTO_THINK_TIME_GENERATOR_H
#define RDMA_PROTO_THINK_TIME_GENERATOR_H

#include <chrono>
#include <random>
#include <string>

#include "constants.h"

namespace rdma {
namespace proto {

class ThinkTimeGenerator {
 public:
  ThinkTimeGenerator(std::string type);
  int GetTime();

 private:
  ThinkTimeType type_;
  std::default_random_engine default_rng_;
  std::normal_distribution<float> normal_dist_;
};

}  // namespace proto
}  // namespace rdma

#endif
