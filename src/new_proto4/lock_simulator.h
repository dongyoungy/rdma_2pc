#ifndef RDMA_PROTO_LOCKSIMULATOR_H
#define RDMA_PROTO_LOCKSIMULATOR_H

#include <pthread.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <future>
#include <iostream>
#include <thread>
#include <vector>

#include "Poco/Random.h"
#include "Poco/Runnable.h"
#include "Poco/Timestamp.h"

#include "constants.h"
#include "lock_manager.h"
#include "lock_request.h"

const uint64_t kTransactionMax = 100000000;

using namespace std;

namespace rdma {
namespace proto {

class LockManager;

class LockSimulator : public Poco::Runnable {
 public:
  LockSimulator(LockManager* manager, int num_nodes, int num_objects,
                int request_size);
  ~LockSimulator();
  virtual void run();  // for Poco::Runnable

  void Stop();
  uint64_t GetCount() const;

  void SortLatency();
  double GetAverageLatency() const;
  double Get99PercentileLatency() const;

 protected:
  virtual void CreateRequest();

  Poco::Random rng_;
  LockManager* manager_;
  std::vector<uint64_t> latency_;
  std::vector<std::unique_ptr<LockRequest>> requests_;
  int num_nodes_;
  int num_objects_;
  int request_size_;
  uint64_t count_;
  volatile bool is_done_;
};

}  // namespace proto
}  // namespace rdma

#endif
