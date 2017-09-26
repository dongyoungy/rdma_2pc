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

#include "Poco/Mutex.h"
#include "Poco/Random.h"
#include "Poco/Runnable.h"
#include "Poco/Timestamp.h"

#include "constants.h"
#include "lock_manager.h"
#include "lock_request.h"
#include "think_time_generator.h"

using namespace std;

namespace rdma {
namespace proto {

class LockManager;

// Default LockSimulator.
class LockSimulator : public Poco::Runnable {
 public:
  LockSimulator(LockManager* manager, int num_nodes, int num_objects,
                int request_size, string think_time_type,
                bool run_random_backoff);
  ~LockSimulator();
  virtual void run();  // for Poco::Runnable

  void Stop();
  uint64_t GetCount() const;
  uint64_t GetCountWithContention() const;
  uint64_t GetCountWithBackoff() const;

  void SortLatency();
  double GetAverageLatency() const;
  double Get99PercentileLatency() const;
  double Get999PercentileLatency() const;
  uint64_t GetMaxLatency() const;

  double GetAverageLatencyWithContention() const;
  double Get99PercentileLatencyWithContention() const;
  double Get999PercentileLatencyWithContention() const;

  double GetAverageLatencyWithBackoff() const;
  double Get99PercentileLatencyWithBackoff() const;
  double Get999PercentileLatencyWithBackoff() const;

  double GetAverageBackoffTime() const;

 protected:
  virtual void CreateRequest();
  void RevertLocks(int& index);
  int PerformRandomBackoff(int& attempt);

  Poco::Random rng_;
  Poco::Timestamp last_lock_start_time_;
  Poco::Timestamp last_lock_try_time_;
  LockManager* manager_;
  std::vector<uint64_t> latency_;
  std::vector<uint64_t> contention_latency_;
  std::vector<uint64_t> backoff_latency_;
  std::vector<uint64_t> backoff_time_;
  std::vector<std::unique_ptr<LockRequest>> requests_;
  int num_nodes_;
  int num_objects_;
  int request_size_;
  int max_request_size_;
  string think_time_type_;
  bool do_random_backoff_;
  uint64_t count_;
  uint64_t backoff_count_;
  bool is_done_;
  int seq_count_;

  Poco::Mutex mutex_;
};

}  // namespace proto
}  // namespace rdma

#endif
