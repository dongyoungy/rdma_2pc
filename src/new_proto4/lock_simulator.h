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
  LockSimulator(LockManager* manager, int id, int num_nodes, int num_objects,
                int request_size, string think_time_type,
                bool run_random_backoff);
  ~LockSimulator();
  virtual void run();  // for Poco::Runnable

  int GetID() const;
  void Stop();
  void SetThinkTimeDuration(int duration);
  uint64_t GetCount() const;
  uint64_t GetLockCount() const;
  uint64_t GetBackoffCount() const;
  uint64_t GetCountWithContention() const;
  uint64_t GetCountWithBackoff() const;

  void SortLatency();
  double GetAverageLatency() const;
  double Get99PercentileLatency() const;
  double Get999PercentileLatency() const;
  uint64_t GetMaxLatency() const;
  uint64_t GetFalsePositives() const;

  double GetAverageLatencyWithContention() const;
  double Get99PercentileLatencyWithContention() const;
  double Get999PercentileLatencyWithContention() const;

  double GetAverageLatencyWithBackoff() const;
  double Get99PercentileLatencyWithBackoff() const;
  double Get999PercentileLatencyWithBackoff() const;

  double GetAverageBackoffTime() const;

  double GetAverageContentionCount() const;
  double GetAverageContentionCount2() const;
  double GetAverageContentionCount3() const;
  double GetAverageContentionCount4() const;
  double GetAverageContentionCount5() const;
  double GetAverageContentionCount6() const;

  static void SetBaseBackoff(double backoff);
  static void SetMaxBackoff(double backoff);
  static void SetReadLockTime(long time);

 protected:
  static const int LEASE_EXTENSION_TIME = 100;
  static double kMaxBackoff;
  static double kBaseBackoff;
  static long kReadLockTime;
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
  std::vector<LockStat> stats_;
  std::vector<std::unique_ptr<LockRequest>> requests_;
  LockRequest** temp_requests_;
  int id_;
  int num_nodes_;
  int num_objects_;
  int request_size_;
  int max_request_size_;
  string think_time_type_;
  bool do_random_backoff_;
  uint64_t trx_count_;
  uint64_t backoff_count_;
  bool is_done_;
  int seq_count_;
  int think_time_duration_;
  uint64_t false_positives_;
  uint64_t lock_count_;

  double d2lm_fail_rate_;

  Poco::Mutex mutex_;
};

}  // namespace proto
}  // namespace rdma

#endif
