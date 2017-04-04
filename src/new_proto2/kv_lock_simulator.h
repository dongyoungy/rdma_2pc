#ifndef RDMA_PROTO_KVLOCKSIMULATOR_H
#define RDMA_PROTO_KVLOCKSIMULATOR_H

#include <cmath>
#include <algorithm>

#include "lock_simulator.h"
#include "constants.h"

using namespace std;

namespace rdma { namespace proto {

class KVLockSimulator : public LockSimulator {
  public:
    KVLockSimulator(LockManager* manager, uint32_t id, uint32_t home_id, int workload_type,
        int num_manager, int num_tx, int num_objects, double update_ratio,
        double alpha, long seed, bool verbose,
        bool measure_lock_time, int lock_mode,
        bool transaction_delay = false, double transaction_delay_min = 10,
        double transaction_delay_max = 100, int min_backoff_time = 1000,
        int max_backoff_time = 100000, int sleep_time = 10000, int think_time = 0);

    virtual void Run();
  protected:
    virtual void StartLockRequests();
    virtual void CreateLockRequests();
    virtual void SubmitLockRequest();
    virtual void SubmitUnlockRequest();
  private:
    uint64_t getZipfRand(double p, double s, double N);
    void Generate();
    uint32_t home_id_;
    double update_ratio_;
    double alpha_;
    double N_;
    int num_objects_;
};

}}

#endif

