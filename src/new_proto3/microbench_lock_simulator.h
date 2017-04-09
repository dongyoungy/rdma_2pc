#ifndef RDMA_PROTO_MICROBENCHLOCKSIMULATOR_H
#define RDMA_PROTO_MICROBENCHLOCKSIMULATOR_H

#include <set>

#include "lock_simulator.h"
#include "constants.h"

using namespace std;

namespace rdma { namespace proto {

class MicrobenchLockSimulator : public LockSimulator {
  public:
    MicrobenchLockSimulator(LockManager* manager, uint32_t id, uint32_t home_id, int workload_type,
        int num_manager, int num_tx, int num_objects,
        double contention_index, long seed, bool verbose,
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
    void Generate();
    uint32_t home_id_;
    double contention_index_;
    int num_objects_;
    int num_hot_objects_;
    set<int> obj_index_set_;
};

}}

#endif

