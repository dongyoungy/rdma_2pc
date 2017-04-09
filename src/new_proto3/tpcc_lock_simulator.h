#ifndef RDMA_PROTO_TPCCLOCKSIMULATOR_H
#define RDMA_PROTO_TPCCLOCKSIMULATOR_H

#include "lock_simulator.h"
#include "tpcc_lock_gen.h"
#include "constants.h"

using namespace std;

namespace rdma { namespace proto {

class TPCCLockSimulator : public LockSimulator {
  public:
    TPCCLockSimulator(LockManager* manager, uint32_t id, uint32_t home_id, int workload_type,
        int num_manager, int num_tx, long seed, bool verbose, bool measure_lock_time, int lock_mode,
        bool transaction_delay = false, double transaction_delay_min = 10,
        double transaction_delay_max = 100, int min_backoff_time = 1000,
        int max_backoff_time = 100000, int sleep_time = 10000, int think_time = 10);

    virtual void Run();
  protected:
    virtual void StartLockRequests();
    virtual void CreateLockRequests();
    virtual void SubmitLockRequest();
    virtual void SubmitUnlockRequest();
    TPCCLockGen* tpcc_lock_gen_;
  private:
    uint32_t home_id_;
};

}}

#endif

