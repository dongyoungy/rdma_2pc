#ifndef RDMA_PROTO_TPCCLOCKSIMULATOR_H
#define RDMA_PROTO_TPCCLOCKSIMULATOR_H

#include "lock_simulator.h"
#include "tpcc_lock_gen.h"

using namespace std;

namespace rdma { namespace proto {

class TPCCLockSimulator : public LockSimulator {
  public:
    TPCCLockSimulator(LockManager* manager, int id, int num_manager,
        int num_tx, long seed, bool verbose, bool measure_lock_time, int lock_mode,
        bool transaction_delay = false, double transaction_delay_min = 10,
        double transaction_delay_max = 100, int min_backoff_time = 1000,
        int max_backoff_time = 100000);

    virtual void Run();
  protected:
    virtual void StartLockRequests();
    virtual void CreateLockRequests();
    virtual void SubmitLockRequest();
    virtual void SubmitUnlockRequest();
    int num_tx_;
    TPCCLockGen* tpcc_lock_gen_;
};

}}

#endif

