#ifndef RDMA_PROTO_LOCKSIMULATOR_H
#define RDMA_PROTO_LOCKSIMULATOR_H

#include <iostream>
#include <vector>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include "lock_manager.h"
#include "lock_request.h"

using namespace std;

namespace rdma { namespace proto {

class LockManager;

class LockSimulator {

  public:
    LockSimulator(LockManager* manager, int id, int num_manager,
        int num_lock_object, int duration);
    LockSimulator(LockManager* manager, int id, int num_manager,
        int num_lock_object, int duration, bool verbose);
    void Run();
    int NotifyResult(int task, int lock_type, int obj_index, bool result);
    int GetID() const;
    int GetState() const;
    uint64_t GetDuration() const;
    uint64_t GetTotalNumLocks() const;
    uint64_t GetTotalNumUnlocks() const;

    static const int STATE_IDLE = 0;
    static const int STATE_LOCKING = 1;
    static const int STATE_UNLOCKING = 2;
    static const int STATE_DONE = 3;

  private:
    void CreateLockRequests();
    void SubmitLockRequest();
    void SubmitUnlockRequest();

    vector<LockRequest*> requests_;
    LockManager* manager_;
    bool verbose_;
    int state_;
    int id_;
    int num_manager_;
    int num_lock_object_;
    int duration_;
    int last_request_idx_;
    int current_request_idx_;
    int request_size_;
    time_t start_time_;
    time_t current_time_;
    uint64_t total_num_locks_;
    uint64_t total_num_unlocks_;
};

}}

#endif

