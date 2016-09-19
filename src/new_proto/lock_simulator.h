#ifndef RDMA_PROTO_LOCKSIMULATOR_H
#define RDMA_PROTO_LOCKSIMULATOR_H

#include <algorithm>
#include <iostream>
#include <vector>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include "lock_manager.h"
#include "lock_request.h"

#define MAX_LOCK_REQUESTS 500000000

using namespace std;

namespace rdma { namespace proto {

class LockManager;

class LockSimulator {

  public:
    LockSimulator(LockManager* manager, int id, int num_manager,
        int num_lock_object, uint64_t num_lock_request);
    LockSimulator(LockManager* manager, int id, int num_manager,
        int num_lock_object, uint64_t num_lock_request, int num_request_per_tx,
        long seed, bool verbose,
        bool measure_lock_time, int workload_type, int lock_mode,
        double local_percentage = 0.5, double shared_lock_ratio = 0.5,
        bool transaction_delay = false, double transaction_delay_min = 10,
        double transaction_delay_max = 100, int max_backoff_time = 0,
        double* custom_cdf = NULL);
    ~LockSimulator();
    void Run();
    int NotifyResult(int task, int lock_type, int obj_index, int result);
    int GetID() const;
    int GetState() const;
    bool IsLockTimeMeasured() const;
    uint64_t GetDuration() const;
    uint64_t GetTotalNumLocks() const;
    uint64_t GetTotalNumUnlocks() const;
    uint64_t GetTotalNumLockSuccess() const;
    uint64_t GetTotalNumLockFailure() const;
    double GetAverageTimeTakenToLock() const;
    double GetTimeTaken() const;
    double Get99PercentileLockTime();

    static const int STATE_IDLE      = 0;
    static const int STATE_LOCKING   = 1;
    static const int STATE_UNLOCKING = 2;
    static const int STATE_DONE      = 3;

    static const int WORKLOAD_UNIFORM   = 0;
    static const int WORKLOAD_HOTSPOT   = 1;
    static const int WORKLOAD_ALL_LOCAL = 2;
    static const int WORKLOAD_MIXED     = 3;
    static const int WORKLOAD_UNIFORM_RANDOM_LENGTH = 4;
    static const int WORKLOAD_CUSTOM    = 99;

  private:
    void StartLockRequests();
    void CreateLockRequests();
    void SubmitLockRequest();
    void SubmitLockRequestLocal();
    void SubmitUnlockRequest();
    void SubmitUnlockRequestLocal();
    void InitializeCDF();
    inline void SimulateTransactionDelay() {
      if (transaction_delay_) {
        double time_to_sleep = transaction_delay_min_ +
          (rand() % (int)(transaction_delay_max_ - transaction_delay_min_ + 1));
        usleep(time_to_sleep);
      }
    }

    vector<LockRequest*> requests_;
    LockManager* manager_;
    double* lock_times_;
    bool is_tx_failed_;
    bool restart_;
    bool is_all_local_;
    bool measure_lock_time_;
    bool verbose_;
    bool transaction_delay_;
    double* cdf_;
    double total_time_taken_to_lock_;
    double local_percentage_;
    double shared_lock_ratio_;
    double transaction_delay_min_;
    double transaction_delay_max_;
    int retry_;
    int state_;
    int id_;
    int local_manager_id_;
    int num_manager_;
    int num_lock_object_;
    int lock_mode_;
    int local_lock_count_;
    int local_unlock_count_;
    long seed_;
    unsigned int backoff_seed_;
    int last_request_idx_;
    int current_request_idx_;
    int workload_type_;
    int max_backoff_time_;
    struct timespec start_time_;
    struct timespec current_time_;
    struct timespec start_lock_;
    struct timespec end_lock_;
    uint64_t total_num_locks_;
    uint64_t total_num_unlocks_;
    uint64_t total_num_lock_success_;
    uint64_t total_num_lock_failure_;
    uint64_t request_size_;
    uint64_t max_request_size_;
    uint64_t count_;
    uint64_t count_limit_;
    pthread_mutex_t mutex_;
    double time_taken_;
};

}}

#endif

