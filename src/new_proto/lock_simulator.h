#ifndef RDMA_PROTO_LOCKSIMULATOR_H
#define RDMA_PROTO_LOCKSIMULATOR_H

#include <algorithm>
#include <iostream>
#include <vector>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include "constants.h"
#include "lock_manager.h"
#include "lock_request.h"

#define MAX_LOCK_REQUESTS 500000000

using namespace std;

namespace rdma { namespace proto {

class LockManager;

class LockSimulator {

  public:
    LockSimulator();
    LockSimulator(LockManager* manager, uint32_t id, int num_manager,
        int num_lock_object, uint64_t num_lock_request);
    LockSimulator(LockManager* manager, uint32_t id, int num_manager,
        int num_lock_object, uint64_t num_lock_request, int num_request_per_tx,
        long seed, bool verbose,
        bool measure_lock_time, int workload_type, int lock_mode,
        double local_percentage = 0.5, double shared_lock_ratio = 0.5,
        bool transaction_delay = false, double transaction_delay_min = 10,
        double transaction_delay_max = 100, int min_backoff_time = 5000,
        int max_backoff_time = 100000,
        int sleep_time = 1000,
        double* custom_cdf = NULL);
    ~LockSimulator();
    virtual void Run();
    int NotifyResult(int seq_no, int task, int lock_type, int obj_index, int result);
    int TimeOut();
    uint32_t GetID() const;
    int GetState() const;
    int GetLockMode() const;
    int GetMaxBackoff() const;
    int GetCurrentBackoff() const;
    int GetLastTask();
    int GetSleepTime() const;
    bool IsLockTimeMeasured() const;
    bool GetMeasureTimeOut() const;
    bool IsBackingOff() const;
    bool IsVerbose() const;
    uint64_t GetDuration() const;
    uint64_t GetTotalNumLocks() const;
    uint64_t GetTotalNumUnlocks() const;
    uint64_t GetTotalNumLockSuccess() const;
    uint64_t GetTotalNumLockFailure() const;
    uint64_t GetTotalNumTimeout() const;
    double GetAverageTimeTakenToLock() const;
    double GetTimeTaken() const;
    double Get99PercentileLockTime();
    double Get95PercentileLockTime();
    double Get99PercentileSingleLockTime();
    double GetTimeSinceLastLock();
    uint64_t GetCount() const;
    uint64_t GetSeqCount() const;
    void ChangeState(int state);

    static void* CheckTimeOut(void* arg);

    static const int STATE_IDLE      = 0;
    static const int STATE_WAIT      = 1;
    static const int STATE_LOCKING   = 2;
    static const int STATE_UNLOCKING = 3;
    static const int STATE_DONE      = 4;
    static const int STATE_TIMEOUT   = 5;
    static const int STATE_QUEUED    = 6;

    static const int WORKLOAD_UNIFORM   = 0;
    static const int WORKLOAD_HOTSPOT   = 1;
    static const int WORKLOAD_ALL_LOCAL = 2;
    static const int WORKLOAD_MIXED     = 3;
    static const int WORKLOAD_UNIFORM_RANDOM_LENGTH = 4;
    static const int WORKLOAD_CUSTOM    = 99;

  protected:

    inline void SimulateTransactionDelay() {
      if (transaction_delay_) {
        double time_to_sleep = transaction_delay_min_ +
          (rand() % (int)(transaction_delay_max_ - transaction_delay_min_ + 1));
        usleep(time_to_sleep);
      }
    }

    LockManager* manager_;
    bool restart_;
    bool measure_lock_time_;
    bool verbose_;
    bool transaction_delay_;
    bool is_tx_failed_;
    volatile bool is_backing_off_;
    double* lock_times_; // time taken to get all locks requred by a tx
    double* single_lock_times_; // average time taken to get a single lock in a tx
    double transaction_delay_min_;
    double transaction_delay_max_;
    double time_taken_;
    int sleep_time_;
    int num_manager_;
    uint32_t id_;
    int lock_mode_;
    int seq_count_;
    int last_request_idx_;
    int current_request_idx_;
    int max_backoff_time_;
    int default_backoff_time_;
    int current_backoff_time_;
    int retry_;
    int state_;
    int last_seq_no_;
    int last_task_;
    int think_time_;
    unsigned int seed_;
    unsigned int seed2_;
    unsigned int backoff_seed_;
    unsigned int time_out_seed_;
    uint64_t last_count_;
    uint64_t request_size_;
    uint64_t max_request_size_;
    uint64_t count_;
    uint64_t total_num_locks_;
    uint64_t total_num_unlocks_;
    uint64_t total_num_lock_success_;
    uint64_t total_num_lock_failure_;
    uint64_t total_num_timeouts_;
    vector<LockRequest*> requests_;
    pthread_mutex_t mutex_;
    pthread_mutex_t time_mutex_;
    pthread_mutex_t lock_mutex_;
    pthread_mutex_t state_mutex_;
    pthread_cond_t state_cond_;
    pthread_t timeout_thread_;
    struct timespec last_lock_time_;
    struct timespec start_lock_;
    struct timespec end_lock_;
    struct timespec start_time_;
    struct timespec current_time_;
    volatile bool measure_time_out_;

    virtual void StartLockRequests();
    virtual void SubmitLockRequest();
    virtual void SubmitUnlockRequest();
    virtual void CreateLockRequests();
    double total_time_taken_to_lock_;
    int local_manager_id_;
    int workload_type_;
  private:
    void SubmitLockRequestLocal();
    void SubmitUnlockRequestLocal();
    void InitializeCDF();

    bool is_all_local_;
    double* cdf_;
    double local_percentage_;
    double shared_lock_ratio_;
    int num_lock_object_;
    int local_lock_count_;
    int local_unlock_count_;
    uint64_t count_limit_;
};

}}

#endif

