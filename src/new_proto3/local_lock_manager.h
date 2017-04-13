#ifndef RDMA_PROTO_LOCALLOCKMANAGER_H
#define RDMA_PROTO_LOCALLOCKMANAGER_H

#include <iostream>
#include <cstring>
#include <queue>
#include <map>
#include <unordered_map>
#include <mutex>
#include <pthread.h>

#include "tbb/concurrent_unordered_map.h"
#include "local_lock_wait_element.h"

using namespace std;

namespace rdma { namespace proto {

/**
 * This class manages globally obtained locks for local threads.
 * The class does not guarantee thread-safe. Therefore, other classes which utilize this class
 * must ensure that race condition does not happen with extra care.
 */
class LocalLockManager {
  public:
    LocalLockManager(int node_id, int num_nodes, int num_objects);
    ~LocalLockManager();
    int TryLock(int target_node_id, int target_obj_index, int owner_user_id, int lock_type);
    int TryUnlock(int target_node_id, int target_obj_index, int owner_user_id, int lock_type);
    int CheckLock(int seq_no, int ownder_thread_id, int target_node_id,
        int target_obj_index, int lock_type);
    int Lock(int target_node_id, int target_obj_index, int owner_user_id,
        int lock_type, int result);
    int Unlock(int target_node_id, int target_obj_index, int owner_user_id,
        int lock_type, int result);
    int LeaveExclusive(int target_node_id, int target_obj_index, int owner_user_id);
    int GetCount(int target_node_id, int target_obj_index, int lock_type);
    int GetStatus(int target_node_id, int target_obj_index);
    void SetStatus(int target_node_id, int target_obj_index, int status);
    queue<LocalLockWaitElement>& GetQueue(int target_node_id, int target_obj_index);
    inline int index(int node_id, int obj_index) const {
      return node_id * num_objects_ + obj_index;
    }
    inline long GetSharedToExclusiveFailCount() const {
      return shared_to_exclusive_fail_count_;
    }
    inline long GetExclusiveToExclusiveFailCount() const {
      return exclusive_to_exclusive_fail_count_;
    }
    inline long GetExclusiveToSharedFailCount() const {
      return exclusive_to_shared_fail_count_;
    }
    inline long GetMaxSharedLockCount() const {
      return max_shared_lock_count_;
    }
    inline long GetMaxExclusiveLockCount() const {
      return max_exclusive_lock_count_;
    }
    inline int GetCurrentExclusiveOwner(int node_id, int obj_index) {
      return current_exclusive_owner_[index(node_id,obj_index)];
    }
  private:
    int max_shared_locks_;
    int max_exclusive_locks_;
    int owner_node_id_;
    int num_objects_;
    volatile int* shared_counter_;
    volatile int* exclusive_counter_;
    volatile int* current_exclusive_owner_;
    volatile int* last_exclusive_owner_;
    volatile int* lock_status_;
    //map<int, int> shared_counter_;
    //map<int, int> exclusive_counter_;
    //map<int, int> lock_status_;

    tbb::concurrent_unordered_map<int, map<int, int>> next_exclusive_owner_;
    tbb::concurrent_unordered_map<int, map<int, int>> prev_exclusive_owner_;
    queue<LocalLockWaitElement>* wait_queue_;
    std::mutex* mutex_;

    // for stats
    long shared_to_exclusive_fail_count_;
    long exclusive_to_exclusive_fail_count_;
    long exclusive_to_shared_fail_count_;
    long max_shared_lock_count_;
    long max_exclusive_lock_count_;
};

}}

#endif
