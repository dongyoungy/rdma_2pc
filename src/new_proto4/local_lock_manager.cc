#include "local_lock_manager.h"
#include <unistd.h>
#include "constants.h"

namespace rdma {
namespace proto {

// constructor
LocalLockManager::LocalLockManager(int node_id, int num_nodes, int num_objects,
                                   int max_exclusive_locks,
                                   int max_shared_locks) {
  owner_node_id_ = node_id;
  num_objects_ = num_objects;

  // shared_counter_.clear();
  // exclusive_counter_.clear();
  // lock_status_.clear();

  shared_counter_ = new volatile int[num_nodes * num_objects];
  exclusive_counter_ = new volatile int[num_nodes * num_objects];
  current_exclusive_owner_ = new volatile int[num_nodes * num_objects];
  last_exclusive_owner_ = new volatile int[num_nodes * num_objects];
  lock_status_ = new volatile int[num_nodes * num_objects];
  memset((void*)shared_counter_, 0x00,
         sizeof(volatile int) * num_nodes * num_objects_);
  memset((void*)exclusive_counter_, 0x00,
         sizeof(volatile int) * num_nodes * num_objects_);
  memset((void*)current_exclusive_owner_, 0x00,
         sizeof(volatile int) * num_nodes * num_objects_);
  memset((void*)last_exclusive_owner_, 0x00,
         sizeof(volatile int) * num_nodes * num_objects_);
  memset((void*)lock_status_, 0x00,
         sizeof(volatile int) * num_nodes * num_objects_);
  // wait_queue_ = new queue<LocalLockWaitElement>[num_nodes * num_objects];
  max_shared_locks_ = max_shared_locks;
  max_exclusive_locks_ = max_exclusive_locks;
  mutex_ = new mutex[num_nodes * num_objects];

  max_shared_lock_count_ = 0;
  max_exclusive_lock_count_ = 0;
  shared_to_exclusive_fail_count_ = 0;
  exclusive_to_exclusive_fail_count_ = 0;
  exclusive_to_shared_fail_count_ = 0;
}

// destructor
LocalLockManager::~LocalLockManager() {
  delete[] shared_counter_;
  delete[] exclusive_counter_;
  delete[] current_exclusive_owner_;
  delete[] last_exclusive_owner_;
  delete[] lock_status_;
}

int LocalLockManager::TryLock(int target_node_id, int target_obj_index,
                              int owner_user_id, int lock_type) {
  int ret = 0;
  int idx = index(target_node_id, target_obj_index);
  mutex_[idx].lock();
  // if (lock_status_[idx] != LOCK_STATUS_IDLE) {
  if (lock_status_[idx] != LOCK_STATUS_IDLE &&
      lock_status_[idx] != LOCK_STATUS_FULL) {
    ret = LOCAL_LOCK_FAIL;
  } else {
    if (lock_type == SHARED) {
      if (current_exclusive_owner_[idx] > 0) {
        ++exclusive_to_shared_fail_count_;
        ret = LOCAL_LOCK_FAIL;
      } else if (shared_counter_[idx] == 0 &&
                 current_exclusive_owner_[idx] == 0) {
        lock_status_[idx] = LOCK_STATUS_LOCKING;
        ret = LOCAL_LOCK_RETRY;
      } else if (shared_counter_[idx] > 0 &&
                 current_exclusive_owner_[idx] == 0) {
        ++shared_counter_[idx];
        if (shared_counter_[idx] >= max_shared_locks_) {
          lock_status_[idx] = LOCK_STATUS_FULL;
          ++max_shared_lock_count_;
        }
        ret = LOCAL_LOCK_PASS;
      } else {
        ret = LOCAL_LOCK_FAIL;
      }
    } else {
      if (current_exclusive_owner_[idx] == owner_user_id &&
          shared_counter_[idx] == 0) {
        ret = LOCAL_LOCK_PASS;
      } else if (current_exclusive_owner_[idx] != 0 &&
                 shared_counter_[idx] == 0) {
        // Ex -> Ex
        ++exclusive_to_exclusive_fail_count_;
        ++exclusive_counter_[idx];
        if (exclusive_counter_[idx] >= max_exclusive_locks_) {
          lock_status_[idx] = LOCK_STATUS_FULL;
          ++max_exclusive_lock_count_;
        }
        next_exclusive_owner_[idx][(int)last_exclusive_owner_[idx]] =
            owner_user_id;
        next_exclusive_owner_[idx][owner_user_id] = 0;
        prev_exclusive_owner_[idx][owner_user_id] = last_exclusive_owner_[idx];
        last_exclusive_owner_[idx] = owner_user_id;

        ret = LOCAL_LOCK_WAIT;
      } else if (current_exclusive_owner_[idx] != 0 ||
                 shared_counter_[idx] > 0) {
        if (shared_counter_[idx] > 0) ++shared_to_exclusive_fail_count_;
        ret = LOCAL_LOCK_FAIL;
      } else {
        lock_status_[idx] = LOCK_STATUS_LOCKING;
        ret = LOCAL_LOCK_RETRY;
      }
    }
  }
  mutex_[idx].unlock();
  return ret;
}

int LocalLockManager::TryUnlock(int target_node_id, int target_obj_index,
                                int owner_user_id, int lock_type) {
  int ret = 0;
  int idx = index(target_node_id, target_obj_index);
  mutex_[idx].lock();
  if (lock_status_[idx] != LOCK_STATUS_IDLE &&
      lock_status_[idx] != LOCK_STATUS_FULL) {
    ret = LOCAL_LOCK_FAIL;
  } else {
    if (lock_type == SHARED) {
      if (shared_counter_[idx] > 1) {
        --shared_counter_[idx];
        ret = LOCAL_LOCK_PASS;
      } else if (shared_counter_[idx] == 1) {
        lock_status_[idx] = LOCK_STATUS_UNLOCKING;
        ret = LOCAL_LOCK_RETRY;
      } else {
        ret = LOCAL_LOCK_PASS;
      }
    } else {
      if (last_exclusive_owner_[idx] == owner_user_id) {
        lock_status_[idx] = LOCK_STATUS_UNLOCKING;
        ret = LOCAL_LOCK_RETRY;
      } else {
        --exclusive_counter_[idx];
        int curr_owner = current_exclusive_owner_[idx];
        current_exclusive_owner_[idx] =
            next_exclusive_owner_[idx][owner_user_id];
        next_exclusive_owner_[idx][owner_user_id] = 0;
        prev_exclusive_owner_[idx][owner_user_id] = 0;
        prev_exclusive_owner_[idx][(int)current_exclusive_owner_[idx]] = 0;
        ret = LOCAL_LOCK_PASS;
      }
    }
  }
  mutex_[idx].unlock();
  return ret;
}

int LocalLockManager::CheckLock(int seq_no, int owner_thread_id,
                                int target_node_id, int target_obj_index,
                                int lock_type) {
  // int ret = 0;
  // LocalLockWaitElement new_element;
  // new_element.seq_no           = seq_no;
  // new_element.owner_thread_id  = owner_thread_id;
  // new_element.target_node_id   = target_node_id;
  // new_element.target_obj_index = target_obj_index;
  // new_element.lock_type        = lock_type;

  // if (lock_status_[index(target_node_id,target_obj_index)] !=
  // LOCK_STATUS_IDLE) {  return LOCAL_LOCK_FAIL;
  //}

  // if (shared_counter_[index(target_node_id,target_obj_index)] > 0 ||
  // exclusive_counter_[index(target_node_id,target_obj_index)] > 0) {
  // if (lock_type == EXCLUSIVE) {
  // return LOCAL_LOCK_FAIL;
  //}

  // if (shared_counter_[index(target_node_id,target_obj_index)] > 0 &&
  // exclusive_counter_[index(target_node_id,target_obj_index)] == 0 &&
  // lock_type == SHARED) {
  // return LOCAL_LOCK_PASS;
  //} else {
  // return LOCAL_LOCK_FAIL;
  //}
  //} else {
  //// check if a thread is retrying, meaning it already has an waiting element
  ///in the queue..
  // if (wait_queue_[index(target_node_id,target_obj_index)].size() > 0) {
  // LocalLockWaitElement& elem =
  // wait_queue_[index(target_node_id,target_obj_index)].front();  if
  // (elem.owner_thread_id == owner_thread_id &&  elem.target_node_id ==
  // target_node_id &&  elem.target_obj_index == target_obj_index &&  elem.status
  // == GLOBAL_LOCK_WAITING) {
  // elem.seq_no = seq_no; // update seq_no
  // return LOCAL_LOCK_RETRY;
  //} else if (elem.lock_type == SHARED && lock_type == SHARED) {
  // new_element.status = LOCAL_LOCK_WAITING;
  // ret = LOCAL_LOCK_WAIT;
  // wait_queue_[index(target_node_id,target_obj_index)].push(new_element);
  //} else {
  // return LOCAL_LOCK_FAIL;
  //}
  //} else {
  // new_element.status = GLOBAL_LOCK_WAITING;
  // ret = LOCAL_LOCK_RETRY;
  // wait_queue_[index(target_node_id,target_obj_index)].push(new_element);
  //}
  // if (ret == LOCAL_LOCK_RETRY)
  // return ret;
  //}
  return 0;
}

int LocalLockManager::LeaveExclusive(int target_node_id, int target_obj_index,
                                     int owner_user_id) {
  int ret = 0;
  int idx = index(target_node_id, target_obj_index);
  mutex_[idx].lock();
  if (current_exclusive_owner_[idx] == owner_user_id) {
    ret = LOCAL_LOCK_PASS;
  } else {
    --exclusive_counter_[idx];
    int prev_owner = prev_exclusive_owner_[idx][owner_user_id];
    int next_owner = next_exclusive_owner_[idx][owner_user_id];
    next_exclusive_owner_[idx][prev_owner] = next_owner;
    prev_exclusive_owner_[idx][next_owner] = prev_owner;
    prev_exclusive_owner_[idx][owner_user_id] = 0;
    next_exclusive_owner_[idx][owner_user_id] = 0;
    if (last_exclusive_owner_[idx] == owner_user_id) {
      last_exclusive_owner_[idx] = prev_owner;
    }
    ret = LOCAL_LOCK_FAIL;
  }
  mutex_[idx].unlock();
  return ret;
}

int LocalLockManager::Lock(int target_node_id, int target_obj_index,
                           int owner_user_id, int lock_type, int result) {
  int idx = index(target_node_id, target_obj_index);
  mutex_[idx].lock();
  lock_status_[idx] = LOCK_STATUS_IDLE;
  // lock_status_.erase(index(target_node_id, target_obj_index));
  if (result == RESULT_SUCCESS || result == RESULT_SUCCESS_FROM_QUEUED) {
    if (lock_type == SHARED) {
      ++shared_counter_[idx];
      // if (shared_counter_[index(target_node_id, target_obj_index)] >=
      // max_shared_locks_) {  lock_status_[index(target_node_id,
      // target_obj_index)] = LOCK_STATUS_FULL;
      //}
    } else {
      current_exclusive_owner_[idx] = owner_user_id;
      last_exclusive_owner_[idx] = owner_user_id;
      next_exclusive_owner_[idx][owner_user_id] = 0;
      prev_exclusive_owner_[idx][owner_user_id] = 0;
    }
  }
  mutex_[idx].unlock();
  return FUNC_SUCCESS;
}

int LocalLockManager::Unlock(int target_node_id, int target_obj_index,
                             int owner_user_id, int lock_type, int result) {
  int idx = index(target_node_id, target_obj_index);
  mutex_[idx].lock();
  if (lock_status_[idx] != LOCK_STATUS_FULL) {
    lock_status_[idx] = LOCK_STATUS_IDLE;
  }
  // lock_status_.erase(index(target_node_id, target_obj_index));
  if (result == RESULT_SUCCESS) {
    if (lock_type == SHARED) {
      if (shared_counter_[idx] > 0) --shared_counter_[idx];
      if (shared_counter_[index(target_node_id, target_obj_index)] == 0) {
        lock_status_[index(target_node_id, target_obj_index)] =
            LOCK_STATUS_IDLE;
      }
    } else {
      if (current_exclusive_owner_[idx] == owner_user_id) {
        // exclusive_counter_.erase(index(target_node_id, target_obj_index));
        --exclusive_counter_[idx];
        current_exclusive_owner_[idx] = 0;
        last_exclusive_owner_[idx] = 0;
        next_exclusive_owner_[idx][owner_user_id] = 0;
        prev_exclusive_owner_[idx][owner_user_id] = 0;
        if (exclusive_counter_[index(target_node_id, target_obj_index)] == 0) {
          lock_status_[index(target_node_id, target_obj_index)] =
              LOCK_STATUS_IDLE;
        }
        // if (next_exclusive_owner_.find(idx) != next_exclusive_owner_.end()) {
        // next_exclusive_owner_.erase(idx);
        //}
        // if (prev_exclusive_owner_.find(idx) != prev_exclusive_owner_.end()) {
        // prev_exclusive_owner_.erase(idx);
        //}
      }
    }
  }
  mutex_[idx].unlock();
  return FUNC_SUCCESS;
}

int LocalLockManager::GetCount(int target_node_id, int target_obj_index,
                               int lock_type) {
  if (lock_type == EXCLUSIVE) {
    return current_exclusive_owner_[index(target_node_id, target_obj_index)];
  } else if (lock_type == SHARED) {
    return shared_counter_[index(target_node_id, target_obj_index)];
  }
  return FUNC_FAIL;
}

int LocalLockManager::GetStatus(int target_node_id, int target_obj_index) {
  return lock_status_[index(target_node_id, target_obj_index)];
}

void LocalLockManager::SetStatus(int target_node_id, int target_obj_index,
                                 int status) {
  lock_status_[index(target_node_id, target_obj_index)] = status;
}

queue<LocalLockWaitElement>& LocalLockManager::GetQueue(int target_node_id,
                                                        int target_obj_index) {
  return wait_queue_[index(target_node_id, target_obj_index)];
}

}  // namespace proto
}  // namespace rdma
