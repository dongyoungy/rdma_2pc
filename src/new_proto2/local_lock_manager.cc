#include <unistd.h>
#include "constants.h"
#include "local_lock_manager.h"

namespace rdma { namespace proto {

// constructor
LocalLockManager::LocalLockManager(int node_id, int num_nodes, int num_objects) {
  owner_node_id_ = node_id;
  num_objects_ = num_objects;
  shared_counter_ = new volatile int[num_nodes * num_objects];
  exclusive_counter_ = new volatile int[num_nodes * num_objects];
  lock_status_ = new volatile int[num_nodes * num_objects];
  //wait_queue_ = new queue<LocalLockWaitElement>[num_nodes * num_objects];

  memset((void*)shared_counter_, 0x00, sizeof(volatile int)*num_nodes*num_objects_);
  memset((void*)exclusive_counter_, 0x00, sizeof(volatile int)*num_nodes*num_objects_);
  memset((void*)lock_status_, 0x00, sizeof(volatile int)*num_nodes*num_objects_);

  pthread_mutex_init(&mutex_, NULL);
}

// destructor
LocalLockManager::~LocalLockManager() {
  delete[] shared_counter_;
  delete[] exclusive_counter_;
  delete[] lock_status_;
}

int LocalLockManager::TryLock(int target_node_id, int target_obj_index, int owner_user_id,
    int lock_type) {
  int ret = 0;
  pthread_mutex_lock(&mutex_);
  if (lock_status_[index(target_node_id, target_obj_index)] != LOCK_STATUS_IDLE) {
    ret = LOCAL_LOCK_FAIL;
  } else {
    if (lock_type == SHARED) {
      if (exclusive_counter_[index(target_node_id, target_obj_index)] > 0) {
        ret = LOCAL_LOCK_FAIL;
      } else if (shared_counter_[index(target_node_id,target_obj_index)] == 0 &&
          exclusive_counter_[index(target_node_id,target_obj_index)] == 0) {
        lock_status_[index(target_node_id, target_obj_index)] = LOCK_STATUS_LOCKING;
        ret = LOCAL_LOCK_RETRY;
      } else if (shared_counter_[index(target_node_id,target_obj_index)] > 0 &&
          exclusive_counter_[index(target_node_id,target_obj_index)] == 0) {
        ++shared_counter_[index(target_node_id, target_obj_index)];
        ret = LOCAL_LOCK_PASS;
      } else {
        ret = LOCAL_LOCK_FAIL;
      }
    } else {
      if (exclusive_counter_[index(target_node_id, target_obj_index)] == owner_user_id &&
          shared_counter_[index(target_node_id, target_obj_index)] == 0) {
        ret = LOCAL_LOCK_PASS;
      } else if (exclusive_counter_[index(target_node_id, target_obj_index)] > 0 ||
          shared_counter_[index(target_node_id, target_obj_index)] > 0) {
        ret = LOCAL_LOCK_FAIL;
      } else {
        lock_status_[index(target_node_id, target_obj_index)] = LOCK_STATUS_LOCKING;
        ret = LOCAL_LOCK_RETRY;
      }
    }
  }

  pthread_mutex_unlock(&mutex_);
  return ret;
}

int LocalLockManager::TryUnlock(int target_node_id, int target_obj_index, int owner_user_id,
    int lock_type) {
  int ret = 0;
  pthread_mutex_lock(&mutex_);
  if (lock_status_[index(target_node_id, target_obj_index)] != LOCK_STATUS_IDLE) {
    ret = LOCAL_LOCK_FAIL;
  } else {
    if (lock_type == SHARED) {
      if (shared_counter_[index(target_node_id, target_obj_index)] > 1) {
        --shared_counter_[index(target_node_id, target_obj_index)];
        ret = LOCAL_LOCK_PASS;
      } else if (shared_counter_[index(target_node_id, target_obj_index)] == 1) {
        lock_status_[index(target_node_id, target_obj_index)] = LOCK_STATUS_UNLOCKING;
        ret = LOCAL_LOCK_RETRY;
      } else {
        ret = LOCAL_LOCK_PASS;
      }
    } else {
      if (exclusive_counter_[index(target_node_id, target_obj_index)] == owner_user_id) {
        lock_status_[index(target_node_id, target_obj_index)] = LOCK_STATUS_UNLOCKING;
        ret = LOCAL_LOCK_RETRY;
      } else {
        ret = LOCAL_LOCK_PASS;
      }
    }
  }
  pthread_mutex_unlock(&mutex_);
  return ret;
}

int LocalLockManager::CheckLock(int seq_no, int owner_thread_id, int target_node_id,
    int target_obj_index, int lock_type) {

  int ret = 0;
  LocalLockWaitElement new_element;
  new_element.seq_no           = seq_no;
  new_element.owner_thread_id  = owner_thread_id;
  new_element.target_node_id   = target_node_id;
  new_element.target_obj_index = target_obj_index;
  new_element.lock_type        = lock_type;

  if (lock_status_[index(target_node_id,target_obj_index)] != LOCK_STATUS_IDLE) {
    return LOCAL_LOCK_FAIL;
  }

  if (shared_counter_[index(target_node_id,target_obj_index)] > 0 ||
      exclusive_counter_[index(target_node_id,target_obj_index)] > 0) {
    if (lock_type == EXCLUSIVE) {
      return LOCAL_LOCK_FAIL;
    }

    if (shared_counter_[index(target_node_id,target_obj_index)] > 0 &&
        exclusive_counter_[index(target_node_id,target_obj_index)] == 0 &&
        lock_type == SHARED) {
      return LOCAL_LOCK_PASS;
    } else {
      return LOCAL_LOCK_FAIL;
    }
  } else {
    // check if a thread is retrying, meaning it already has an waiting element in the queue..
    if (wait_queue_[index(target_node_id,target_obj_index)].size() > 0) {
      LocalLockWaitElement& elem = wait_queue_[index(target_node_id,target_obj_index)].front();
      if (elem.owner_thread_id == owner_thread_id &&
          elem.target_node_id == target_node_id &&
          elem.target_obj_index == target_obj_index &&
          elem.status == GLOBAL_LOCK_WAITING) {
        elem.seq_no = seq_no; // update seq_no
        return LOCAL_LOCK_RETRY;
      } else if (elem.lock_type == SHARED && lock_type == SHARED) {
        new_element.status = LOCAL_LOCK_WAITING;
        ret = LOCAL_LOCK_WAIT;
        wait_queue_[index(target_node_id,target_obj_index)].push(new_element);
      } else {
        return LOCAL_LOCK_FAIL;
      }
    } else {
      new_element.status = GLOBAL_LOCK_WAITING;
      ret = LOCAL_LOCK_RETRY;
      wait_queue_[index(target_node_id,target_obj_index)].push(new_element);
    }
    //if (ret == LOCAL_LOCK_RETRY)
    return ret;
  }
}

int LocalLockManager::Lock(int target_node_id, int target_obj_index, int owner_user_id,
    int lock_type, int result) {
  pthread_mutex_lock(&mutex_);
  lock_status_[index(target_node_id, target_obj_index)] = LOCK_STATUS_IDLE;
  if (result == RESULT_SUCCESS || result == RESULT_SUCCESS_FROM_QUEUED) {
    if (lock_type == SHARED) {
      ++shared_counter_[index(target_node_id, target_obj_index)];
    } else {
      exclusive_counter_[index(target_node_id, target_obj_index)] = owner_user_id;
    }
  }
  pthread_mutex_unlock(&mutex_);
  return FUNC_SUCCESS;
}

int LocalLockManager::Unlock(int target_node_id, int target_obj_index, int owner_user_id,
    int lock_type, int result) {
  pthread_mutex_lock(&mutex_);
  lock_status_[index(target_node_id, target_obj_index)] = LOCK_STATUS_IDLE;
  if (result == RESULT_SUCCESS) {
    if (lock_type == SHARED) {
      if (shared_counter_[index(target_node_id, target_obj_index)] > 0)
        --shared_counter_[index(target_node_id, target_obj_index)];
    } else {
      if (exclusive_counter_[index(target_node_id, target_obj_index)] == owner_user_id)
        exclusive_counter_[index(target_node_id, target_obj_index)] = 0;
    }
  }
  pthread_mutex_unlock(&mutex_);
  return FUNC_SUCCESS;
}

int LocalLockManager::GetCount(int target_node_id, int target_obj_index, int lock_type) {
  if (lock_type == EXCLUSIVE) {
    return exclusive_counter_[index(target_node_id,target_obj_index)];
  } else if (lock_type == SHARED) {
    return shared_counter_[index(target_node_id,target_obj_index)];
  }
  return FUNC_FAIL;
}

int LocalLockManager::GetStatus(int target_node_id, int target_obj_index) {
  return lock_status_[index(target_node_id,target_obj_index)];
}

void LocalLockManager::SetStatus(int target_node_id, int target_obj_index, int status) {
  lock_status_[index(target_node_id,target_obj_index)] = status;
}

queue<LocalLockWaitElement>& LocalLockManager::GetQueue(int target_node_id,
    int target_obj_index) {
  return wait_queue_[index(target_node_id,target_obj_index)];
}

}}
