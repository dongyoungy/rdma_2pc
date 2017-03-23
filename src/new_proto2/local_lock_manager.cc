#include "constants.h"
#include "local_lock_manager.h"

namespace rdma { namespace proto {

// constructor
LocalLockManager::LocalLockManager(int node_id, int num_nodes, int num_objects) {
  owner_node_id_ = node_id;
  num_objects_ = num_objects;
  shared_counter_ = new int[num_nodes * num_objects];
  exclusive_counter_ = new int[num_nodes * num_objects];
  lock_status_ = new int[num_nodes * num_objects];
  wait_queue_ = new queue<LocalLockWaitElement>[num_nodes * num_objects];
}

// destructor
LocalLockManager::~LocalLockManager() {
  delete[] shared_counter_;
  delete[] exclusive_counter_;
  delete[] lock_status_;
  delete[] wait_queue_;
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

int LocalLockManager::Lock(int target_node_id, int target_obj_index, int lock_type) {
  if (lock_type == EXCLUSIVE) {
    exclusive_counter_[index(target_node_id,target_obj_index)]++;
  } else if (lock_type == SHARED) {
    shared_counter_[index(target_node_id,target_obj_index)]++;
  } else {
    return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

int LocalLockManager::Unlock(int target_node_id, int target_obj_index, int lock_type) {
  if (lock_type == EXCLUSIVE) {
    exclusive_counter_[index(target_node_id,target_obj_index)]--;
  } else if (lock_type == SHARED) {
    shared_counter_[index(target_node_id,target_obj_index)]--;
  } else {
    return FUNC_FAIL;
  }

  if (wait_queue_[index(target_node_id,target_obj_index)].size() > 0) {
    return LOCAL_LOCK_EXIST;
  } else {
    return LOCAL_LOCK_NOT_EXIST;
  }
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
