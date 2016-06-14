#include "lock_simulator.h"

namespace rdma { namespace proto {

LockSimulator::LockSimulator(LockManager* manager, int id, int num_manager,
    int num_lock_object, int duration) {
  manager_           = manager;
  id_                = id;
  num_manager_       = num_manager;
  num_lock_object_   = num_lock_object;
  duration_          = duration;
  state_             = LockSimulator::STATE_IDLE;
  request_size_      = 10;
  duration_          = duration;
  verbose_           = false;
  total_num_locks_   = 0;
  total_num_unlocks_ = 0;
}

LockSimulator::LockSimulator(LockManager* manager, int id, int num_manager,
    int num_lock_object, int duration, bool verbose) {
  manager_           = manager;
  id_                = id;
  num_manager_       = num_manager;
  num_lock_object_   = num_lock_object;
  duration_          = duration;
  state_             = LockSimulator::STATE_IDLE;
  request_size_      = 10;
  duration_          = duration;
  verbose_           = verbose;
  total_num_locks_   = 0;
  total_num_unlocks_ = 0;
}

void LockSimulator::Run() {
  time(&start_time_);
  srand(time(NULL)+id_);

  CreateLockRequests();
}

int LockSimulator::GetState() const {
  return state_;
}

void LockSimulator::CreateLockRequests() {
  state_ = LockSimulator::STATE_IDLE;

  time(&current_time_);

  if (difftime(current_time_, start_time_) >= duration_) {
    if (verbose_)
      cout << "Time limit of " << duration_ << " has reached. Terminating.";
    state_ = LockSimulator::STATE_DONE;
    return;
  }

  if (requests_.empty()) {
    for (int i = 0; i < request_size_; ++i) {
       LockRequest* request = new LockRequest;
       requests_.push_back(request);
    }
  }


  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->lm_id = rand() % num_manager_;
    requests_[i]->obj_index = rand() % num_lock_object_;
    requests_[i]->lock_type = rand() % 2;
    //requests_[i]->lock_type = 0;
    //if (total_num_locks_ == 0) {
      //requests_[i]->lock_type = 0;
    //} else {
      //if (i==0)
        //requests_[i]->lock_type = 1;
      //else
        //requests_[i]->lock_type = 0;
    //}
    requests_[i]->task = LockManager::TASK_LOCK;
  }

  last_request_idx_ = 0;
  current_request_idx_ = 0;

  SubmitLockRequest();
}

void LockSimulator::SubmitLockRequest() {
  state_ = LockSimulator::STATE_LOCKING;
  if (current_request_idx_ < request_size_) {
    if (verbose_)
      cout << "Simulator " << id_ << ": " << "Sending lock request at LM " <<
        requests_[current_request_idx_]->lm_id <<
        " of type " << requests_[current_request_idx_]->lock_type <<
        " for object " << requests_[current_request_idx_]->obj_index << endl;
    manager_->Lock(id_, requests_[current_request_idx_]->lm_id,
        requests_[current_request_idx_]->lock_type,
        requests_[current_request_idx_]->obj_index);
    last_request_idx_ = current_request_idx_;
    ++current_request_idx_;
  } else {
    --current_request_idx_;
    SubmitUnlockRequest();
  }
}

void LockSimulator::SubmitUnlockRequest() {
  state_ = LockSimulator::STATE_UNLOCKING;
  if (current_request_idx_ >= 0) {
    if (verbose_)
      cout << "Simulator " << id_ << ": " << "Sending unlock request at LM " <<
        requests_[current_request_idx_]->lm_id <<
        " of type " << requests_[current_request_idx_]->lock_type <<
        " for object " << requests_[current_request_idx_]->obj_index << endl;
    manager_->Unlock(id_, requests_[current_request_idx_]->lm_id,
        requests_[current_request_idx_]->lock_type,
        requests_[current_request_idx_]->obj_index);
    last_request_idx_ = current_request_idx_;
    --current_request_idx_;
  } else {
    CreateLockRequests();
  }
}

int LockSimulator::NotifyResult(int task, int lock_type, int obj_index,
    bool result) {
  if (task == LockManager::TASK_LOCK) {
    if (result == true &&
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      if (verbose_) {
        cout << "Simulator " << id_ << ": " << "Successful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      ++total_num_locks_;
      SubmitLockRequest();
    } else {
      if (lock_type == LockManager::SHARED &&
          manager_->GetLockMode() == LockManager::LOCK_REMOTE) {
        current_request_idx_ = last_request_idx_;
      } else {
        current_request_idx_ = last_request_idx_ - 1;
      }
      if (verbose_) {
        cout << "Simulator " << id_ << ": " << "Unsuccessful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      SubmitUnlockRequest();
    }
  } else if (task == LockManager::TASK_UNLOCK) {
    if (result ==  true &&
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      if (verbose_) {
        cout << "Simulator " << id_ << ": " << "Successful unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      ++total_num_unlocks_;
      SubmitUnlockRequest();
    } else {
      if (verbose_) {
        cout << "Simulator " << id_ << ": " << "Unsuccessful unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      SubmitUnlockRequest();
    }
  }
}

int LockSimulator::GetID() const {
  return id_;
}

uint64_t LockSimulator::GetDuration() const {
  return duration_;
}

uint64_t LockSimulator::GetTotalNumLocks() const {
  return total_num_locks_;
}

uint64_t LockSimulator::GetTotalNumUnlocks() const {
  return total_num_unlocks_;
}

}}
