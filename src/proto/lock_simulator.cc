#include "lock_simulator.h"

namespace rdma { namespace proto {

LockSimulator::LockSimulator(LockManager* manager, int id, int num_manager,
    int num_lock_object, int duration) {
  manager_                  = manager;
  id_                       = id;
  num_manager_              = num_manager;
  num_lock_object_          = num_lock_object;
  duration_                 = duration;
  state_                    = LockSimulator::STATE_IDLE;
  request_size_             = 10;
  duration_                 = duration;
  verbose_                  = false;
  measure_lock_time_        = false;
  workload_type_            = WORKLOAD_UNIFORM;
  lock_mode_                = LockManager::LOCK_REMOTE;
  total_num_locks_          = 0;
  total_num_unlocks_        = 0;
  total_num_lock_success_   = 0;
  total_num_lock_failure_   = 0;
  total_time_taken_to_lock_ = 0;
  is_all_local_             = false;

  pthread_mutex_init(&mutex_, NULL);
}

LockSimulator::LockSimulator(LockManager* manager, int id, int num_manager,
    int num_lock_object, int duration, bool verbose, bool measure_lock_time,
    int workload_type, int lock_mode, double local_percentage) {
  manager_                  = manager;
  id_                       = id;
  num_manager_              = num_manager;
  num_lock_object_          = num_lock_object;
  duration_                 = duration;
  state_                    = LockSimulator::STATE_IDLE;
  request_size_             = 10;
  duration_                 = duration;
  verbose_                  = verbose;
  measure_lock_time_        = measure_lock_time;
  workload_type_            = workload_type;
  lock_mode_                = lock_mode;
  local_percentage_         = local_percentage;
  total_num_locks_          = 0;
  total_num_unlocks_        = 0;
  total_num_lock_success_   = 0;
  total_num_lock_failure_   = 0;
  total_time_taken_to_lock_ = 0;
  is_all_local_             = false;

  pthread_mutex_init(&mutex_, NULL);
}

LockSimulator::~LockSimulator() {
  pthread_mutex_destroy(&mutex_);
}

void LockSimulator::Run() {
  time(&start_time_);
  srand(time(NULL)+id_);
  srand48(time(NULL)+id_);

  StartLockRequests();
  //if (workload_type_ == WORKLOAD_ALL_LOCAL &&
      //lock_mode_ == LockManager::LOCK_LOCAL) {
    //while (true) {
      //CreateLockRequests();
      //time(&current_time_);

      //if (difftime(current_time_, start_time_) >= duration_) {
        //if (verbose_)
          //cout << "Time limit of " << duration_ << " has reached. Terminating.";
        //state_ = LockSimulator::STATE_DONE;
        //return;
      //}
    //}
  //} else {
    //CreateLockRequests();
  //}
}

void LockSimulator::StartLockRequests() {
begin_lock:
  CreateLockRequests();

  if (is_all_local_ && lock_mode_ == LockManager::LOCK_LOCAL &&
      state_ != LockSimulator::STATE_DONE) {
    goto begin_lock;
  }
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

  if (workload_type_ == WORKLOAD_MIXED) {
    if (drand48() < local_percentage_) {
      is_all_local_ = true;
    } else {
      is_all_local_ = false;
    }
  } else if (workload_type_ == WORKLOAD_ALL_LOCAL) {
    is_all_local_ = true;
  }

  for (int i = 0; i < request_size_; ++i) {
    //requests_[i]->lm_id = 0;
    //requests_[i]->lm_id = rand() % num_manager_;
    switch (workload_type_) {
      case WORKLOAD_UNIFORM:
        requests_[i]->lm_id = rand() % num_manager_;
        break;
      case WORKLOAD_HOTSPOT:
        if (drand48() < 0.95) {
          if (num_manager_ > 1)
            requests_[i]->lm_id = rand() % 2;
          else
            requests_[i]->lm_id = rand() % num_manager_;
        } else {
          if (num_manager_ > 2)
            requests_[i]->lm_id = 2 + (rand() % num_manager_ - 2);
          else
            requests_[i]->lm_id = rand() % num_manager_;
        }
      case WORKLOAD_ALL_LOCAL:
        requests_[i]->lm_id = manager_->GetID(); // local only
        break;
      case WORKLOAD_MIXED:
        if (is_all_local_) {
          requests_[i]->lm_id = manager_->GetID(); // local only
        } else {
          requests_[i]->lm_id = rand() % num_manager_;
        }
        break;
      default:
        break;
    }

    requests_[i]->obj_index = rand() % num_lock_object_;
    requests_[i]->lock_type = rand() % 2;
    requests_[i]->task = LockManager::TASK_LOCK;
  }

  last_request_idx_ = 0;
  current_request_idx_ = 0;

  if (is_all_local_ && lock_mode_ == LockManager::LOCK_LOCAL) {
    SubmitLockRequestLocal();
  } else {
    SubmitLockRequest();
  }
}

void LockSimulator::SubmitLockRequest() {
  state_ = LockSimulator::STATE_LOCKING;
  if (current_request_idx_ < request_size_) {
    if (verbose_)
      cout << "Simulator " << id_ << ": " << "Sending lock request at LM " <<
        requests_[current_request_idx_]->lm_id <<
        " of type " << requests_[current_request_idx_]->lock_type <<
        " for object " << requests_[current_request_idx_]->obj_index << endl;
    if (measure_lock_time_)
      clock_gettime(CLOCK_MONOTONIC, &start_lock_);
    manager_->Lock(id_, requests_[current_request_idx_]->lm_id,
        requests_[current_request_idx_]->lock_type,
        requests_[current_request_idx_]->obj_index);
    last_request_idx_ = current_request_idx_;
    ++current_request_idx_;
    ++total_num_locks_;
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
    StartLockRequests();
  }
}

void LockSimulator::SubmitLockRequestLocal() {
  int lock_result;
  state_ = LockSimulator::STATE_LOCKING;
  while (current_request_idx_ < request_size_) {
    if (measure_lock_time_)
      clock_gettime(CLOCK_MONOTONIC, &start_lock_);
    lock_result = manager_->LockLocalDirect(
        id_,
        requests_[current_request_idx_]->lock_type,
        requests_[current_request_idx_]->obj_index
        );
    if (measure_lock_time_) {
      clock_gettime(CLOCK_MONOTONIC, &end_lock_);
      double time_taken = ((double)end_lock_.tv_sec * 1e+9 +
          (double)end_lock_.tv_nsec) - ((double)start_lock_.tv_sec * 1e+9 +
            (double)start_lock_.tv_nsec);
      total_time_taken_to_lock_ += time_taken;
    }
    ++total_num_locks_;
    if (lock_result == LockManager::RESULT_SUCCESS) {
      ++total_num_lock_success_;
      ++current_request_idx_;
    } else {
      ++total_num_lock_failure_;
      --current_request_idx_;
      break;
    }
  }
  if (lock_result == LockManager::RESULT_FAILURE ||
      current_request_idx_ >= request_size_) {
    if (current_request_idx_ >= request_size_) {
       current_request_idx_ = request_size_ - 1;
    }
    SubmitUnlockRequestLocal();
  }
}

void LockSimulator::SubmitUnlockRequestLocal() {
  state_ = LockSimulator::STATE_UNLOCKING;
  while (current_request_idx_ >= 0) {
    manager_->UnlockLocalDirect(
        id_,
        requests_[current_request_idx_]->lock_type,
        requests_[current_request_idx_]->obj_index
        );
    --current_request_idx_;
    ++total_num_unlocks_;
  }
}


int LockSimulator::NotifyResult(int task, int lock_type, int obj_index,
    int result) {

  pthread_mutex_lock(&mutex_);

  if (task == LockManager::TASK_LOCK) {
    if (result == LockManager::RESULT_SUCCESS &&
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      if (verbose_) {
        cout << "Simulator " << id_ << ": " <<
          "Successful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      if (measure_lock_time_) {
        clock_gettime(CLOCK_MONOTONIC, &end_lock_);
        double time_taken = ((double)end_lock_.tv_sec * 1e+9 +
            (double)end_lock_.tv_nsec) - ((double)start_lock_.tv_sec * 1e+9 +
            (double)start_lock_.tv_nsec);
        total_time_taken_to_lock_ += time_taken;
      }
      ++total_num_lock_success_;
      SubmitLockRequest();
    } else {
      if (lock_type == LockManager::SHARED &&
          manager_->GetLockMode() == LockManager::LOCK_REMOTE) {
        current_request_idx_ = last_request_idx_;
      } else {
        current_request_idx_ = last_request_idx_ - 1;
      }
      if (verbose_) {
        cout << "Simulator " << id_ << ": " <<
          "Unsuccessful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      ++total_num_lock_failure_;
      SubmitUnlockRequest();
    }
  } else if (task == LockManager::TASK_UNLOCK) {
    if (result == LockManager::RESULT_SUCCESS &&
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      if (verbose_) {
        cout << "Simulator " << id_ << ": " <<
          "Successful unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      ++total_num_unlocks_;
      SubmitUnlockRequest();
    } else if (result == LockManager::RESULT_RETRY) {
      current_request_idx_ = last_request_idx_;
      if (verbose_) {
        cout << "Simulator " << id_ << ": " <<
          "retrying exclusive unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      SubmitUnlockRequest();
    } else {
      if (verbose_) {
        cout << "Simulator " << id_ << ": " <<
          "Unsuccessful unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
      }
      SubmitUnlockRequest();
    }
  }
  pthread_mutex_unlock(&mutex_);
}

int LockSimulator::GetID() const {
  return id_;
}

bool LockSimulator::IsLockTimeMeasured() const {
  return measure_lock_time_;
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

uint64_t LockSimulator::GetTotalNumLockSuccess() const {
  return total_num_lock_success_;
}

uint64_t LockSimulator::GetTotalNumLockFailure() const {
  return total_num_lock_failure_;
}

double LockSimulator::GetAverageTimeTakenToLock() const {
  return total_time_taken_to_lock_ / (double)total_num_lock_success_;
}

}}
