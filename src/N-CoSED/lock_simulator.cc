#include "lock_simulator.h"

namespace rdma { namespace n_cosed {

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
  local_manager_id_         = manager_->GetID();

  lock_times_ = new double[MAX_LOCK_REQUESTS];

  pthread_mutex_init(&mutex_, NULL);
}

LockSimulator::LockSimulator(LockManager* manager, int id, int num_manager,
    int num_lock_object, int num_lock_request, int duration, bool verbose,
    bool measure_lock_time, int workload_type, int lock_mode,
    double local_percentage, double shared_lock_ratio, bool transaction_delay,
    double transaction_delay_min, double transaction_delay_max,
    double* custom_cdf) {
  manager_                  = manager;
  id_                       = id;
  num_manager_              = num_manager;
  num_lock_object_          = num_lock_object;
  duration_                 = duration;
  state_                    = LockSimulator::STATE_IDLE;
  request_size_             = num_lock_request;
  duration_                 = duration;
  verbose_                  = verbose;
  measure_lock_time_        = measure_lock_time;
  workload_type_            = workload_type;
  lock_mode_                = lock_mode;
  local_percentage_         = local_percentage;
  shared_lock_ratio_        = shared_lock_ratio;
  total_num_locks_          = 0;
  total_num_unlocks_        = 0;
  total_num_lock_success_   = 0;
  total_num_lock_failure_   = 0;
  total_time_taken_to_lock_ = 0;
  is_all_local_             = false;
  transaction_delay_        = transaction_delay;
  transaction_delay_min_    = transaction_delay_min;
  transaction_delay_max_    = transaction_delay_max;
  local_manager_id_         = manager_->GetID();

  lock_times_ = new double[MAX_LOCK_REQUESTS];

  if (custom_cdf) {
    cdf_ = new double[num_manager_];
    for (int i=0;i<num_manager_;++i) {
      cdf_[i] = custom_cdf[i];
    }
  } else {
    cdf_ = NULL;
  }

  pthread_mutex_init(&mutex_, NULL);
}

LockSimulator::~LockSimulator() {
  pthread_mutex_destroy(&mutex_);
  if (lock_times_) {
    delete[] lock_times_;
  }
}

void LockSimulator::Run() {
  time(&start_time_);
  srand(time(NULL)+id_);
  srand48(time(NULL)+id_);

  InitializeCDF();

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

void LockSimulator::InitializeCDF() {
  if (cdf_ == NULL) {
    cdf_ = new double[num_manager_];
  }
  for (int i=0;i<num_manager_;++i) {
    cdf_[i] = 0;
  }
  double val;
  switch (workload_type_) {
    case WORKLOAD_UNIFORM:
      val = 1.0 / (double)num_manager_;
      cdf_[0] = val;
      for (int i=1;i<num_manager_;++i) {
        cdf_[i] = cdf_[i-1] + val;
      }
      break;
    case WORKLOAD_HOTSPOT:
      if (num_manager_ > 1) {
        val = 0.05 / (double) (num_manager_ - 1);
        cdf_[0] = 0.95;
        for (int i=1;i<num_manager_;++i) {
          cdf_[i] = cdf_[i-1]+val;
        }
      } else {
        cdf_[0] = 1;
      }
      break;
    case WORKLOAD_ALL_LOCAL:
      for (int i=local_manager_id_;i<num_manager_;++i) {
        cdf_[i] = 1;
      }
      break;
    case WORKLOAD_MIXED:
    {
      //if (num_manager_ > 1) {
        //double* pdf = new double[num_manager_];
        //double other_percentage = (1.0 - local_percentage_) / (double)(num_manager_ - 1);
        //for (int i=0;i<num_manager_;++i) {
          //pdf[i] = other_percentage;
        //}
        //pdf[local_manager_id_] = local_percentage_;
        //cdf_[0] = pdf[0];
        //for (int i=1;i<num_manager_;++i) {
          //cdf_[i] = cdf_[i-1]+pdf[i];
        //}
      //} else {
        //cdf_[0] = 1;
      //}
      break;
    }
    case WORKLOAD_CUSTOM:
    default:
      break;
  }
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
    if (drand48() < local_percentage_ || num_manager_ == 1) {
      is_all_local_ = true;
    } else {
      is_all_local_ = false;
    }
  } else if (workload_type_ == WORKLOAD_ALL_LOCAL) {
    is_all_local_ = true;
  }

  for (int i = 0; i < request_size_; ++i) {
    if (workload_type_ == WORKLOAD_MIXED ||
        workload_type_ == WORKLOAD_ALL_LOCAL) {
      if (is_all_local_) {
        requests_[i]->lm_id = local_manager_id_;
      } else {
        requests_[i]->lm_id = 1 + rand() % num_manager_;
      }
    } else {
      double val = drand48();
      for (int j = 0; j < num_manager_; ++j) {
        if (val <= cdf_[j]) {
          requests_[i]->lm_id = j + 1;
          break;
        }
      }
    }
    //requests_[i]->lm_id = 1;
    requests_[i]->obj_index = rand() % num_lock_object_;
    if (drand48() < shared_lock_ratio_) {
      requests_[i]->lock_type = LockManager::SHARED;
      //requests_[i]->lock_type = LockManager::EXCLUSIVE;
    } else {
      //requests_[i]->lock_type = LockManager::SHARED;
      requests_[i]->lock_type = LockManager::EXCLUSIVE;
    }
    //if (id_ == 1) {
      //requests_[i]->lock_type = LockManager::SHARED;
      ////requests_[i]->lock_type = LockManager::EXCLUSIVE;
    //} else {
      //requests_[i]->lock_type = LockManager::SHARED;
      ////requests_[i]->lock_type = LockManager::EXCLUSIVE;
    //}
    requests_[i]->task = LockManager::TASK_LOCK;
  }

  is_all_local_ = true;
  for (int i = 0; i < request_size_; ++i) {
    if (requests_[i]->lm_id != local_manager_id_) {
      is_all_local_ = false;
      break;
    }
  }

  //for (int i = 0; i < request_size_; ++i) {
    //switch (workload_type_) {
      //case WORKLOAD_UNIFORM:
        //requests_[i]->lm_id = rand() % num_manager_;
        //break;
      //case WORKLOAD_HOTSPOT:
        //if (drand48() < 0.95) {
          //if (num_manager_ > 1)
            //requests_[i]->lm_id = rand() % 2;
          //else
            //requests_[i]->lm_id = rand() % num_manager_;
        //} else {
          //if (num_manager_ > 2)
            //requests_[i]->lm_id = 2 + (rand() % num_manager_ - 2);
          //else
            //requests_[i]->lm_id = rand() % num_manager_;
        //}
      //case WORKLOAD_ALL_LOCAL:
        //requests_[i]->lm_id = manager_->GetID(); // local only
        //break;
      //case WORKLOAD_MIXED:
        //if (is_all_local_) {
          //requests_[i]->lm_id = manager_->GetID(); // local only
        //} else {
          //requests_[i]->lm_id = rand() % num_manager_;
        //}
        //break;
      //default:
        //break;
    //}

  last_request_idx_ = 0;
  current_request_idx_ = 0;

  if (is_all_local_ && lock_mode_ == LockManager::LOCK_LOCAL) {
    SubmitLockRequestLocal();
  } else {
    SubmitLockRequest();
  }
  //SubmitLockRequest();
}

void LockSimulator::SubmitLockRequest() {

  //usleep(1000000+rand()%1000000);

  int lock_result = LockManager::RESULT_SUCCESS;
  state_ = LockSimulator::STATE_LOCKING;
  local_lock_count_ = 0;

  // process local locks first
  //while (current_request_idx_ < request_size_ &&
      //requests_[current_request_idx_]->lm_id == local_manager_id_ &&
      ////drand48() < 0.5 &&
      //manager_->GetLockMode() == LockManager::LOCK_LOCAL) {
    //if (verbose_)
      //cout << "(LOCAL) Simulator " << id_ << ": " << "Sending lock request at LM " <<
        //requests_[current_request_idx_]->lm_id <<
        //" of type " << requests_[current_request_idx_]->lock_type <<
        //" for object " << requests_[current_request_idx_]->obj_index << endl;
    //if (measure_lock_time_)
      //clock_gettime(CLOCK_MONOTONIC, &start_lock_);
    //lock_result = manager_->LockLocalDirect(
        //id_,
        //requests_[current_request_idx_]->lock_type,
        //requests_[current_request_idx_]->obj_index
        //);
    //if (lock_result == LockManager::RESULT_FAILURE) {
      //break;
    //}
    //if (measure_lock_time_) {
      //clock_gettime(CLOCK_MONOTONIC, &end_lock_);
      //double time_taken = ((double)end_lock_.tv_sec * 1e+9 +
          //(double)end_lock_.tv_nsec) - ((double)start_lock_.tv_sec * 1e+9 +
            //(double)start_lock_.tv_nsec);
      //lock_times_[total_num_locks_] = time_taken;
      //total_time_taken_to_lock_ += time_taken;
    //}
    //++local_lock_count_;
    //++total_num_locks_;
    //++current_request_idx_;

    //if (lock_result == LockManager::RESULT_SUCCESS) {
      //++total_num_lock_success_;
    //} else {
      //++total_num_lock_failure_;
      //break;
    //}
  //}

  //// if got all locks by processing local locks or local lock has been failed
  //if (current_request_idx_ >= request_size_ ||
      //lock_result == LockManager::RESULT_FAILURE) {
    //SimulateTransactionDelay();
    //--current_request_idx_;
    //SubmitUnlockRequest();
    //return;
  //}

  if (current_request_idx_ < request_size_) {
    if (verbose_) {
      pthread_mutex_lock(&LockManager::print_mutex);
      cout << "(REMOTE) Simulator " << id_ << ": " << "Sending lock request at LM " <<
        requests_[current_request_idx_]->lm_id <<
        " of type " << requests_[current_request_idx_]->lock_type <<
        " for object " << requests_[current_request_idx_]->obj_index << endl;
      pthread_mutex_unlock(&LockManager::print_mutex);
    }
    if (measure_lock_time_)
      clock_gettime(CLOCK_MONOTONIC, &start_lock_);
    manager_->Lock(id_, requests_[current_request_idx_]->lm_id,
        requests_[current_request_idx_]->lock_type,
        requests_[current_request_idx_]->obj_index);
    last_request_idx_ = current_request_idx_;
    ++current_request_idx_;
    ++total_num_locks_;
  } else {
    SimulateTransactionDelay();
    //--current_request_idx_;
    //SubmitUnlockRequest();
  }
}

void LockSimulator::SubmitUnlockRequest() {

  //usleep(1000000+rand()%1000000);

  int lock_result = LockManager::RESULT_SUCCESS;
  state_ = LockSimulator::STATE_UNLOCKING;
  local_unlock_count_ = 0;

  restart_ = false;

   //process local unlocks first
  //while (current_request_idx_ >= 0 &&
      //requests_[current_request_idx_]->lm_id == local_manager_id_ &&
      ////drand48() < 0.5 &&
      //manager_->GetLockMode() == LockManager::LOCK_LOCAL) {
    //if (verbose_)
      //cout << "(LOCAL) Simulator " << id_ << ": " << "Sending unlock request at LM " <<
        //requests_[current_request_idx_]->lm_id <<
        //" of type " << requests_[current_request_idx_]->lock_type <<
        //" for object " << requests_[current_request_idx_]->obj_index << endl;
    //manager_->UnlockLocalDirect(
        //id_,
        //requests_[current_request_idx_]->lock_type,
        //requests_[current_request_idx_]->obj_index
        //);
    //--current_request_idx_;
    //++local_unlock_count_;
    //++total_num_unlocks_;
    //if (current_request_idx_ < 0) {
      //if (local_unlock_count_ == request_size_)
        //restart_ = true;
      //else
        //StartLockRequests();
    //}
  //}

  if (current_request_idx_ >= 0) {
    if (verbose_) {
      pthread_mutex_lock(&LockManager::print_mutex);
      cout << "(REMOTE) Simulator " << id_ << ": " << "Sending unlock request at LM " <<
        requests_[current_request_idx_]->lm_id <<
        " of type " << requests_[current_request_idx_]->lock_type <<
        " for object " << requests_[current_request_idx_]->obj_index << endl;
      pthread_mutex_unlock(&LockManager::print_mutex);
    }
    manager_->Unlock(id_, requests_[current_request_idx_]->lm_id,
        requests_[current_request_idx_]->lock_type,
        requests_[current_request_idx_]->obj_index);
    last_request_idx_ = current_request_idx_;
    --current_request_idx_;
  } else {
    if (!restart_)
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
      lock_times_[total_num_locks_] = time_taken;
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
      SimulateTransactionDelay();
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


int LockSimulator::NotifyResult(int task, int lock_type, int home_id,
    int obj_index,
    int result) {

  pthread_mutex_lock(&mutex_);

  if (task == LockManager::TASK_LOCK) {

    if (measure_lock_time_) {
      clock_gettime(CLOCK_MONOTONIC, &end_lock_);
      double time_taken = ((double)end_lock_.tv_sec * 1e+9 +
          (double)end_lock_.tv_nsec) - ((double)start_lock_.tv_sec * 1e+9 +
            (double)start_lock_.tv_nsec);
      lock_times_[total_num_locks_-1] = time_taken;
      total_time_taken_to_lock_ += time_taken;
    }

    if (result == LockManager::RESULT_SUCCESS &&
        requests_[last_request_idx_]->lm_id == home_id &&
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      if (verbose_) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cout << "Simulator " << id_ << ": " <<
          "Successful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      ++total_num_lock_success_;
      if (current_request_idx_ < request_size_) {
        SubmitLockRequest();
      } else {
        current_request_idx_ = request_size_ - 1;
        SubmitUnlockRequest();
      }
    //} else if (result == LockManager::RESULT_RETRY &&
        //requests_[last_request_idx_]->lm_id == home_id &&
        //requests_[last_request_idx_]->lock_type == lock_type &&
        //requests_[last_request_idx_]->obj_index == obj_index) {
      //if (verbose_) {
        //pthread_mutex_lock(&LockManager::print_mutex);
        //cout << "Simulator " << id_ << ": " <<
          //"Retrying lock request at LM " <<
          //requests_[last_request_idx_]->lm_id <<
          //" of type " << requests_[last_request_idx_]->lock_type <<
          //" for object " << requests_[last_request_idx_]->obj_index << endl;
        //pthread_mutex_unlock(&LockManager::print_mutex);
      //}
      //current_request_idx_ = last_request_idx_;
      //SubmitLockRequest();
    } else {
      //if (lock_type == LockManager::SHARED &&
          //manager_->GetLockMode() == LockManager::LOCK_REMOTE) {
        //current_request_idx_ = last_request_idx_;
      //} else {
        //current_request_idx_ = last_request_idx_ - 1;
      //}
      current_request_idx_ = last_request_idx_ - 1;
      if (verbose_) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cout << "Simulator " << id_ << ": " <<
          "Unsuccessful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      ++total_num_lock_failure_;
      SubmitUnlockRequest();
    }
  } else if (task == LockManager::TASK_UNLOCK) {
    if (result == LockManager::RESULT_SUCCESS &&
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      if (verbose_) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cout << "Simulator " << id_ << ": " <<
          "Successful unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      ++total_num_unlocks_;
      SubmitUnlockRequest();
    } else if (result == LockManager::RESULT_RETRY) {
      current_request_idx_ = last_request_idx_;
      if (verbose_) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cout << "Simulator " << id_ << ": " <<
          "retrying unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      SubmitUnlockRequest();
    } else {
      if (verbose_) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cout << "Simulator " << id_ << ": " <<
          "Unsuccessful unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
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
  return total_time_taken_to_lock_ / (double)total_num_locks_;
}

double LockSimulator::Get99PercentileLockTime() {
  sort(lock_times_, lock_times_ + total_num_locks_);
  size_t pos = (size_t)((double)total_num_locks_ * 0.99);
  return lock_times_[pos];
}

}}
