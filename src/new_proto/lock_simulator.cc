#include "lock_simulator.h"

namespace rdma { namespace proto {

LockSimulator::LockSimulator(LockManager* manager, int id, int num_manager,
    int num_lock_object, uint64_t num_lock_request) {
  manager_                  = manager;
  id_                       = id;
  num_manager_              = num_manager;
  num_lock_object_          = num_lock_object;
  seed_                     = 1;
  state_                    = LockSimulator::STATE_IDLE;
  max_request_size_         = 1;
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
  count_limit_              = num_lock_request;
  seq_count_                = 0;
  count_                    = 0;
  request_size_             = 1;
  last_seq_no_              = 0;
  is_backing_off_           = false;
  think_time_               = 10;

  lock_times_ = new double[MAX_LOCK_REQUESTS];

  pthread_mutex_init(&mutex_, NULL);
  pthread_mutex_init(&time_mutex_, NULL);
  pthread_mutex_init(&lock_mutex_, NULL);
  pthread_mutex_init(&state_mutex_, NULL);
  pthread_cond_init(&state_cond_, NULL);
}

LockSimulator::LockSimulator(LockManager* manager, int id, int num_manager,
    int num_lock_object, uint64_t num_tx, int num_request_per_tx, long seed, bool verbose,
    bool measure_lock_time, int workload_type, int lock_mode,
    double local_percentage, double shared_lock_ratio, bool transaction_delay,
    double transaction_delay_min, double transaction_delay_max,
    int min_backoff_time, int max_backoff_time, double time_out_threshold, double* custom_cdf) {
  manager_                  = manager;
  id_                       = id;
  num_manager_              = num_manager;
  num_lock_object_          = num_lock_object;
  state_                    = LockSimulator::STATE_IDLE;
  //request_size_           = num_lock_request;
  seed_                     = seed;
  seed2_                    = seed;
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
  measure_time_out_         = false;
  is_all_local_             = false;
  transaction_delay_        = transaction_delay;
  transaction_delay_min_    = transaction_delay_min;
  transaction_delay_max_    = transaction_delay_max;
  local_manager_id_         = manager_->GetID();
  count_limit_              = num_tx;
  count_                    = 0;
  last_count_               = 0;
  max_request_size_         = num_request_per_tx;
  max_backoff_time_         = max_backoff_time;
  default_backoff_time_     = min_backoff_time;
  current_backoff_time_     = default_backoff_time_;
  backoff_seed_             = seed_;
  time_out_                 = time_out_threshold;
  time_out_seed_            = seed_ + id;
  last_seq_no_              = 0;
  seq_count_                = 0;
  is_backing_off_           = false;
  think_time_               = 10;

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
  pthread_mutex_init(&time_mutex_, NULL);
  pthread_mutex_init(&lock_mutex_, NULL);
  pthread_mutex_init(&state_mutex_, NULL);
  pthread_cond_init(&state_cond_, NULL);
}

LockSimulator::~LockSimulator() {
  pthread_mutex_destroy(&mutex_);
  if (lock_times_) {
    delete[] lock_times_;
  }
}

void LockSimulator::Run() {
  //srand(seed_+id_);
  seed_ += id_;
  seed2_ += id_;
  backoff_seed_ += id_;
  srand48(seed_+id_);
  is_tx_failed_ = false;

  InitializeCDF();

  if (lock_mode_ == LOCK_REMOTE_NOTIFY || lock_mode_ == LOCK_PROXY_QUEUE) {
    int ret = pthread_create(&timeout_thread_, NULL, &LockSimulator::CheckTimeOut, (void*)this);
    if (ret) {
      cerr << "LockSimulator::pthread_create(): " << strerror(ret) << endl;
      exit(-1);
    }
  }

  clock_gettime(CLOCK_MONOTONIC, &start_time_);

  ChangeState(STATE_IDLE);
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
    case WORKLOAD_UNIFORM_RANDOM_LENGTH:
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

  int current_state = STATE_WAIT;
  while (true) {
    pthread_mutex_lock(&state_mutex_);
    while (state_ == STATE_WAIT || state_ == STATE_QUEUED) {
      pthread_cond_wait(&state_cond_, &state_mutex_);
    }
    current_state = state_;
    if (current_state != STATE_QUEUED) {
      state_ = STATE_WAIT;
    }
    pthread_mutex_unlock(&state_mutex_);
    switch (current_state) {
      case STATE_IDLE:
        CreateLockRequests();
        break;
      case STATE_LOCKING:
        SubmitLockRequest();
        break;
      case STATE_UNLOCKING:
        SubmitUnlockRequest();
        break;
      default:
        break;
    }
    if (state_ == STATE_DONE)
      break;
  }


//begin_lock:
  //CreateLockRequests();

  //if (is_all_local_ && lock_mode_ == LockManager::LOCK_LOCAL &&
      //state_ != LockSimulator::STATE_DONE) {
    //goto begin_lock;
  //}
  //if (lock_mode_ == LockManager::LOCK_LOCAL && count_ < count_limit_) {
    //goto begin_lock;
  //}
}

int LockSimulator::GetState() const {
  return state_;
}

void LockSimulator::ChangeState(int state) {
  pthread_mutex_lock(&state_mutex_);
  state_ = state;
  pthread_cond_signal(&state_cond_);
  pthread_mutex_unlock(&state_mutex_);
}

void LockSimulator::CreateLockRequests() {
  //if (difftime(current_time_, start_time_) >= duration_) {
    //if (verbose_)
      //cout << "Time limit of " << duration_ << " has reached. Terminating.";
    //state_ = LockSimulator::STATE_DONE;
    //return;
  //}
  if (count_ >= count_limit_) {
    if (verbose_)
      cout << "Lock request count of " << count_limit_ << " has reached. Terminating.";
    clock_gettime(CLOCK_MONOTONIC, &current_time_);
    double dt = ((double)current_time_.tv_sec *1.0e+9 + current_time_.tv_nsec) -
    ((double)start_time_.tv_sec * 1.0e+9 + start_time_.tv_nsec);
    time_taken_ = dt;
    ChangeState(STATE_DONE);
    return;
  }

  request_size_ = max_request_size_;

  if (workload_type_ == WORKLOAD_UNIFORM_RANDOM_LENGTH) {
    request_size_ = 1 + (rand_r(&seed_) % max_request_size_);
  }

  if (requests_.empty()) {
    for (int i = 0; i < max_request_size_; ++i) {
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

  if (!is_tx_failed_) {
    for (int i = 0; i < request_size_; ++i) {
      if (workload_type_ == WORKLOAD_MIXED ||
          workload_type_ == WORKLOAD_ALL_LOCAL) {
        if (is_all_local_) {
          requests_[i]->lm_id = local_manager_id_;
        } else {
          requests_[i]->lm_id = rand_r(&seed_) % num_manager_;
        }
      } else {
        double val = drand48();
        for (int j = 0; j < num_manager_; ++j) {
          if (val <= cdf_[j]) {
            requests_[i]->lm_id = j;
            break;
          }
        }
      }
      requests_[i]->obj_index = rand_r(&seed_) % num_lock_object_;
      if (drand48() < shared_lock_ratio_) {
        requests_[i]->lock_type = LockManager::SHARED;
      } else {
        requests_[i]->lock_type = LockManager::EXCLUSIVE;
      }
      requests_[i]->task = LockManager::TASK_LOCK;

      bool conflict = false;
      for (int j = 0; j < i; ++j) {
        if (requests_[i]->lm_id == requests_[j]->lm_id &&
            requests_[i]->obj_index == requests_[j]->obj_index) {
          conflict = true;
          break;
        }
      }
      if (conflict)
        --i;
    }

    is_all_local_ = true;
    for (int i = 0; i < request_size_; ++i) {
      if (requests_[i]->lm_id != local_manager_id_) {
        is_all_local_ = false;
        break;
      }
    }
  } else {
    // if tx failed & backoff time exists, randomly backoff
    if (max_backoff_time_ > 0) {
      if (last_count_ == count_) {
        current_backoff_time_ = current_backoff_time_ * 2;
        if (current_backoff_time_ > max_backoff_time_) {
          current_backoff_time_ = max_backoff_time_;
        }
      } else {
        current_backoff_time_ = default_backoff_time_;
      }
      last_count_     = count_;
      int amount      = rand_r(&backoff_seed_) % current_backoff_time_;
      is_backing_off_ = true;
      usleep(amount);
      is_backing_off_ = false;
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
  retry_ = 0;

  //if (is_all_local_ && lock_mode_ == LockManager::LOCK_LOCAL) {
    //SubmitLockRequestLocal();
  //} else {
    //SubmitLockRequest();
  //}
  SubmitLockRequest();
}

void LockSimulator::SubmitLockRequest() {

  int lock_result = LockManager::RESULT_SUCCESS;
  local_lock_count_ = 0;

  pthread_mutex_lock(&time_mutex_);
  clock_gettime(CLOCK_MONOTONIC, &last_lock_time_);
  pthread_mutex_unlock(&time_mutex_);

  // enforce think time
  usleep(think_time_);
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

  pthread_mutex_lock(&lock_mutex_);
  if (current_request_idx_ < request_size_) {
    if (verbose_) {
      pthread_mutex_lock(&PRINT_MUTEX);
      cout << "(REMOTE) Simulator " << id_ << ": " << "Sending lock request #" <<
        current_request_idx_ <<
        " at LM " <<
        requests_[current_request_idx_]->lm_id <<
        " of type " << requests_[current_request_idx_]->lock_type <<
        " for object " << requests_[current_request_idx_]->obj_index << endl;
      pthread_mutex_unlock(&PRINT_MUTEX);
    }
    if (measure_lock_time_)
      clock_gettime(CLOCK_MONOTONIC, &start_lock_);
    requests_[current_request_idx_]->task = TASK_LOCK;
    requests_[current_request_idx_]->seq_no = seq_count_;
    last_seq_no_ = seq_count_;
    ++seq_count_;
    last_request_idx_ = current_request_idx_;
    ++current_request_idx_;
    manager_->Lock(
        requests_[last_request_idx_]->seq_no,
        id_,
        requests_[last_request_idx_]->lm_id,
        requests_[last_request_idx_]->lock_type,
        requests_[last_request_idx_]->obj_index);
    measure_time_out_ = true;
    ++total_num_locks_;
  } else {
    SimulateTransactionDelay();
    //--current_request_idx_;
    //SubmitUnlockRequest();
  }
  pthread_mutex_unlock(&lock_mutex_);
}

void LockSimulator::SubmitUnlockRequest() {

  int lock_result = LockManager::RESULT_SUCCESS;
  local_unlock_count_ = 0;
  measure_time_out_ = false;

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

  pthread_mutex_lock(&lock_mutex_);
  if (current_request_idx_ >= 0) {
    if (verbose_) {
      pthread_mutex_lock(&PRINT_MUTEX);
      cout << "(REMOTE) Simulator " << id_ << ": " << "Sending unlock request #" <<
        current_request_idx_ <<
        " at LM " <<
        requests_[current_request_idx_]->lm_id <<
        " of type " << requests_[current_request_idx_]->lock_type <<
        " for object " << requests_[current_request_idx_]->obj_index << endl;
      pthread_mutex_unlock(&PRINT_MUTEX);
    }
    pthread_mutex_lock(&time_mutex_);
    clock_gettime(CLOCK_MONOTONIC, &last_lock_time_);
    pthread_mutex_unlock(&time_mutex_);
    requests_[current_request_idx_]->task = TASK_UNLOCK;
    requests_[current_request_idx_]->seq_no = seq_count_;
    last_seq_no_ = seq_count_;
    ++seq_count_;
    last_request_idx_ = current_request_idx_;
    --current_request_idx_;
    manager_->Unlock(
        requests_[last_request_idx_]->seq_no,
        id_,
        requests_[last_request_idx_]->lm_id,
        requests_[last_request_idx_]->lock_type,
        requests_[last_request_idx_]->obj_index);
  } else {
    ChangeState(STATE_IDLE);
  }
  //else {
    //if (!restart_)
      //pthread_mutex_unlock(&lock_mutex_);
      //StartLockRequests();
      //return;

    //pthread_mutex_unlock(&lock_mutex_);

    //pthread_mutex_lock(&state_mutex_);
    //state_ = STATE_IDLE;
    //pthread_cond_signal(&state_cond_);
    //pthread_mutex_unlock(&state_mutex_);
    //return;
  //}
  pthread_mutex_unlock(&lock_mutex_);
}

void LockSimulator::SubmitLockRequestLocal() {
  int lock_result;
  pthread_mutex_lock(&state_mutex_);
  state_ = LockSimulator::STATE_LOCKING;
  pthread_mutex_unlock(&state_mutex_);
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


int LockSimulator::NotifyResult(int seq_no, int task, int lock_type, int obj_index,
    int result) {

  pthread_mutex_lock(&mutex_);

  if (requests_[last_request_idx_]->seq_no != seq_no ||
      requests_[last_request_idx_]->task != task ||
      (last_seq_no_ > seq_no && task == TASK_LOCK)) {
    // sequence number or task is different --> ignore
    pthread_mutex_lock(&PRINT_MUTEX);
    cout << "Simulator " << id_ << ", seq no: " <<
      requests_[last_request_idx_]->seq_no << " != " << seq_no;
    pthread_mutex_unlock(&PRINT_MUTEX);
    pthread_mutex_unlock(&mutex_);
    return -1;
  }

  if (task == TASK_LOCK && result == RESULT_QUEUED) {
    if (verbose_) {
      pthread_mutex_lock(&PRINT_MUTEX);
      cout << "Simulator " << id_ << ": " <<
        "Queued lock request at LM " <<
        requests_[last_request_idx_]->lm_id <<
        " of type " << requests_[last_request_idx_]->lock_type <<
        " for object " << requests_[last_request_idx_]->obj_index << endl;
      pthread_mutex_unlock(&PRINT_MUTEX);
    }
    ChangeState(STATE_QUEUED);
    pthread_mutex_unlock(&mutex_);
    return 0;
  }

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
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      if (verbose_) {
        pthread_mutex_lock(&PRINT_MUTEX);
        cout << "Simulator " << id_ << ": " <<
          "Successful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&PRINT_MUTEX);
      }
      ++total_num_lock_success_;
      retry_ = 0;
      if (current_request_idx_ < request_size_) {
        ChangeState(STATE_LOCKING);
      } else {
        ++count_;
        current_request_idx_ = request_size_ - 1;
        is_tx_failed_ = false;
        ChangeState(STATE_UNLOCKING);
      }
    } else if (result == RESULT_RETRY &&
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      ++retry_;
      current_request_idx_ = last_request_idx_;
      if (verbose_) {
        pthread_mutex_lock(&PRINT_MUTEX);
        cout << "Simulator " << id_ << ": " <<
          "(Retry) Unsuccessful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&PRINT_MUTEX);
      }
      // retry
      if (retry_ > LockManager::GetFailRetry()) {
        current_request_idx_ = last_request_idx_ - 1;
        ++total_num_lock_failure_;
        retry_ = 0;
        is_tx_failed_ = true;
        if (last_request_idx_ == 0) {
          ChangeState(STATE_IDLE);
        } else {
          ChangeState(STATE_UNLOCKING);
        }
      } else {
        ChangeState(STATE_LOCKING);
      }
    } else {
      current_request_idx_ = last_request_idx_ - 1;
      if (verbose_) {
        pthread_mutex_lock(&PRINT_MUTEX);
        cout << "Simulator " << id_ << ": " <<
          "(Fail) Unsuccessful lock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index <<
          " (" << lock_type << "," << obj_index << ")" << endl;
        pthread_mutex_unlock(&PRINT_MUTEX);
      }
      ++total_num_lock_failure_;
      is_tx_failed_ = true;
      // retry upon failure
      if (last_request_idx_ == 0) {
        ChangeState(STATE_IDLE);
      } else {
        ChangeState(STATE_UNLOCKING);
      }
    }
  } else if (task == LockManager::TASK_UNLOCK) {
    if (result == LockManager::RESULT_SUCCESS &&
        requests_[last_request_idx_]->lock_type == lock_type &&
        requests_[last_request_idx_]->obj_index == obj_index) {
      if (verbose_) {
        pthread_mutex_lock(&PRINT_MUTEX);
        cout << "Simulator " << id_ << ": " <<
          "Successful unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&PRINT_MUTEX);
      }
      ++total_num_unlocks_;
      if (last_request_idx_ == 0) {
        ChangeState(STATE_IDLE);
      } else {
        ChangeState(STATE_UNLOCKING);
      }
    } else if (result == LockManager::RESULT_RETRY) {
      current_request_idx_ = last_request_idx_;
      if (verbose_) {
        pthread_mutex_lock(&PRINT_MUTEX);
        cout << "Simulator " << id_ << ": " <<
          "retrying exclusive unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&PRINT_MUTEX);
      }
      ChangeState(STATE_UNLOCKING);
    } else {
      if (verbose_) {
        pthread_mutex_lock(&PRINT_MUTEX);
        cout << "Simulator " << id_ << ": " <<
          "Unsuccessful unlock request at LM " <<
          requests_[last_request_idx_]->lm_id <<
          " of type " << requests_[last_request_idx_]->lock_type <<
          " for object " << requests_[last_request_idx_]->obj_index << endl;
        pthread_mutex_unlock(&PRINT_MUTEX);
      }
      ChangeState(STATE_UNLOCKING);
    }
  }
  pthread_mutex_unlock(&mutex_);
}

int LockSimulator::TimeOut() {

  pthread_mutex_lock(&mutex_);
  pthread_mutex_lock(&lock_mutex_);
  if (requests_[last_request_idx_]->task == TASK_LOCK) {
    if (lock_mode_ == LOCK_PROXY_QUEUE ||
        (lock_mode_ == LOCK_REMOTE_NOTIFY && state_ == STATE_QUEUED))
      pthread_mutex_lock(&PRINT_MUTEX);
      cout << "Time Out" << endl;
      pthread_mutex_unlock(&PRINT_MUTEX);

      current_request_idx_ = last_request_idx_;
      is_tx_failed_ = true;
      measure_time_out_ = false;

      ChangeState(STATE_UNLOCKING);
  }
  pthread_mutex_unlock(&lock_mutex_);
  pthread_mutex_unlock(&mutex_);
}

double LockSimulator::GetTimeSinceLastLock() {
  struct timespec now;
  double time_taken;
  clock_gettime(CLOCK_MONOTONIC, &now);
  pthread_mutex_lock(&time_mutex_);
  time_taken = ((double)now.tv_sec * 1e+9 +
      (double)now.tv_nsec) - ((double)last_lock_time_.tv_sec * 1e+9 +
        (double)last_lock_time_.tv_nsec);
  cout << "time = " << time_taken << endl;
  pthread_mutex_unlock(&time_mutex_);

  return time_taken;
}

int LockSimulator::GetID() const {
  return id_;
}
int LockSimulator::GetLockMode() const {
  return lock_mode_;
}

bool LockSimulator::IsLockTimeMeasured() const {
  return measure_lock_time_;
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

uint64_t LockSimulator::GetCount() const {
  return count_;
}

uint64_t LockSimulator::GetSeqCount() const {
  return seq_count_;
}

double LockSimulator::GetAverageTimeTakenToLock() const {
  return total_time_taken_to_lock_ / (double)total_num_locks_;
}

double LockSimulator::GetTimeTaken() const {
  return time_taken_;
}

double LockSimulator::GetTimeOutThreshold() {
  return ((int)rand_r(&time_out_seed_) % (int)time_out_) + 100000000.0;
}

bool LockSimulator::GetMeasureTimeOut() const {
  return measure_time_out_;
}

double LockSimulator::Get99PercentileLockTime() {
  sort(lock_times_, lock_times_ + total_num_locks_);
  size_t pos = (size_t)((double)total_num_locks_ * 0.99);
  return lock_times_[pos];
}

int LockSimulator::GetMaxBackoff() const {
  return max_backoff_time_;
}

int LockSimulator::GetCurrentBackoff() const {
  return current_backoff_time_;
}

bool LockSimulator::IsBackingOff() const {
  return is_backing_off_;
}

void* LockSimulator::CheckTimeOut(void* arg) {
  LockSimulator* simulator = (LockSimulator*)arg;
  int retry = 0;
  uint64_t last_count = simulator->GetSeqCount();
  uint64_t count;
  int backoff = simulator->GetMaxBackoff();
  int lock_mode = simulator->GetLockMode();
  unsigned int backoff_seed = pthread_self();

  while (simulator->GetState() != STATE_DONE) {
    //if (simulator->GetID() == 1) {
      //pthread_mutex_lock(&PRINT_MUTEX);
      //cout << "COUNT = " << simulator->GetCount() << "," << count << "," <<
        //simulator->GetCurrentBackoff() << endl;
      //pthread_mutex_unlock(&PRINT_MUTEX);
    //}
    usleep(100000);
    //cout << simulator->GetCount() << endl;
    if (lock_mode == LOCK_REMOTE_NOTIFY) {
      if (simulator->GetState() != STATE_QUEUED) {
        continue;
      }
    } else if (lock_mode == LOCK_PROXY_QUEUE) {
      if (simulator->GetState() != STATE_IDLE) {
        continue;
      }
    }
    if (simulator->IsBackingOff()) {
      retry = 0;
      continue;
    }
    count = simulator->GetSeqCount();
    if (last_count == count) {
       ++retry;
    } else {
      retry = 0;
      last_count = count;
    }
    if (retry > LockManager::GetFailRetry()) {
      if (simulator->GetMeasureTimeOut()) {
        simulator->TimeOut();
        retry = 0;
      } else {
        retry = 0;
      }
    }
    //if (simulator->GetTimeSinceLastLock() > simulator->GetTimeOutThreshold() &&
        //simulator->GetState() == STATE_LOCKING) {
      //if (simulator->GetMeasureTimeOut()) {
        //simulator->TimeOut();
      //}
    //}
  }
}

}}
