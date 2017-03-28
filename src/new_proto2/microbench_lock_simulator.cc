#include "microbench_lock_simulator.h"

namespace rdma { namespace proto {

MicrobenchLockSimulator::MicrobenchLockSimulator(LockManager* manager, uint32_t id,
    uint32_t home_id,
    int workload_type, int num_manager, int num_tx, int num_objects, double contention_index,
    long seed, bool verbose,
    bool measure_lock_time, int lock_mode,
    bool transaction_delay, double transaction_delay_min,
    double transaction_delay_max, int min_backoff_time,
    int max_backoff_time, int sleep_time, int think_time) {
  manager_                           = manager;
  id_                                = id;
  home_id_                           = home_id;
  num_manager_                       = num_manager;
  num_tx_                            = num_tx;
  num_objects_                       = num_objects;
  contention_index_                  = contention_index;
  seed_                              = seed;
  verbose_                           = verbose;
  measure_lock_time_                 = measure_lock_time;
  lock_mode_                         = lock_mode;
  transaction_delay_                 = transaction_delay;
  transaction_delay_min_             = transaction_delay_min;
  transaction_delay_max_             = transaction_delay_max;
  default_backoff_time_              = min_backoff_time;
  current_backoff_time_              = default_backoff_time_;
  max_backoff_time_                  = max_backoff_time;
  local_manager_id_                  = manager_->GetID();
  max_request_size_                  = 10;
  request_size_                      = 10;
  is_tx_failed_                      = false;
  total_num_locks_                   = 0;
  total_num_unlocks_                 = 0;
  total_num_lock_success_            = 0;
  total_num_lock_failure_            = 0;
  total_num_timeouts_                = 0;
  total_num_lock_contention_         = 0;
  total_num_lock_success_with_retry_ = 0;
  sum_retry_when_success_            = 0;
  sum_index_when_timeout_            = 0;
  total_time_taken_to_lock_          = 0;
  count_                             = 0;
  last_count_                        = 0;
  last_seq_no_                       = 0;
  seq_count_                         = 0;
  is_backing_off_                    = false;
  is_tx_failed_                      = false;
  is_tx_timed_out_                   = false;
  sleep_time_                        = sleep_time;
  think_time_                        = think_time;
  workload_type_                     = workload_type;
  num_hot_objects_                   = (int) (1.0 / contention_index);
}


void MicrobenchLockSimulator::Run() {

  seed_ += id_;
  seed2_ += id_;
  backoff_seed_ += id_;
  srand48(seed_+id_);
  is_tx_failed_ = false;
  is_tx_timed_out_ = false;

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
}

void MicrobenchLockSimulator::StartLockRequests() {

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
  //state_ = STATE_IDLE;
  //while (state_ != STATE_DONE) {
    //switch (state_) {
      //case STATE_IDLE:
        //state_ = STATE_WAIT;
        //CreateLockRequests();
        //break;
      //case STATE_LOCKING:
        //state_ = STATE_WAIT;
        //SubmitLockRequest();
        //break;
      //case STATE_UNLOCKING:
        //state_ = STATE_WAIT;
        //SubmitUnlockRequest();
        //break;
      //default:
        //break;
    //}
  //}
  //cout << "STATE DONE" << endl;
}

void MicrobenchLockSimulator::Generate() {
  int target_manager_id = rand_r(&seed_) % num_manager_;
  // set first request for hot object
  requests_[0]->lm_id     = target_manager_id;
  requests_[0]->lock_type = EXCLUSIVE;
  requests_[0]->obj_index = rand_r(&seed_) % num_hot_objects_;
  requests_[0]->task      = TASK_LOCK;

  obj_index_set_.clear();
  while (obj_index_set_.size() != request_size_ - 1) {
    obj_index_set_.insert(num_hot_objects_ +
        (rand_r(&seed_) % (num_objects_ - num_hot_objects_)));
  }

  set<int>::iterator it;
  int i = 1;
  for (it = obj_index_set_.begin(); it != obj_index_set_.end(); ++it) {
    requests_[i]->lm_id     = target_manager_id;
    requests_[i]->lock_type = 1 + (rand_r(&seed_) % 2);
    requests_[i]->obj_index = *it;
    requests_[i]->task      = TASK_LOCK;
    ++i;
  }
}

void MicrobenchLockSimulator::CreateLockRequests() {

  if (count_ >= num_tx_) {
    if (verbose_)
      cout << "Tx count of " << num_tx_ << " has reached. Terminating.";
    clock_gettime(CLOCK_MONOTONIC, &current_time_);
    double dt = ((double)current_time_.tv_sec *1.0e+9 + current_time_.tv_nsec) -
    ((double)start_time_.tv_sec * 1.0e+9 + start_time_.tv_nsec);
    time_taken_ = dt;
    ChangeState(STATE_DONE);
    return;
  }
  if (requests_.empty()) {
    for (int i = 0; i < max_request_size_; ++i) {
       LockRequest* request = new LockRequest;
       requests_.push_back(request);
    }
  }

  struct timespec before, after;
  double time_taken;

  if (!is_tx_failed_) {
    clock_gettime(CLOCK_MONOTONIC, &before);
    Generate();
    clock_gettime(CLOCK_MONOTONIC, &after);
    time_taken = ((double)after.tv_sec * 1e+9 +
        (double)after.tv_nsec) - ((double)before.tv_sec * 1e+9 +
          (double)before.tv_nsec);
    if (measure_lock_time_)
      clock_gettime(CLOCK_MONOTONIC, &start_lock_);
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

    ++total_num_timeouts_;
    sum_index_when_timeout_ += num_lock_acquired_till_timeout_;
  }

  last_request_idx_ = 0;
  current_request_idx_ = 0;
  retry_ = 0;
  SubmitLockRequest();
}

void MicrobenchLockSimulator::SubmitLockRequest() {

  int lock_result = RESULT_SUCCESS;

  pthread_mutex_lock(&time_mutex_);
  clock_gettime(CLOCK_MONOTONIC, &last_lock_time_);
  pthread_mutex_unlock(&time_mutex_);

  // enforce think time
  usleep(think_time_);

  int ret = 0;
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
    //if (measure_lock_time_)
      //clock_gettime(CLOCK_MONOTONIC, &start_lock_);
    requests_[current_request_idx_]->task = TASK_LOCK;
    requests_[current_request_idx_]->seq_no = seq_count_;
    last_seq_no_ = seq_count_;
    ++seq_count_;
    last_request_idx_ = current_request_idx_;
    ++current_request_idx_;
    ret = manager_->Lock(
        requests_[last_request_idx_]->seq_no,
        id_,
        requests_[last_request_idx_]->lm_id,
        requests_[last_request_idx_]->lock_type,
        requests_[last_request_idx_]->obj_index);
    measure_time_out_ = true;
    ++total_num_locks_;
  } else {
    SimulateTransactionDelay();
  }
  pthread_mutex_unlock(&lock_mutex_);
  if (ret == LOCAL_LOCK_PASS) {
    this->NotifyResult(
        requests_[last_request_idx_]->seq_no,
        LockManager::TASK_LOCK,
        requests_[last_request_idx_]->lock_type,
        requests_[last_request_idx_]->obj_index,
        LockManager::RESULT_SUCCESS
        );
  } else if (ret == LOCAL_LOCK_FAIL) {
    this->NotifyResult(
        requests_[last_request_idx_]->seq_no,
        LockManager::TASK_LOCK,
        requests_[last_request_idx_]->lock_type,
        requests_[last_request_idx_]->obj_index,
        RESULT_LOCAL_FAILURE
        );
  }
}

void MicrobenchLockSimulator::SubmitUnlockRequest() {

  int lock_result = RESULT_SUCCESS;
  measure_time_out_ = false;

  restart_ = false;

  int ret = 0;
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
    ret = manager_->Unlock(
        requests_[last_request_idx_]->seq_no,
        id_,
        requests_[last_request_idx_]->lm_id,
        requests_[last_request_idx_]->lock_type,
        requests_[last_request_idx_]->obj_index);
  } else {
    ChangeState(STATE_IDLE);
  }
  pthread_mutex_unlock(&lock_mutex_);
  if (ret == LOCAL_LOCK_PASS) {
    this->NotifyResult(
        requests_[last_request_idx_]->seq_no,
        LockManager::TASK_UNLOCK,
        requests_[last_request_idx_]->lock_type,
        requests_[last_request_idx_]->obj_index,
        LockManager::RESULT_SUCCESS
        );
  }
}

}}
