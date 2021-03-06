#include "lock_simulator.h"

namespace rdma {
namespace proto {

double LockSimulator::kMaxBackoff = 100000;
double LockSimulator::kBaseBackoff = 10;
long LockSimulator::kReadLockTime = 0;

LockSimulator::LockSimulator(LockManager* manager, int id, int num_nodes,
                             int num_objects, int request_size,
                             string think_time_type, bool do_random_backoff)
    : manager_(manager),
      id_(id),
      num_nodes_(num_nodes),
      num_objects_(num_objects),
      request_size_(request_size),
      max_request_size_(request_size),
      think_time_type_(think_time_type),
      do_random_backoff_(do_random_backoff),
      trx_count_(0),
      backoff_count_(0),
      seq_count_(0),
      think_time_duration_(0),
      false_positives_(0),
      lock_count_(0),
      d2lm_fail_rate_(0) {
  latency_.reserve(kTransactionMax);
  contention_latency_.reserve(kTransactionMax);
  backoff_time_.reserve(kTransactionMax);
  backoff_latency_.reserve(kTransactionMax);
  stats_.reserve(kTransactionMax);
  rng_.seed();
}

LockSimulator::~LockSimulator() {}

int LockSimulator::GetID() const { return id_; }

void LockSimulator::SetThinkTimeDuration(int duration) {
  think_time_duration_ = duration;
}

void LockSimulator::SetBaseBackoff(double backoff) { kBaseBackoff = backoff; }

void LockSimulator::SetMaxBackoff(double backoff) { kMaxBackoff = backoff; }

void LockSimulator::SetReadLockTime(long time) { kReadLockTime = time; }

void LockSimulator::run() {
  // Initialize requests array if empty.
  if (requests_.empty()) {
    temp_requests_ = new LockRequest*[max_request_size_];
    for (int i = 0; i < max_request_size_; ++i) {
      std::unique_ptr<LockRequest> request(new LockRequest);
      temp_requests_[i] = request.get();
      requests_.push_back(std::move(request));
    }
  }

  // Initialize think time generator.
  ThinkTimeGenerator think_time_gen(think_time_type_, think_time_duration_);

  bool job_done = false;
  {
    Poco::Mutex::ScopedLock lock(mutex_);
    is_done_ = false;
    job_done = is_done_;
  }

  Poco::Timestamp job_start;
  auto job_start_time = job_start.epochTime();
  cerr << "job start time (" << id_
       << ") = " << asctime(localtime(&job_start_time)) << endl;

  d2lm_fail_rate_ = manager_->GetD2LMFailRate();
  // Keep requesting locks until done.
  while (!job_done) {
    CreateRequest();

    // lock.
    int i = 0;
    int contention_count = 0;
    int contention_count2 = 0;
    int contention_count3 = 0;
    int contention_count4 = 0;
    int contention_count5 = 0;
    int contention_count6 = 0;
    int attempt = 0;
    int time_spent_backoff = 0;
    bool backoff_done = false;
    bool lock_success = true;
    bool is_failed = false;
    Poco::Timestamp lock_start;
    last_lock_start_time_.update();
    LockMode lock_mode = manager_->GetLockMode();
    int d2lm_deadlock_limit = manager_->GetD2LMDeadlockLimit();
    while (i < request_size_) {
      last_lock_try_time_.update();
      requests_[i]->contention_count = 0;
      requests_[i]->contention_count2 = 0;
      requests_[i]->contention_count3 = 0;
      requests_[i]->contention_count4 = 0;
      requests_[i]->contention_count5 = 0;
      requests_[i]->contention_count6 = 0;
      requests_[i]->is_failed = false;
      auto& lock_result = manager_->Lock(*requests_[i]);
      if (lock_result.isSpecified()) {
        auto lock_future = lock_result.value()->get_future();
        LockResultInfo result_info = lock_future.get();
        if (result_info.result == SUCCESS ||
            result_info.result == SUCCESS_FROM_QUEUED) {
          if (requests_[i]->wait_time > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(requests_[i]->wait_time));
          }
          // if (lock_mode == REMOTE_D2LM_V2 &&
          // requests_[i]->wait_time > LEASE_EXTENSION_TIME) {
          // lock_success = false;
          // is_failed = true;
          //++contention_count2;
          //}
          ++i;
        } else if (result_info.result == RETRY) {
          if (do_random_backoff_) {
            time_spent_backoff += PerformRandomBackoff(attempt);
            ++backoff_count_;
          }
        } else {
          // Handle queued case.
          bool timeout = false;
          if (result_info.result == QUEUED) {
            ++contention_count;
            auto lock_future = manager_->GetLockResult(id_)->get_future();
            auto future_status =
                lock_future.wait_for(std::chrono::milliseconds(100));
            if (future_status == std::future_status::timeout) {
              // sleep(5);
              manager_->SetLockStatusInvalid(id_, requests_[i]->lm_id,
                                             requests_[i]->obj_index);
              // Revert acquired + queued locks.
              RevertLocks(i);
              i = 0;
              timeout = true;
            } else if (future_status == std::future_status::ready) {
              result_info = lock_future.get();
            } else {
              cerr << "'deferred' future_status should not happen!" << endl;
              exit(ERROR_INVALID_FUTURE_STATUS);
            }
          }
          // if locks have been failed, revert acquired locks and perform random
          // backoff.
          if (timeout || result_info.result == FAILURE) {
            if (i > 0) {
              // Revert only acquired locks.
              RevertLocks(--i);
            }
            time_spent_backoff += PerformRandomBackoff(attempt);
            ++backoff_count_;
            backoff_done = true;
            i = 0;
          } else if (result_info.result == SUCCESS ||
                     result_info.result == SUCCESS_FROM_QUEUED) {
            if (requests_[i]->wait_time > 0) {
              std::this_thread::sleep_for(
                  std::chrono::milliseconds(requests_[i]->wait_time));
            }
            // if (lock_mode == REMOTE_D2LM_V2 &&
            // requests_[i]->wait_time > LEASE_EXTENSION_TIME) {
            // lock_success = false;
            // is_failed = true;
            //++contention_count2;
            //}
            ++i;
          }
        }
        contention_count += result_info.stat.contention_count;
        contention_count2 += result_info.stat.contention_count2;
        contention_count3 += result_info.stat.contention_count3;
        contention_count4 += result_info.stat.contention_count4;
        contention_count5 += result_info.stat.contention_count5;
        contention_count6 += result_info.stat.contention_count6;
      } else {
        // This mean node has died.
        --i;
        lock_success = false;
        break;
      }
    }
    Poco::Timestamp::TimeDiff latency = lock_start.elapsed();
    if (!is_failed && d2lm_fail_rate_ > 0 &&
        rng_.nextDouble() < d2lm_fail_rate_) {
      is_failed = true;
      ++contention_count2;
    }
    if (!is_failed && lock_mode == REMOTE_D2LM_V2 && latency >= 10000) {
      // is_failed = true;
      ++contention_count6;
    }
    if (requests_[0]->wait_time > 0) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(requests_[0]->wait_time));
    }
    if (lock_success) {
      latency_.push_back(latency);  // microseconds.
      LockStat s(contention_count, contention_count2, contention_count3,
                 contention_count4, contention_count5, contention_count6);
      stats_.push_back(s);

      if (!is_failed) ++trx_count_;
      lock_count_ += request_size_;
      if (time_spent_backoff > 0) {
        backoff_time_.push_back(time_spent_backoff);
      }
      if (backoff_done) {
        backoff_latency_.push_back(latency);
      } else if ((contention_count + contention_count2 + contention_count3 +
                  contention_count4 + contention_count5 + contention_count6) >
                 0) {
        contention_latency_.push_back(latency);
      }

      // Enforce think time.
      // int think_time = think_time_gen.GetTime();
      // if (think_time == -1) {
      // cerr << "Unknown think time generator: " << think_time_type_ << endl;
      // exit(ERROR_UNKNOWN_THINK_TIME_TYPE);
      //} else if (think_time > 0) {
      //// Sleeps for 'think_time' microseconds.
      //// Note that the accuracy of this sleep is not guaranteed.
      // std::this_thread::sleep_for(std::chrono::microseconds(think_time));
      //}
      i = request_size_ - 1;
    }

    // if (latency > d2lm_deadlock_limit) {
    // for (int j = 0; j < request_size_; ++j) {
    // requests_[j]->is_failed = true;
    //}
    //}
    if (is_failed) {
      for (int j = 0; j < request_size_; ++j) {
        requests_[j]->is_failed = true;
      }
    }

    // unlock.
    while (i >= 0) {
      if (manager_->GetLockMode() == REMOTE_DRTM &&
          requests_[i]->lock_type == SHARED) {
        --i;
        continue;
      }
      if (requests_[i]->lock_type == SHARED_EXTEND ||
          requests_[i]->lock_type == EXCLUSIVE_EXTEND) {
        --i;
        continue;
      }
      if (!requests_[i]->is_failed) {
        //(lock_mode == REMOTE_D2LM_V2 &&
        // manager_->IsD2LMUserDoReset(requests_[i]->lm_id, id_,
        // requests_[i]->obj_index))) {
        auto& lock_result = manager_->Unlock(*requests_[i]);
        if (lock_result.isSpecified()) {
          auto lock_future = lock_result.value()->get_future();
          LockResultInfo result_info = lock_future.get();
          if (result_info.result == SUCCESS) {
            --i;
          } else if (result_info.result == FAILURE) {
            cerr << "Unlock failure (1)." << endl;
            exit(ERROR_UNLOCK_FAIL);
          }
        } else {
          // This means target node has been failed.
          --i;
          continue;
        }
      } else {
        --i;
      }
    }
    // Enforce think time.
    int think_time = think_time_gen.GetTime();
    if (think_time == -1) {
      cerr << "Unknown think time generator: " << think_time_type_ << endl;
      exit(ERROR_UNKNOWN_THINK_TIME_TYPE);
    } else if (think_time > 0) {
      // Sleeps for 'think_time' microseconds.
      // Note that the accuracy of this sleep is not guaranteed.
      std::this_thread::sleep_for(std::chrono::microseconds(think_time));
    }

    // Check whether the simulator has received the stop signal.
    {
      Poco::Mutex::ScopedLock lock(mutex_);
      job_done = is_done_;
    }
  }
  cout << "Simulator " << id_ << " done" << endl;
  std::flush(cout);
}

void LockSimulator::RevertLocks(int& index) {
  while (index >= 0) {
    if (manager_->GetLockMode() == REMOTE_DRTM &&
        requests_[index]->lock_type == SHARED) {
      --index;
      continue;
    }
    auto& lock_result = manager_->Unlock(*requests_[index]);
    if (lock_result.isSpecified()) {
      auto lock_future = lock_result.value()->get_future();
      LockResultInfo result_info = lock_future.get();
      if (result_info.result == SUCCESS) {
        --index;
      } else if (result_info.result == FAILURE) {
        cerr << "Unlock failure. (3)" << endl;
        exit(ERROR_UNLOCK_FAIL);
      }
    } else {
      cerr << "Unlock failure. (4)" << endl;
      exit(ERROR_UNLOCK_FAIL);
    }
  }
  index = -1;
}

int LockSimulator::PerformRandomBackoff(int& attempt) {
  if (!do_random_backoff_) return 0;

  double sleep =
      rng_.nextDouble() *
      std::min((double)kMaxBackoff, kBaseBackoff * pow(2.0, attempt++));

  std::this_thread::sleep_for(
      std::chrono::microseconds((uint32_t)(std::ceil(sleep))));

  return sleep;
}

void LockSimulator::Stop() {
  Poco::Mutex::ScopedLock lock(mutex_);
  is_done_ = true;
  auto time = last_lock_start_time_.epochTime();
  auto time2 = last_lock_try_time_.epochTime();
  cerr << "last lock start time (" << id_ << ") = " << asctime(localtime(&time))
       << endl;
  cerr << "last lock try time (" << id_ << ") = " << asctime(localtime(&time2))
       << endl;
  cerr << "Backoff count = " << backoff_count_ << endl;
}

uint64_t LockSimulator::GetCount() const { return trx_count_; }

uint64_t LockSimulator::GetLockCount() const { return lock_count_; }

uint64_t LockSimulator::GetBackoffCount() const { return backoff_count_; }

uint64_t LockSimulator::GetCountWithContention() const {
  return contention_latency_.size();
}

uint64_t LockSimulator::GetCountWithBackoff() const {
  return backoff_latency_.size();
}

void LockSimulator::SortLatency() {
  std::sort(latency_.begin(), latency_.end());
  std::sort(contention_latency_.begin(), contention_latency_.end());
  std::sort(backoff_latency_.begin(), backoff_latency_.end());
}

double LockSimulator::GetAverageLatency() const {
  if (latency_.empty()) return 0;
  double sum = 0;
  for (auto latency : latency_) {
    sum += latency;
  }
  return sum / (double)latency_.size();
}

double LockSimulator::GetAverageBackoffTime() const {
  if (backoff_time_.empty()) return 0;
  double sum = 0;
  for (auto backoff : backoff_time_) {
    sum += backoff;
  }
  return sum / (double)backoff_time_.size();
}

double LockSimulator::GetAverageContentionCount() const {
  if (stats_.empty()) return 0;
  double sum = 0;
  for (auto s : stats_) {
    sum += s.contention_count;
  }
  // return sum / (double)stats_.size();
  return sum;
}

double LockSimulator::GetAverageContentionCount2() const {
  if (stats_.empty()) return 0;
  double sum = 0;
  for (auto s : stats_) {
    sum += s.contention_count2;
  }
  // return sum / (double)stats_.size();
  return sum;
}

double LockSimulator::GetAverageContentionCount3() const {
  if (stats_.empty()) return 0;
  double sum = 0;
  for (auto s : stats_) {
    sum += s.contention_count3;
  }
  // return sum / (double)stats_.size();
  return sum;
}

double LockSimulator::GetAverageContentionCount4() const {
  if (stats_.empty()) return 0;
  double sum = 0;
  for (auto s : stats_) {
    sum += s.contention_count4;
  }
  // return sum / (double)stats_.size();
  return sum;
}
double LockSimulator::GetAverageContentionCount5() const {
  if (stats_.empty()) return 0;
  double sum = 0;
  for (auto s : stats_) {
    sum += s.contention_count5;
  }
  // return sum / (double)stats_.size();
  return sum;
}
double LockSimulator::GetAverageContentionCount6() const {
  if (stats_.empty()) return 0;
  double sum = 0;
  for (auto s : stats_) {
    sum += s.contention_count6;
  }
  // return sum / (double)stats_.size();
  return sum;
}

double LockSimulator::Get99PercentileLatency() const {
  if (latency_.empty()) return 0;
  // Assumes that latency_ has been sorted.
  return latency_[floor(latency_.size() * 0.99)];
}

double LockSimulator::Get999PercentileLatency() const {
  if (latency_.empty()) return 0;
  // Assumes that latency_ has been sorted.
  return latency_[floor(latency_.size() * 0.999)];
}

uint64_t LockSimulator::GetMaxLatency() const {
  if (latency_.empty()) return 0;
  // Assumes that latency_ has been sorted.
  return latency_.back();
}

uint64_t LockSimulator::GetFalsePositives() const { return false_positives_; }

double LockSimulator::GetAverageLatencyWithContention() const {
  if (contention_latency_.empty()) return 0;
  double sum = 0;
  for (auto latency : contention_latency_) {
    sum += latency;
  }
  return sum / (double)contention_latency_.size();
}

double LockSimulator::Get99PercentileLatencyWithContention() const {
  if (contention_latency_.empty()) return 0;
  // Assumes that contention_latency_ has been sorted.
  return contention_latency_[floor(contention_latency_.size() * 0.99)];
}

double LockSimulator::Get999PercentileLatencyWithContention() const {
  if (contention_latency_.empty()) return 0;
  // Assumes that contention_latency_ has been sorted.
  return contention_latency_[floor(contention_latency_.size() * 0.999)];
}

double LockSimulator::GetAverageLatencyWithBackoff() const {
  if (backoff_latency_.empty()) return 0;
  double sum = 0;
  for (auto latency : backoff_latency_) {
    sum += latency;
  }
  return sum / (double)backoff_latency_.size();
}

double LockSimulator::Get99PercentileLatencyWithBackoff() const {
  if (backoff_latency_.empty()) return 0;
  // Assumes that backoff_latency_ has been sorted.
  return backoff_latency_[floor(backoff_latency_.size() * 0.99)];
}

double LockSimulator::Get999PercentileLatencyWithBackoff() const {
  if (backoff_latency_.empty()) return 0;
  // Assumes that backoff_latency_ has been sorted.
  return backoff_latency_[floor(backoff_latency_.size() * 0.999)];
}

// Create requests.
void LockSimulator::CreateRequest() {
  // Generate random requests.
  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->seq_no = seq_count_++;
    requests_[i]->user_id = id_;
    requests_[i]->owner_node_id = manager_->GetID();
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = 1 + (rng_.next() % num_nodes_);
    // requests_[i]->lm_id = 1;
    requests_[i]->obj_index = rng_.next() % num_objects_;
    requests_[i]->lock_type = (rng_.nextBool()) ? SHARED : EXCLUSIVE;
    // requests_[i]->lock_type = EXCLUSIVE;
    requests_[i]->contention_count = 0;
  }
}

}  // namespace proto
}  // namespace rdma
