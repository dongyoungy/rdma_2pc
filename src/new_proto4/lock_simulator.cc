#include "lock_simulator.h"

namespace rdma {
namespace proto {

LockSimulator::LockSimulator(LockManager* manager, int num_nodes,
                             int num_objects, int request_size,
                             string think_time_type)
    : manager_(manager),
      num_nodes_(num_nodes),
      num_objects_(num_objects),
      request_size_(request_size),
      think_time_type_(think_time_type),
      count_(0),
      count_with_contention_(0) {
  latency_.reserve(kTransactionMax);
  contention_latency_.reserve(kTransactionMax);
  rng_.seed();
}

LockSimulator::~LockSimulator() {}

void LockSimulator::run() {
  is_done_ = false;

  // Initialize requests array if empty.
  if (requests_.empty()) {
    for (int i = 0; i < request_size_; ++i) {
      std::unique_ptr<LockRequest> request(new LockRequest);
      requests_.push_back(std::move(request));
    }
  }

  // Initialize think time generator.
  ThinkTimeGenerator think_time_gen(think_time_type_);

  // Keep requesting locks until done.
  while (!is_done_) {
    CreateRequest();

    // lock.
    int i = 0;
    int contention_count = 0;
    Poco::Timestamp lock_start;
    while (i < request_size_) {
      auto& lock_result = manager_->Lock(*requests_[i]);
      if (lock_result.isSpecified()) {
        auto lock_future = lock_result.value()->get_future();
        LockResultInfo result_info = lock_future.get();
        if (result_info.result == SUCCESS) {
          ++i;
        }
        contention_count += result_info.contention_count;
      } else {
        exit(-1);
      }
    }
    Poco::Timestamp::TimeDiff latency = lock_start.elapsed();
    latency_.push_back(latency);  // microseconds.
    ++count_;
    if (contention_count > 0) {
      contention_latency_.push_back(latency);
      ++count_with_contention_;
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

    // unlock.
    i = request_size_ - 1;
    while (i >= 0) {
      auto& lock_result = manager_->Unlock(*requests_[i]);
      if (lock_result.isSpecified()) {
        auto lock_future = lock_result.value()->get_future();
        LockResultInfo result_info = lock_future.get();
        if (result_info.result == SUCCESS) --i;
      } else {
        exit(-1);
      }
    }
  }
}

void LockSimulator::Stop() { is_done_ = true; }

uint64_t LockSimulator::GetCount() const { return count_; }

uint64_t LockSimulator::GetCountWithContention() const {
  return count_with_contention_;
}

void LockSimulator::SortLatency() {
  std::sort(latency_.begin(), latency_.end());
  std::sort(contention_latency_.begin(), contention_latency_.end());
}

double LockSimulator::GetAverageLatency() const {
  if (latency_.empty()) return 0;
  double sum = 0;
  for (auto latency : latency_) {
    sum += latency;
  }
  return sum / (double)latency_.size();
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

// Create requests.
void LockSimulator::CreateRequest() {
  // Generate random requests.
  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->user_id = (uintptr_t)this;
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = rng_.next() % num_nodes_;
    requests_[i]->obj_index = rng_.next() % num_objects_;
    requests_[i]->lock_type = (rng_.nextBool()) ? SHARED : EXCLUSIVE;
    requests_[i]->contention_count = 0;
  }
}

}  // namespace proto
}  // namespace rdma
