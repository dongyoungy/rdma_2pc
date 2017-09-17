#include "lock_simulator.h"

namespace rdma {
namespace proto {

LockSimulator::LockSimulator(LockManager* manager, int num_nodes,
                             int num_objects, int request_size)
    : manager_(manager),
      num_nodes_(num_nodes),
      num_objects_(num_objects),
      request_size_(request_size),
      count_(0) {
  latency_.reserve(kTransactionMax);
  rng_.seed();
}

LockSimulator::~LockSimulator() {}

void LockSimulator::run() {
  is_done_ = false;

  // Keep requesting locks until done
  while (!is_done_) {
    CreateRequest();

    // lock.
    int i = 0;
    Poco::Timestamp lock_start;
    while (i < request_size_) {
      auto& lock_result = manager_->Lock(*requests_[i]);
      if (lock_result.isSpecified()) {
        auto lock_future = lock_result.value()->get_future();
        LockResult result = lock_future.get();
        if (result == SUCCESS) ++i;
      } else {
        exit(-1);
      }
    }
    Poco::Timestamp::TimeDiff latency = lock_start.elapsed();
    latency_.push_back(latency);  // microseconds
    ++count_;

    // unlock.
    i = request_size_ - 1;
    while (i >= 0) {
      auto& lock_result = manager_->Unlock(*requests_[i]);
      if (lock_result.isSpecified()) {
        auto lock_future = lock_result.value()->get_future();
        LockResult result = lock_future.get();
        if (result == SUCCESS) --i;
      } else {
        exit(-1);
      }
    }
  }
}

void LockSimulator::Stop() { is_done_ = true; }

uint64_t LockSimulator::GetCount() const { return count_; }

void LockSimulator::SortLatency() {
  std::sort(latency_.begin(), latency_.begin() + latency_.size());
}

double LockSimulator::GetAverageLatency() const {
  double sum = 0;
  for (auto latency : latency_) {
    sum += latency;
  }
  return sum / (double)latency_.size();
}

double LockSimulator::Get99PercentileLatency() const {
  return latency_[floor(latency_.size() * 0.99)];
}

// Create requests.
void LockSimulator::CreateRequest() {
  // Initialize requests array if empty.
  if (requests_.empty()) {
    for (int i = 0; i < request_size_; ++i) {
      std::unique_ptr<LockRequest> request(new LockRequest);
      requests_.push_back(std::move(request));
    }
  }

  // Generate random requests.
  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->user_id = (uintptr_t)this;
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = rng_.next() % num_nodes_;
    requests_[i]->obj_index = rng_.next() % num_objects_;
    requests_[i]->lock_type = (rng_.nextBool()) ? SHARED : EXCLUSIVE;
  }
}

}  // namespace proto
}  // namespace rdma
