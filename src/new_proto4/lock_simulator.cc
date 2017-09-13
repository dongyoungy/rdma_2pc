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

    int i = 0;
    Poco::Timestamp lock_start;
    while (i < request_size_) {
      std::future<LockResult> lock_future =
          manager_->Lock(*requests_[i]).get_future();

      LockResult result = lock_future.get();
      if (result == Success) ++i;
    }
    Poco::Timestamp::TimeDiff latency = lock_start.elapsed();
    latency_.push_back(latency);  // microseconds
    ++count_;

    // unlock.
    i = request_size_ - 1;
    while (i >= 0) {
      std::future<LockResult> lock_future =
          manager_->Unlock(*requests_[i]).get_future();

      LockResult result = lock_future.get();
      if (result == Success) --i;
    }
  }
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
    requests_[i]->task = LockManager::TASK_LOCK;
    requests_[i]->lm_id = rng_.next() % num_nodes_;
    requests_[i]->obj_index = rng_.next() % num_objects_;
    requests_[i]->lock_type = (rng_.nextBool()) ? SHARED : EXCLUSIVE;
  }
}

}  // namespace proto
}  // namespace rdma
