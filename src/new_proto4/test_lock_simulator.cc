#include "test_lock_simulator.h"

namespace rdma {
namespace proto {

TestLockSimulator::TestLockSimulator(LockManager* manager, int num_nodes,
                                     int num_objects, string think_time_type,
                                     bool do_random_backoff)
    : LockSimulator(manager, num_nodes, num_objects, 5, think_time_type,
                    do_random_backoff) {
  temp_lock_requests_ = new LockRequest*[128];
}

void TestLockSimulator::CreateRequest() {
  request_size_ = 5;
  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->seq_no = seq_count_++;
    requests_[i]->user_id = (uintptr_t)this;
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = rng_.next() % num_nodes_;
    requests_[i]->obj_index = i;
    requests_[i]->lock_type = (rng_.nextBool()) ? SHARED : EXCLUSIVE;
    requests_[i]->contention_count = 0;
    temp_lock_requests_[i] = requests_[i].get();
  }
}

}  // namespace proto
}  // namespace rdma
