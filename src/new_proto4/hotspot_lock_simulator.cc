#include "hotspot_lock_simulator.h"

namespace rdma {
namespace proto {

HotspotLockSimulator::HotspotLockSimulator(LockManager* manager, int num_nodes,
                                           int num_objects, int request_size,
                                           string think_time_type,
                                           bool do_random_backoff)
    : LockSimulator(manager, num_nodes, num_objects, request_size,
                    think_time_type, do_random_backoff) {}

void HotspotLockSimulator::CreateRequest() {
  // Generate random requests.
  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->seq_no = seq_count_++;
    requests_[i]->user_id = (uintptr_t)this;
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = 0;  // this is fixed to node 0.
    requests_[i]->obj_index = rng_.next() % num_objects_;
    requests_[i]->lock_type = (rng_.nextBool()) ? SHARED : EXCLUSIVE;
    requests_[i]->contention_count = 0;
  }
}

}  // namespace proto
}  // namespace rdma
