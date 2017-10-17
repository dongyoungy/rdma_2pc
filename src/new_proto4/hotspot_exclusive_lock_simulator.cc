#include "hotspot_exclusive_lock_simulator.h"

namespace rdma {
namespace proto {

HotspotExclusiveLockSimulator::HotspotExclusiveLockSimulator(
    LockManager* manager, int id, int num_nodes, int num_objects,
    int request_size, string think_time_type, bool do_random_backoff)
    : LockSimulator(manager, id, num_nodes, num_objects, request_size,
                    think_time_type, do_random_backoff) {}

void HotspotExclusiveLockSimulator::CreateRequest() {
  // Generate random requests.
  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->seq_no = seq_count_++;
    requests_[i]->user_id = id_;
    requests_[i]->owner_node_id = manager_->GetID();
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = 1;  // this is fixed to node 1.
    requests_[i]->obj_index = rng_.next() % num_objects_;
    requests_[i]->lock_type = EXCLUSIVE;
    requests_[i]->contention_count = 0;
    requests_[i]->contention_count2 = 0;
  }
}

}  // namespace proto
}  // namespace rdma
