#include "powerlaw_lock_simulator.h"
#include "Poco/Thread.h"

namespace rdma {
namespace proto {

PowerlawLockSimulator::PowerlawLockSimulator(
    LockManager* manager, int id, int num_nodes, int num_objects,
    int request_size, string think_time_type, bool do_random_backoff,
    double exponent, double shared_lock_ratio, bool use_update_lock)
    : LockSimulator(manager, id, num_nodes, num_objects, request_size,
                    think_time_type, do_random_backoff),
      exponent_(exponent),
      shared_lock_ratio_(shared_lock_ratio),
      use_update_lock_(use_update_lock) {
  exponent_ = exponent_ * -1;
  min_pow_ = pow(1, exponent_ + 1);
  max_pow_ = pow(num_objects, exponent_ + 1);
}

void PowerlawLockSimulator::CreateRequest() {
  // Generate random requests.
  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->seq_no = seq_count_++;
    requests_[i]->user_id = id_;
    requests_[i]->owner_node_id = manager_->GetID();
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = (rng_.next() % num_nodes_) + 1;
    // get obj index from power law distribution
    int obj_index =
        (int)pow((max_pow_ - min_pow_) * rng_.nextDouble() + min_pow_,
                 1 / (exponent_ + 1)) -
        1;
    requests_[i]->obj_index = obj_index;
    if (use_update_lock_) {
      int val = rng_.next(3);
      switch (val) {
        case 0:
          requests_[i]->lock_type = SHARED;
          break;
        case 1:
          requests_[i]->lock_type = EXCLUSIVE;
          break;
        case 2:
          requests_[i]->lock_type = UPDATE;
          break;
        default:
          break;
      }
    } else {
      requests_[i]->lock_type =
          (rng_.nextDouble() <= shared_lock_ratio_) ? SHARED : EXCLUSIVE;
    }
    requests_[i]->contention_count = 0;
    requests_[i]->contention_count2 = 0;
  }
}

}  // namespace proto
}  // namespace rdma
