#include "long_lock_simulator.h"
#include "Poco/Thread.h"

namespace rdma {
namespace proto {

LongLockSimulator::LongLockSimulator(LockManager* manager, int id,
                                     int num_nodes, int num_objects,
                                     int request_size, string think_time_type,
                                     bool do_random_backoff, double exponent,
                                     double shared_lock_ratio,
                                     double full_scan_ratio, int full_scan_time,
                                     bool require_extension)
    : LockSimulator(manager, id, num_nodes, num_objects, 10000, think_time_type,
                    do_random_backoff),
      exponent_(exponent),
      shared_lock_ratio_(shared_lock_ratio),
      full_scan_ratio_(full_scan_ratio),
      full_scan_time_(full_scan_time),
      require_extension_(require_extension) {
  exponent_ = exponent_ * -1;
  min_pow_ = pow(1, exponent_ + 1);
  max_pow_ = pow(num_objects - 1, exponent_ + 1);
}

void LongLockSimulator::CreateRequest() {
  request_size_ = 0;
  int i = 0;
  int lm_id = (rng_.next() % num_nodes_) + 1;
  int wait_time = full_scan_time_;  // full scan read time = 5 ~ 30 ms

  bool is_full_scan = (rng_.nextDouble() <= full_scan_ratio_) ? true : false;
  if (is_full_scan) {
    // for (i = 0; i < full_scan_time_; ++i) {
    // requests_[i]->seq_no = seq_count_++;
    // requests_[i]->user_id = id_;
    // requests_[i]->owner_node_id = manager_->GetID();
    // requests_[i]->task = LOCK;
    // requests_[i]->lm_id = lm_id;
    // requests_[i]->obj_index = i;
    // requests_[i]->lock_type = SHARED;
    // requests_[i]->contention_count = 0;
    // requests_[i]->contention_count2 = 0;
    // requests_[i]->wait_time = 0;
    //}

    requests_[i]->seq_no = seq_count_++;
    requests_[i]->user_id = id_;
    requests_[i]->owner_node_id = manager_->GetID();
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = lm_id;
    requests_[i]->obj_index = 0;
    requests_[i]->lock_type = SHARED;
    requests_[i]->contention_count = 0;
    requests_[i]->contention_count2 = 0;
    requests_[i]->wait_time = 0;
    // if lease extension is required (for DSLR).
    if (require_extension_) {
      requests_[i]->wait_time = LEASE_EXTENSION_TIME;

      wait_time -= LEASE_EXTENSION_TIME;
      while (wait_time > 0) {
        ++i;
        requests_[i]->seq_no = seq_count_++;
        requests_[i]->user_id = id_;
        requests_[i]->owner_node_id = manager_->GetID();
        requests_[i]->task = LOCK;
        requests_[i]->lm_id = lm_id;
        requests_[i]->obj_index = 0;
        requests_[i]->lock_type = SHARED_EXTEND;
        requests_[i]->contention_count = 0;
        requests_[i]->contention_count2 = 0;
        if (wait_time > LEASE_EXTENSION_TIME) {
          requests_[i]->wait_time = LEASE_EXTENSION_TIME;
          wait_time -= LEASE_EXTENSION_TIME;
        } else {
          requests_[i]->wait_time = wait_time;
          wait_time = 0;
        }
      }
    } else {
      requests_[i]->wait_time = wait_time;
    }
    request_size_ = 1;
  } else {
    bool is_shared_lock = false;
    (rng_.nextDouble() <= shared_lock_ratio_) ? true : false;

    requests_[i]->seq_no = seq_count_++;
    requests_[i]->user_id = id_;
    requests_[i]->owner_node_id = manager_->GetID();
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = lm_id;
    requests_[i]->obj_index = 0;
    requests_[i]->lock_type = (is_shared_lock ? SHARED : EXCLUSIVE);
    requests_[i]->contention_count = 0;
    requests_[i]->contention_count2 = 0;
    requests_[i]->wait_time = 0;

    ++i;
    requests_[i]->seq_no = seq_count_++;
    requests_[i]->user_id = id_;
    requests_[i]->owner_node_id = manager_->GetID();
    requests_[i]->task = LOCK;
    requests_[i]->lm_id = lm_id;
    // get obj index from power law distribution
    int obj_index =
        (int)pow((max_pow_ - min_pow_) * rng_.nextDouble() + min_pow_,
                 1 / (exponent_ + 1)) -
        1;
    requests_[i]->obj_index = 1 + obj_index;
    if (requests_[i]->obj_index >= num_objects_) {
      requests_[i]->obj_index = num_objects_ - 1;
    }
    requests_[i]->lock_type = (is_shared_lock ? SHARED : EXCLUSIVE);
    requests_[i]->contention_count = 0;
    requests_[i]->contention_count2 = 0;
    requests_[i]->wait_time = 0;
    request_size_ = i + 1;
  }
}

}  // namespace proto
}  // namespace rdma
