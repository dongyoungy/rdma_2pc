#include "tpcc_lock_simulator.h"

namespace rdma {
namespace proto {

TPCCLockSimulator::TPCCLockSimulator(
    LockManager* manager, int id, int num_nodes, int num_objects,
    string think_time_type, bool do_random_backoff, int home_warehouse_id,
    bool random_warehouse, int num_warehouse, double full_scan_ratio,
    int full_scan_rows, int full_scan_time)
    : LockSimulator(manager, id, num_nodes, num_objects, 50000, think_time_type,
                    do_random_backoff),
      home_warehouse_id_(home_warehouse_id),
      random_warehouse_(random_warehouse),
      num_warehouse_(num_warehouse),
      full_scan_ratio_(full_scan_ratio) {
  tpcc_lock_gen_.reset(new TPCCLockGen(home_warehouse_id_, num_nodes,
                                       full_scan_ratio_, full_scan_rows,
                                       full_scan_time));
  temp_lock_requests_ = new LockRequest*[50000];
}

void TPCCLockSimulator::CreateRequest() {
  int random_warehouse_id = 1 + (rng_.next() % num_nodes_);
  int local_warehouse = rng_.next() % num_warehouse_;
  // int local_warehouse = id_ % num_warehouse_;
  request_size_ = tpcc_lock_gen_->Generate(requests_);
  for (int i = 0; i < request_size_; ++i) {
    requests_[i]->seq_no = seq_count_++;
    requests_[i]->lm_id =
        (random_warehouse_ ? random_warehouse_id : requests_[i]->lm_id + 1);
    // requests_[i]->lm_id += 1;  // node starts from 1.
    requests_[i]->contention_count = 0;
    requests_[i]->contention_count2 = 0;
    requests_[i]->obj_index += (local_warehouse * kTPCCNumObjects);
    requests_[i]->user_id = id_;
    requests_[i]->owner_node_id = manager_->GetID();
  }
}

}  // namespace proto
}  // namespace rdma
