#include "tpcc_lock_simulator.h"

namespace rdma {
namespace proto {

TPCCLockSimulator::TPCCLockSimulator(LockManager* manager, int num_nodes,
                                     int num_objects, string think_time_type,
                                     int home_warehouse_id)
    : LockSimulator(manager, num_nodes, num_objects, 128, think_time_type),
      home_warehouse_id_(home_warehouse_id) {
  tpcc_lock_gen_.reset(new TPCCLockGen(home_warehouse_id_, num_nodes));
}

void TPCCLockSimulator::CreateRequest() {
  request_size_ = tpcc_lock_gen_->Generate(requests_);
}

}  // namespace proto
}  // namespace rdma
