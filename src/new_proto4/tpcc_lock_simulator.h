#ifndef RDMA_PROTO_TPCCLOCKSIMULATOR_H
#define RDMA_PROTO_TPCCLOCKSIMULATOR_H

#include "constants.h"
#include "lock_simulator.h"
#include "tpcc_lock_gen.h"

using namespace std;

namespace rdma {
namespace proto {

class TPCCLockSimulator : public LockSimulator {
 public:
  TPCCLockSimulator(LockManager* manager, int id, int num_nodes,
                    int num_objects, string think_time_type,
                    bool do_random_backoff, int home_warehouse_id,
                    bool random_warehouse);

 protected:
  virtual void CreateRequest();

 private:
  std::unique_ptr<TPCCLockGen> tpcc_lock_gen_;
  uint32_t home_warehouse_id_;
  int max_request_size_;
  bool random_warehouse_;
  LockRequest** temp_lock_requests_;
};

}  // namespace proto
}  // namespace rdma

#endif
