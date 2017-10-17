#ifndef RDMA_PROTO_TESTLOCKSIMULATOR_H
#define RDMA_PROTO_TESTLOCKSIMULATOR_H

#include "constants.h"
#include "lock_simulator.h"
#include "tpcc_lock_gen.h"

using namespace std;

namespace rdma {
namespace proto {

class TestLockSimulator : public LockSimulator {
 public:
  TestLockSimulator(LockManager* manager, int id, int num_nodes,
                    int num_objects, string think_time_type,
                    bool do_random_backoff);

 protected:
  virtual void CreateRequest();

 private:
  LockRequest** temp_lock_requests_;
};

}  // namespace proto
}  // namespace rdma

#endif
