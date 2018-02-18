#ifndef RDMA_PROTO_POWERLAW_LOCK_SIMULATOR_H
#define RDMA_PROTO_POWERLAW_LOCK_SIMULATOR_H

#include <cmath>
#include "lock_simulator.h"

namespace rdma {
namespace proto {

class PowerlawLockSimulator : public LockSimulator {
 public:
  PowerlawLockSimulator(LockManager* manager, int id, int num_nodes,
                        int num_objects, int request_size,
                        std::string think_time_type, bool do_random_backoff,
                        double exponent, double shared_lock_ratio,
                        bool use_update_lock = false);

 protected:
  virtual void CreateRequest();

 private:
  double exponent_;
  double shared_lock_ratio_;
  double min_pow_;
  double max_pow_;

  bool use_update_lock_;
};

}  // namespace proto
}  // namespace rdma

#endif
