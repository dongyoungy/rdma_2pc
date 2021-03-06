#ifndef RDMA_PROTO_HOTSPOT_LOCK_SIMULATOR_H
#define RDMA_PROTO_HOTSPOT_LOCK_SIMULATOR_H

#include "lock_simulator.h"

namespace rdma {
namespace proto {

class HotspotLockSimulator : public LockSimulator {
 public:
  HotspotLockSimulator(LockManager* manager, int id, int num_nodes,
                       int num_objects, int request_size,
                       std::string think_time_type, bool do_random_backoff);

 protected:
  virtual void CreateRequest();
};

}  // namespace proto
}  // namespace rdma

#endif
