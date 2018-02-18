#ifndef RDMA_PROTO_LONG_LOCK_SIMULATOR_H
#define RDMA_PROTO_LONG_LOCK_SIMULATOR_H

#include <cmath>
#include "lock_simulator.h"

namespace rdma {
namespace proto {

// Lock simulator that includes a long-running reads (i.e., full table scan).
// This simulator is based on the PowerLawSimulator.
class LongLockSimulator : public LockSimulator {
 public:
  LongLockSimulator(LockManager* manager, int id, int num_nodes,
                    int num_objects, int request_size,
                    std::string think_time_type, bool do_random_backoff,
                    double exponent, double shared_lock_ratio,
                    double full_scan_ratio, int full_scan_time,
                    bool require_extension);

 protected:
  virtual void CreateRequest();

 private:
  double exponent_;
  double shared_lock_ratio_;
  double full_scan_ratio_;
  double min_pow_;
  double max_pow_;

  int full_scan_time_;
  bool require_extension_;
};

}  // namespace proto
}  // namespace rdma

#endif
