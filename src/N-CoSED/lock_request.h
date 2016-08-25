#ifndef RDMA_N_COSED_LOCKREQUEST_H
#define RDMA_N_COSED_LOCKREQUEST_H

namespace rdma { namespace n_cosed {

struct LockRequest {
  int lm_id;
  int obj_index;
  int lock_type; // shard, exclusive
  int task; // lock, unlock
};

}}

#endif
