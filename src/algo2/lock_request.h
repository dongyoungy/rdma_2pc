#ifndef RDMA_PROTO_LOCKREQUEST_H
#define RDMA_PROTO_LOCKREQUEST_H

namespace rdma { namespace proto{

struct LockRequest {
  int lm_id;
  int obj_index;
  int lock_type; // shard, exclusive
  int task; // lock, unlock
};

}}

#endif
