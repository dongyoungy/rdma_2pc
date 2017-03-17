#ifndef RDMA_PROTO_LOCALLOCKWAITELEMENT_H
#define RDMA_PROTO_LOCALLOCKWAITELEMENT_H

namespace rdma { namespace proto {

struct LocalLockWaitElement {
  int seq_no;
  uint32_t owner_thread_id;
  uint32_t target_node_id;
  uint32_t target_obj_index;
  int lock_type;
  int status;
};

}}

#endif


