#ifndef RDMA_PROTO_LOCKWAITELEMENT_H
#define RDMA_PROTO_LOCKWAITELEMENT_H

namespace rdma { namespace proto{

struct LockWaitElement {
  int seq_no;
  int user_id;
  int home_id;
  int type; // shared or exclusive
};

}}

#endif
