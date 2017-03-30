#ifndef RDMA_PROTO_LOCKWAITELEMENT_H
#define RDMA_PROTO_LOCKWAITELEMENT_H

#include "stdint.h"

namespace rdma { namespace proto{

struct LockWaitElement {
  int seq_no;
  uint32_t owner_user_id;
  uint32_t owner_node_id;
  uint32_t target_node_id;
  int type; // shared or exclusive
};

}}

#endif
