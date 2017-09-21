#ifndef RDMA_PROTO_LOCKWAITELEMENT_H
#define RDMA_PROTO_LOCKWAITELEMENT_H

#include "constants.h"
#include "stdint.h"

namespace rdma {
namespace proto {

struct LockWaitElement {
  int seq_no;
  uintptr_t owner_user_id;
  uint32_t owner_node_id;
  uint32_t target_node_id;
  LockType type;  // shared or exclusive
};

}  // namespace proto
}  // namespace rdma

#endif
