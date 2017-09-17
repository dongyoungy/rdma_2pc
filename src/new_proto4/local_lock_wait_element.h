#ifndef RDMA_PROTO_LOCALLOCKWAITELEMENT_H
#define RDMA_PROTO_LOCALLOCKWAITELEMENT_H

namespace rdma {
namespace proto {

struct LocalLockWaitElement {
  int seq_no;
  int owner_thread_id;
  int target_node_id;
  int target_obj_index;
  LockType lock_type;
  int status;
};

}  // namespace proto
}  // namespace rdma

#endif
