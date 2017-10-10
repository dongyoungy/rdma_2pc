#ifndef RDMA_PROTO_MESSAGE_H
#define RDMA_PROTO_MESSAGE_H

#include <rdma/rdma_cma.h>
#include "constants.h"

namespace rdma {
namespace proto {

class Message {
 public:
  Message(){};
  enum {
    LOCK_TABLE_MR_REQUEST,
    LOCK_TABLE_MR,
    LOCK_REQUEST,
    LOCK_REQUEST_RESULT,
    UNLOCK_REQUEST,
    UNLOCK_REQUEST_RESULT,
    LOCAL_MANAGER_ID,
    LOCK_MODE_REQUEST,
    LOCK_MODE,
    GRANT_LOCK,
    GRANT_LOCK_ACK,
    REJECT_LOCK,
    NCOSED_LOCK_REQUEST,
    NCOSED_LOCK_GRANT,
    NCOSED_LOCK_RELEASE,
    NCOSED_LOCK_RELEASE_SUCCESS
  } type;
  Task task;
  LockType lock_type;
  LockMode lock_mode;
  struct ibv_mr lock_table_mr;
  int seq_no;  // seq. no. of lock request
  uint32_t owner_node_id;
  uint32_t releasing_node_id;
  uint32_t target_node_id;
  uintptr_t owner_user_id;
  uint32_t manager_id;  // id of lock manager requesting lock
  int obj_index;        // obj index in lock table
  LockResult lock_result;

  // used by NCOSED
  int node_id;
  int request_node_id;
  int shared_remaining;
  uintptr_t request_user_id;
};

}  // namespace proto
}  // namespace rdma

#endif
