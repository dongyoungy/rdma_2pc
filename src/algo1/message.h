#ifndef RDMA_PROTO_MESSAGE_H
#define RDMA_PROTO_MESSAGE_H

#include <rdma/rdma_cma.h>

namespace rdma { namespace proto {

class Message {
  public:
    Message() {};
    enum {
      LOCK_TABLE_MR_REQUEST,
      LOCK_TABLE_MR,
      LOCK_REQUEST,
      LOCK_REQUEST_RESULT,
      UNLOCK_REQUEST,
      UNLOCK_REQUEST_RESULT,
      LOCAL_MANAGER_ID,
      LOCK_MODE_REQUEST,
      LOCK_MODE
    } type;
    int lock_type;
    int lock_mode;
    struct ibv_mr lock_table_mr;
    int user_id;
    int manager_id; // id of lock manager requesting lock
    int obj_index; // obj index in lock table
    int lock_result;
};

}}

#endif
