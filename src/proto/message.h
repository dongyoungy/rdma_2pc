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
    } type;
    int lock_type;
    struct ibv_mr lock_table_mr;
    int user_id;
    uint32_t manager_id; // id of lock manager requesting lock
    int obj_index; // obj index in lock table
    bool lock_result;
};

}}

#endif
