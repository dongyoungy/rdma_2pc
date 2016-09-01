#ifndef RDMA_N_COSED_MESSAGE_H
#define RDMA_N_COSED_MESSAGE_H

#include <rdma/rdma_cma.h>

namespace rdma { namespace n_cosed {

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
      LOCK_MODE,
      EXCLUSIVE_TO_SHARED_LOCK_REQUEST,
      SHARED_UNLOCK_RESULT,
      SHARED_LOCK_RELEASE,
      SHARED_TO_EXCLUSIVE_LOCK_REQUEST,
      EXCLUSIVE_TO_EXCLUSIVE_LOCK_REQUEST,
      EXCLUSIVE_TO_SHARED_LOCK_GRANT,
      EXCLUSIVE_TO_SHARED_LOCK_GRANT_ACK,
      SHARED_TO_EXCLUSIVE_LOCK_GRANT,
      EXCLUSIVE_TO_EXCLUSIVE_LOCK_GRANT,
      SHARED_TO_EXCLUSIVE_LOCK_GRANT_ACK,
      EXCLUSIVE_TO_EXCLUSIVE_LOCK_GRANT_ACK
    } type;
    int lock_type;
    int lock_mode;
    struct ibv_mr lock_table_mr;
    int user_id;
    int next_user_id;
    int prev_user_id;
    int home_id;
    int current_owner;
    int new_owner;
    int manager_id; // id of lock manager requesting lock
    int obj_index; // obj index in lock table
    int lock_result;
    int from;
    int shared_lock_counter;
};

}}

#endif
