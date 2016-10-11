#ifndef RDMA_PROTO_LOCKREQUEST_H
#define RDMA_PROTO_LOCKREQUEST_H

#include <cstddef>
#include <stdint.h>

namespace rdma { namespace proto{

struct LockRequest {
  int seq_no; // sequence no (id)
  bool is_undo;
  int user_id;
  int lm_id;
  int obj_index;
  int lock_type; // shared, exclusive
  int task; // lock, unlock
  int read_target;
  uint64_t* original_value;
  struct ibv_mr* original_value_mr;
  uint32_t* read_buffer;
  struct ibv_mr* read_buffer_mr;
  uint64_t* read_buffer2;
  struct ibv_mr* read_buffer2_mr;

  uint64_t prev_value;
  uint32_t exclusive;
  uint32_t shared;
};

}}

#endif
