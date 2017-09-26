#ifndef RDMA_PROTO_LOCKREQUEST_H
#define RDMA_PROTO_LOCKREQUEST_H

#include <stdint.h>
#include <cstddef>
#include "constants.h"

namespace rdma {
namespace proto {

struct LockRequest {
  LockRequest& operator=(const LockRequest& other) {
    if (this != &other) {
      seq_no = other.seq_no;
      user_id = other.user_id;
      read_target = other.read_target;
      obj_index = other.obj_index;
      lock_type = other.lock_type;
      contention_count = other.contention_count;
      task = other.task;
    }
    return *this;
  }
  int seq_no;  // sequence no (id)
  bool is_undo;
  uint32_t owner_node_id;
  uint32_t releasing_node_id;
  uintptr_t user_id;
  int lm_id;
  int obj_index;
  LockType lock_type;  // shared, exclusive
  Task task;           // lock, unlock
  ReadType read_target;
  int retry;
  int contention_count;
  bool is_retry;
  uint64_t original_value;
  struct ibv_mr* original_value_mr;
  uint32_t read_buffer;
  struct ibv_mr* read_buffer_mr;
  uint64_t read_buffer2;
  struct ibv_mr* read_buffer2_mr;

  uint64_t all_waiters;
  uint64_t prev_value;
  uint32_t waiters;
  uint32_t exclusive;
  uint32_t shared;
};

}  // namespace proto
}  // namespace rdma

#endif
