#ifndef RDMA_PROTO_LOCKREQUEST_H
#define RDMA_PROTO_LOCKREQUEST_H

#include <stdint.h>
#include <cstddef>
#include "Poco/Timestamp.h"
#include "constants.h"

namespace rdma {
namespace proto {

struct LockRequest {
  LockRequest() {
    wait_time = 0;
    d2lm_increment = 1;
  }
  LockRequest(const LockRequest& other) {
    seq_no = other.seq_no;
    lm_id = other.lm_id;
    user_id = other.user_id;
    owner_node_id = other.owner_node_id;
    read_target = other.read_target;
    obj_index = other.obj_index;
    lock_type = other.lock_type;
    deadlock_count = other.deadlock_count;
    timestamp = other.timestamp;
    is_failed = other.is_failed;
    contention_count = other.contention_count;
    contention_count2 = other.contention_count2;
    contention_count3 = other.contention_count3;
    contention_count4 = other.contention_count4;
    contention_count5 = other.contention_count5;
    contention_count6 = other.contention_count6;
    task = other.task;
    prev_value = other.prev_value;
    reset_from = other.reset_from;
    exclusive_number = other.exclusive_number;
    shared_number = other.shared_number;
    last_exclusive_number = other.last_exclusive_number;
    last_shared_number = other.last_shared_number;
    exclusive_leave = other.exclusive_leave;
    shared_leave = other.shared_leave;
    exclusive_max = other.exclusive_max;
    shared_max = other.shared_max;

    update_number = other.update_number;
    last_update_number = other.last_update_number;
    update_max = other.update_max;

    wait_time = other.wait_time;
    d2lm_increment = other.d2lm_increment;

    rdma_send = other.rdma_send;
    rdma_atomic_cas = other.rdma_atomic_cas;
    rdma_atomic_fa = other.rdma_atomic_fa;
    rdma_write = other.rdma_write;
    rdma_read = other.rdma_read;
  }
  LockRequest& operator=(const LockRequest& other) {
    if (this != &other) {
      seq_no = other.seq_no;
      lm_id = other.lm_id;
      user_id = other.user_id;
      read_target = other.read_target;
      obj_index = other.obj_index;
      owner_node_id = other.owner_node_id;
      lock_type = other.lock_type;
      deadlock_count = other.deadlock_count;
      timestamp = other.timestamp;
      reset_from = other.reset_from;
      contention_count = other.contention_count;
      contention_count2 = other.contention_count2;
      contention_count3 = other.contention_count3;
      contention_count4 = other.contention_count4;
      contention_count5 = other.contention_count5;
      contention_count6 = other.contention_count6;
      is_failed = other.is_failed;
      task = other.task;
      prev_value = other.prev_value;
      last_exclusive_number = other.last_exclusive_number;
      last_shared_number = other.last_shared_number;
      exclusive_number = other.exclusive_number;
      shared_number = other.shared_number;
      exclusive_leave = other.exclusive_leave;
      shared_leave = other.shared_leave;
      exclusive_max = other.exclusive_max;
      shared_max = other.shared_max;
      wait_time = other.wait_time;
      d2lm_increment = other.d2lm_increment;

      update_number = other.update_number;
      last_update_number = other.last_update_number;
      update_max = other.update_max;

      rdma_send = other.rdma_send;
      rdma_atomic_cas = other.rdma_atomic_cas;
      rdma_atomic_fa = other.rdma_atomic_fa;
      rdma_write = other.rdma_write;
      rdma_read = other.rdma_read;
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
  int d2lm_increment;
  LockType lock_type;  // shared, exclusive
  Task task;           // lock, unlock
  ReadType read_target;
  int retry;
  int deadlock_count;
  int contention_count;
  int contention_count2;
  int contention_count3;
  int contention_count4;
  int contention_count5;
  int contention_count6;
  bool is_retry;
  uint64_t original_value;
  uint64_t reset_from;
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

  uint16_t last_exclusive_number = 0;
  uint16_t last_shared_number = 0;
  uint16_t last_update_number = 0;
  uint16_t exclusive_number = 0;
  uint16_t shared_number = 0;
  uint16_t update_number = 0;
  uint16_t exclusive_leave = 0;
  uint16_t shared_leave = 0;
  uint16_t exclusive_max = 0;
  uint16_t shared_max = 0;
  uint16_t update_max = 0;
  int wait_time = 0;

  // RDMA stats
  uint64_t rdma_send = 0;
  uint64_t rdma_atomic_cas = 0;
  uint64_t rdma_atomic_fa = 0;
  uint64_t rdma_write = 0;
  uint64_t rdma_read = 0;

  Poco::Timestamp timestamp;
  bool is_failed = false;
};

}  // namespace proto
}  // namespace rdma

#endif
