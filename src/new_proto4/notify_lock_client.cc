#include "notify_lock_client.h"

namespace rdma {
namespace proto {

// constructor
NotifyLockClient::NotifyLockClient(const string& work_dir,
                                   LockManager* local_manager,
                                   uint32_t local_user_count,
                                   uint32_t remote_lm_id)
    : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {}

// destructor
NotifyLockClient::~NotifyLockClient() {}

// read lock object for locking
int NotifyLockClient::ReadForLock(Context* context, int seq_no,
                                  uint32_t user_id, LockType lock_type,
                                  int obj_index) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* request = lock_requests_[lock_request_idx_].get();
  request->seq_no = seq_no;
  request->user_id = user_id;
  request->obj_index = obj_index;
  request->lock_type = lock_type;
  request->task = READ_LOCK;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uint64_t)request->read_buffer2;
  sge.length = sizeof(uint64_t);
  sge.lkey = request->read_buffer2_mr->lkey;

  send_work_request.wr_id = (uint64_t)request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_RDMA_READ;

  send_work_request.wr.rdma.rkey = context->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context->lock_table_mr->addr + (obj_index * sizeof(uint64_t));

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "ReadForUnlock(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return -1;
  }

  ++num_rdma_read_;
  return 0;
}

// read lock object for unlocking
int NotifyLockClient::ReadForUnlock(Context* context, int seq_no,
                                    uint32_t user_id, LockType lock_type,
                                    int obj_index) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* request = lock_requests_[lock_request_idx_].get();
  request->seq_no = seq_no;
  request->user_id = user_id;
  request->obj_index = obj_index;
  request->lock_type = lock_type;
  request->task = READ_UNLOCK;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uint64_t)request->read_buffer2;
  sge.length = sizeof(uint64_t);
  sge.lkey = request->read_buffer2_mr->lkey;

  send_work_request.wr_id = (uint64_t)request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_RDMA_READ;

  send_work_request.wr.rdma.rkey = context->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context->lock_table_mr->addr + (obj_index * sizeof(uint64_t));

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "ReadForUnlock(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return -1;
  }

  ++num_rdma_read_;
  return 0;
}

int NotifyLockClient::TryLock(const Message& message) {
  return this->ReadRemotely(context_, message);
}

// The fucntion is used by NotifyLockClient.
int NotifyLockClient::ReadRemotely(Context* context, const Message& message) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  current_request->seq_no = message.seq_no;
  current_request->read_target = READ_ALL;
  current_request->obj_index = message.obj_index;
  current_request->lock_type = message.lock_type;
  current_request->releasing_node_id = message.releasing_node_id;
  current_request->task = READ;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->read_buffer2;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->read_buffer_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_RDMA_READ;

  send_work_request.wr.rdma.rkey = context->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  clock_gettime(CLOCK_MONOTONIC, &start_rdma_read_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "ReadRemotely(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return -1;
  }

  ++num_rdma_read_;
  return 0;
}

int NotifyLockClient::FindNodePosition(uint32_t value) {
  uint32_t i = 1, pos = 0;
  while (!(i & value)) {
    i = i << 1;
    ++pos;
  }
  return pos;
}

int NotifyLockClient::FindNodePositions(uint32_t value, int* nodes) {
  uint32_t i = 1, pos = 0;
  int count = 0;
  while (pos < 32) {
    if ((i & value) != 0) {
      nodes[count++] = pos;
    }
    i = i << 1;
    ++pos;
  }
  return count;
}

int NotifyLockClient::NotifyWaitingNodes(LockRequest* request, uint64_t value) {
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((value) >> 32);
  shared = (uint32_t)value;

  int nodes[64];
  int sz;
  if (request->lock_type == SHARED) {
    // notify nodes for exclusive lock.
    memset(nodes, 0x00, sizeof(int) * 64);
    sz = FindNodePositions(exclusive, nodes);
    for (int i = 0; i < sz; ++i) {
      local_manager_->GrantLock(request->seq_no, local_owner_id_, remote_lm_id_,
                                nodes[i], EXCLUSIVE, request->obj_index);
    }
  } else {
    // notify nodes for shared lock.
    memset(nodes, 0x00, sizeof(int) * 64);
    sz = FindNodePositions(shared, nodes);
    for (int i = 0; i < sz; ++i) {
      local_manager_->GrantLock(request->seq_no, local_owner_id_, remote_lm_id_,
                                nodes[i], SHARED, request->obj_index);
    }

    // notify nodes for exclusive lock.
    memset(nodes, 0x00, sizeof(int) * 64);
    sz = FindNodePositions(exclusive, nodes);
    for (int i = 0; i < sz; ++i) {
      local_manager_->GrantLock(request->seq_no, local_owner_id_, remote_lm_id_,
                                nodes[i], EXCLUSIVE, request->obj_index);
    }
  }

  return 0;
}

int NotifyLockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "Work completion status is not IBV_WC_SUCCESS: "
         << work_completion->status << endl;
    return -1;
  }

  if (work_completion->opcode == IBV_WC_RECV) {
    Context* context = (Context*)work_completion->wr_id;
    Message* message = context->receive_message_buffer->GetMessage();
    context->receive_message_buffer->Rotate();
    // post receive first.
    ReceiveMessage(context);

    // if received lock table MR info + current lock mode
    if (message->type == Message::LOCK_TABLE_MR) {
      // copy server rdma semaphore region
      local_manager_->UpdateLockModeTable(message->manager_id,
                                          message->lock_mode);
      context->lock_table_mr = new ibv_mr;
      memcpy(context->lock_table_mr, &message->lock_table_mr,
             sizeof(*context->lock_table_mr));
      initialized_ = true;
    } else if (message->type == Message::LOCK_MODE) {
      local_manager_->UpdateLockModeTable(message->manager_id,
                                          message->lock_mode);
    } else if (message->type == Message::LOCK_REQUEST_RESULT) {
      // cout << "received lock request result." << endl;
      local_manager_->NotifyLockRequestResult(
          message->seq_no, message->owner_user_id, message->lock_type,
          remote_lm_id_, message->obj_index, 0, message->lock_result);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      // cout << "received unlock request result" << endl;
      local_manager_->NotifyUnlockRequestResult(
          message->seq_no, message->owner_user_id, message->lock_type,
          remote_lm_id_, message->obj_index, message->lock_result);
    }
  } else if (work_completion->opcode == IBV_WC_SEND) {
    clock_gettime(CLOCK_MONOTONIC, &end_send_message_);
    double time_taken = ((double)end_send_message_.tv_sec * 1e+9 +
                         (double)end_send_message_.tv_nsec) -
                        ((double)start_send_message_.tv_sec * 1e+9 +
                         (double)start_send_message_.tv_nsec);
    total_send_message_time_ += time_taken;
    ++num_send_message_;
  } else if (work_completion->opcode == IBV_WC_FETCH_ADD) {
    LockRequest* request = (LockRequest*)work_completion->wr_id;
    // completion of fetch-and-add, i.e. remote shared/exclusive locking
    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_remote_shared_lock_);
    double time_taken = ((double)end_remote_shared_lock_.tv_sec * 1e+9 +
                         (double)end_remote_shared_lock_.tv_nsec) -
                        ((double)start_remote_shared_lock_.tv_sec * 1e+9 +
                         (double)start_remote_shared_lock_.tv_nsec);
    total_shared_lock_remote_time_ += time_taken;
    ++num_shared_lock_;

    uint64_t prev_value = request->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value) >> 32);
    shared = (uint32_t)value;

    request->exclusive = exclusive;
    request->shared = shared;

    if (request->task == LOCK) {
      if (request->lock_type == EXCLUSIVE) {
        if (exclusive == 0 && shared == 0) {
          ++total_lock_success_;
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
        } else {
          waiters_before_me_[request->obj_index] = value;
          ++total_lock_contention_;
          ++request->contention_count;
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              QUEUED);
        }
      } else if (request->lock_type == SHARED) {
        if (exclusive == 0) {
          ++total_lock_success_;
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
        } else {
          waiters_before_me_[request->obj_index] = value;
          ++total_lock_contention_;
          ++request->contention_count;
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              QUEUED);
        }
      }
    } else if (request->task == UNLOCK) {
      if (!request->is_undo) {
        if (request->lock_type == EXCLUSIVE) {
          prev_value = ((uint64_t)(exclusive - local_owner_bitvector_id_))
                           << 32 |
                       shared;
        } else {
          prev_value = ((uint64_t)exclusive) << 32 |
                       (shared - local_owner_bitvector_id_);
        }
        NotifyWaitingNodes(request, prev_value);
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, SUCCESS);
      }
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    // Reading for lock.
    LockRequest* request = (LockRequest*)work_completion->wr_id;
    uint32_t releasing_node = (uint32_t)pow(request->releasing_node_id, 2);
    uint64_t exclusive_release = ((uint64_t)releasing_node) << 32;
    uint64_t shared_release = releasing_node;
    uint64_t waiters = waiters_before_me_[request->obj_index];
    if ((waiters & exclusive_release) == exclusive_release) {
      waiters_before_me_[request->obj_index] -= exclusive_release;
    }
    if ((waiters & shared_release) == shared_release) {
      waiters_before_me_[request->obj_index] -= shared_release;
    }
    uint64_t value = request->read_buffer2;
    uint32_t exclusive;
    uint64_t remaining_waiters = value & waiters_before_me_[request->obj_index];
    exclusive = (uint32_t)((remaining_waiters) >> 32);
    if (remaining_waiters == 0 ||
        (exclusive == 0 && request->lock_type == SHARED)) {
      local_manager_->NotifyLockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, request->contention_count, SUCCESS_FROM_QUEUED);
    }
  } else {
    cerr << "NotifyLockClient: Invalid opcode = " << work_completion->opcode
         << endl;
    exit(ERROR_INVALID_OPCODE);
  }
  return 0;
}

}  // namespace proto
}  // namespace rdma
