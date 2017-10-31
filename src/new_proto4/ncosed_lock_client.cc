#include "ncosed_lock_client.h"

namespace rdma {
namespace proto {

// constructor
NCOSEDLockClient::NCOSEDLockClient(const string& work_dir,
                                   LockManager* local_manager,
                                   uint32_t local_user_count,
                                   uint32_t remote_lm_id)
    : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {}

// destructor
NCOSEDLockClient::~NCOSEDLockClient() {}

bool NCOSEDLockClient::RequestLock(const LockRequest& request,
                                   LockMode lock_mode) {
  // try locking remotely
  return this->Lock(context_, request);
}

bool NCOSEDLockClient::RequestUnlock(const LockRequest& request,
                                     LockMode lock_mode) {
  return this->Unlock(context_, request);
}

bool NCOSEDLockClient::Lock(Context* context, const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = LOCK;
  current_request->prev_value = 0;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;

  if (current_request->lock_type == SHARED) {
    send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    send_work_request.wr.atomic.compare_add = 1;
    ++num_rdma_atomic_fa_;
  } else if (current_request->lock_type == EXCLUSIVE) {
    send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
    exclusive = current_request->user_id;
    shared = 0;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    send_work_request.wr.atomic.compare_add = 0;
    send_work_request.wr.atomic.swap = new_value;
    ++num_rdma_atomic_cas_;
  }

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "LockRemotely(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return false;
  }
  return true;
}

bool NCOSEDLockClient::Lock(Context* context, const LockRequest& request,
                            uint64_t prev_value) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->owner_node_id = local_owner_id_;
  current_request->prev_value = prev_value;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;

  if (current_request->lock_type == SHARED) {
    send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    send_work_request.wr.atomic.compare_add = 1;
  } else if (current_request->lock_type == EXCLUSIVE) {
    send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
    exclusive = current_request->user_id;
    shared = (uint32_t)prev_value;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    send_work_request.wr.atomic.compare_add = prev_value;
    send_work_request.wr.atomic.swap = new_value;
  }

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Lock(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return false;
  }
  return true;
}

bool NCOSEDLockClient::Unlock(Context* context, const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->owner_node_id = local_owner_id_;
  current_request->task = UNLOCK;
  current_request->is_undo = false;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;

  if (current_request->lock_type == SHARED) {
    send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    send_work_request.wr.atomic.compare_add = -1;
  } else if (current_request->lock_type == EXCLUSIVE) {
    send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
    uint64_t prev_value = ((uint64_t)current_request->user_id) << 32 | 0;
    current_request->prev_value = prev_value;
    send_work_request.wr.atomic.compare_add = prev_value;
    send_work_request.wr.atomic.swap = 0;
  }
  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_atomic_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Unlock(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return false;
  }
  return true;
}

bool NCOSEDLockClient::UnlockExclusiveFA(Context* context,
                                         const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->owner_node_id = local_owner_id_;
  current_request->task = UNLOCK;
  current_request->lock_type = EXCLUSIVE;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;

  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
  uint64_t prev_value = ((uint64_t)current_request->user_id) << 32 | 0;
  current_request->prev_value = prev_value;
  send_work_request.wr.atomic.compare_add = (-1) * prev_value;
  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_atomic_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Unlock(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return false;
  }
  return true;
}

bool NCOSEDLockClient::UnlockBoth(Context* context, const LockRequest& request,
                                  uint32_t exclusive, uint32_t shared) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);
  uint64_t prev_value = ((uint64_t)exclusive) << 32 | shared;

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = UNLOCK;
  current_request->lock_type = BOTH;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
  current_request->prev_value = prev_value;
  send_work_request.wr.atomic.compare_add = prev_value;
  send_work_request.wr.atomic.swap = 0;

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_atomic_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "UnlockBoth(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return false;
  }
  return true;
}

bool NCOSEDLockClient::UnlockShared(Context* context,
                                    const LockRequest& request,
                                    uint32_t shared) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);
  uint64_t prev_value = shared;

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->owner_node_id = local_owner_id_;
  current_request->task = UNLOCK;
  current_request->lock_type = SHARED;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
  current_request->prev_value = prev_value;
  send_work_request.wr.atomic.compare_add = prev_value;
  send_work_request.wr.atomic.swap = 0;

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_atomic_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "UnlockShared(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return false;
  }
  return true;
}

bool NCOSEDLockClient::UnlockShared(const Message& msg, int shared_count) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  current_request->task = UNLOCK;
  current_request->lock_type = SHARED;
  current_request->seq_no = msg.seq_no;
  current_request->lm_id = msg.node_id;
  current_request->obj_index = msg.obj_index;
  current_request->user_id = msg.owner_user_id;
  current_request->owner_node_id = msg.owner_node_id;
  current_request->prev_value = shared_count;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
  send_work_request.wr.atomic.compare_add = current_request->prev_value;
  send_work_request.wr.atomic.swap = 0;

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context_->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context_->lock_table_mr->rkey;

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_atomic_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context_->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "UnlockShared(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return false;
  }
  return true;
}

int NCOSEDLockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {
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
      // cout << "received lock table MR." << endl;
      // copy server rdma semaphore region
      local_manager_->UpdateLockModeTable(message->manager_id,
                                          message->lock_mode);
      context->lock_table_mr = new ibv_mr;
      memcpy(context->lock_table_mr, &message->lock_table_mr,
             sizeof(*context->lock_table_mr));
      if (context->lock_table_mr == NULL) {
        cerr << "lock table MR NULL" << endl;
        return -1;
      }
      initialized_ = true;
    } else if (message->type == Message::LOCK_MODE) {
      local_manager_->UpdateLockModeTable(message->manager_id,
                                          message->lock_mode);
    } else if (message->type == Message::LOCK_REQUEST_RESULT) {
      local_manager_->NotifyLockRequestResult(
          message->seq_no, message->owner_user_id, message->lock_type,
          remote_lm_id_, message->obj_index, 0, message->lock_result);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      local_manager_->NotifyUnlockRequestResult(
          message->seq_no, message->owner_user_id, message->lock_type,
          remote_lm_id_, message->obj_index, message->lock_result);
    }
    //} else if (work_completion->opcode == IBV_WC_SEND) {
    // Context* context = (Context*)work_completion->wr_id;
    // Message* msg = context->last_message;
    // if (msg->type == Message::NCOSED_LOCK_RELEASE) {
    // local_manager_->NotifyUnlockRequestResult(msg->seq_no,
    // msg->owner_user_id,  msg->lock_type, msg->node_id,  msg->obj_index,
    // SUCCESS);
    //}
  } else if (work_completion->opcode == IBV_WC_COMP_SWAP) {
    LockRequest* request = (LockRequest*)work_completion->wr_id;

    uint64_t orig_value = request->original_value;
    uint64_t value = orig_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    if (LockManager::IsAtomicHCAReplyBe()) {
      value = __bswap_constant_64(orig_value);  // Compiler builtin
    }
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value) >> 32);
    shared = (uint32_t)value;
    uint32_t expected_exclusive, expected_shared;
    uint64_t expected = request->prev_value;
    expected_exclusive = (uint32_t)((expected) >> 32);
    expected_shared = (uint32_t)expected;

    if (request->task == LOCK) {
      // cout << "user " << request->user_id << " locking" << endl;
      if (exclusive == 0 && shared == 0) {
        if (expected == value) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
        } else {
          // try again
          ++request->contention_count;
          ++user_retry_count_[request->user_id];
          if (user_retry_count_[request->user_id] >=
              LockManager::GetFailRetry()) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                FAILURE);
          } else {
            this->Lock(context_, *request, value);
          }
        }
      } else if (exclusive != 0 && shared == 0) {
        if (expected == value) {
          // if previous value is equal to expected, then send lock request
          local_manager_->SendNCOSEDLockRequest(
              request->seq_no,
              (exclusive - 1) / local_manager_->GetNumUser() + 1,
              request->lm_id, request->obj_index, local_owner_id_,
              request->user_id, shared, EXCLUSIVE);
        } else {
          // try again
          ++request->contention_count;
          ++user_retry_count_[request->user_id];
          if (user_retry_count_[request->user_id] >=
              LockManager::GetFailRetry()) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                FAILURE);
          } else {
            this->Lock(context_, *request, value);
          }
        }
      } else if (exclusive == 0 && shared != 0) {
        if (expected == value) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
        } else {
          // try again
          ++request->contention_count;
          ++user_retry_count_[request->user_id];
          if (user_retry_count_[request->user_id] >=
              LockManager::GetFailRetry()) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                FAILURE);
          } else {
            this->Lock(context_, *request, value);
          }
        }
      } else {
        local_manager_->NotifyLockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, request->contention_count,
            FAILURE);
      }
    } else if (request->task == UNLOCK) {
      // cout << "user " << request->user_id << " unlocking" << endl;
      if (request->lock_type == EXCLUSIVE) {
        if (exclusive != request->user_id) {
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, SUCCESS);
          if (expected != value) {
            local_manager_->SendNCOSEDLockGrant(remote_lm_id_,
                                                request->obj_index);
          }
        } else if (exclusive == request->user_id && shared != 0) {
          // cout << "!!!" << endl;
          // this->UnlockExclusiveFA(context_, *request);
          this->Unlock(context_, *request);
        } else if (exclusive == request->user_id) {
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, SUCCESS);
        }
      } else {
        // cout << "???" << endl;
      }
    }
  } else if (work_completion->opcode == IBV_WC_FETCH_ADD) {
    // completion of fetch-and-add
    // counter has been increased.

    LockRequest* request = (LockRequest*)work_completion->wr_id;

    uint64_t prev_value = request->original_value;
    uint64_t value = prev_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    if (LockManager::IsAtomicHCAReplyBe()) {
      value = __bswap_constant_64(prev_value);  // Compiler builtin
    }
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value) >> 32);
    shared = (uint32_t)value;
    uint32_t expected_exclusive, expected_shared;
    uint64_t expected = request->prev_value;
    expected_exclusive = (uint32_t)((expected) >> 32);
    expected_shared = (uint32_t)expected;
    if (request->task == LOCK) {
      if (exclusive == 0) {
        local_manager_->NotifyLockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, request->contention_count,
            SUCCESS);
      } else if (exclusive != 0 && shared == 0) {
        // exclusive -> shared
        local_manager_->SendNCOSEDLockRequest(
            request->seq_no, (exclusive - 1) / local_manager_->GetNumUser() + 1,
            request->lm_id, request->obj_index, local_owner_id_,
            request->user_id, shared, SHARED);
      } else if (exclusive != 0 && shared > 0) {
        this->UndoLocking(context_, *request);
      }
    } else if (request->task == UNLOCK) {
      // must be undoing
      if (request->lock_type == EXCLUSIVE) {
        if (exclusive == request->user_id) {
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, SUCCESS);
          // local_manager_->SendNCOSEDLockGrant(remote_lm_id_,
          // request->obj_index);
        } else {
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, FAILURE);
        }
      } else if (request->lock_type == SHARED) {
        if (request->is_undo) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              FAILURE);
        } else {
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, SUCCESS);
        }
        // if (exclusive != 0 && shared == 1) {
        // local_manager_->SendNCOSEDLockGrant(
        //(exclusive - 1) / local_manager_->GetNumUser() + 1, exclusive,
        // request->obj_index);
        //}
      }
    }
  }

  return 0;
}

}  // namespace proto
}  // namespace rdma
