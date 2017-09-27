#include "direct_queue_lock_client.h"

namespace rdma {
namespace proto {

// constructor
DirectQueueLockClient::DirectQueueLockClient(const string& work_dir,
                                             LockManager* local_manager,
                                             uint32_t local_user_count,
                                             uint32_t remote_lm_id)
    : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {}

// destructor
DirectQueueLockClient::~DirectQueueLockClient() {}

bool DirectQueueLockClient::RequestLock(const LockRequest& request,
                                        LockMode lock_mode) {
  // try locking remotely
  return this->LockRemotely(context_, request);
}

bool DirectQueueLockClient::RequestUnlock(const LockRequest& request,
                                          LockMode lock_mode) {
  return this->UnlockRemotely(context_, request);
}

bool DirectQueueLockClient::LockRemotely(Context* context,
                                         const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->owner_node_id = local_owner_bitvector_id_;
  current_request->task = LOCK;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uint64_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  switch (current_request->lock_type) {
    case SHARED: {
      exclusive = 0;
      shared = local_owner_bitvector_id_;
      uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
      send_work_request.wr.atomic.compare_add = new_value;
      break;
    }
    case EXCLUSIVE: {
      exclusive = local_owner_bitvector_id_;
      shared = 0;
      uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
      send_work_request.wr.atomic.compare_add = new_value;
      break;
    }
    default:
      break;
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

  ++num_rdma_atomic_;

  return true;
}

bool DirectQueueLockClient::UnlockRemotely(Context* context,
                                           const LockRequest& request,
                                           bool is_undo, bool retry) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->owner_node_id = local_owner_bitvector_id_;
  current_request->is_undo = is_undo;
  current_request->is_retry = retry;
  current_request->task = UNLOCK;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  switch (current_request->lock_type) {
    case SHARED: {
      exclusive = 0;
      shared = local_owner_bitvector_id_;
      uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
      new_value = (-1) * new_value;  // need to subtract for unlock
      send_work_request.wr.atomic.compare_add = new_value;
      break;
    }
    case EXCLUSIVE: {
      exclusive = 0;
      shared = 0;
      uint64_t new_value = ((uint64_t)local_owner_bitvector_id_) << 32 | shared;
      new_value = (-1) * new_value;  // need to subtract for unlock
      send_work_request.wr.atomic.compare_add = new_value;
      break;
    }
    default:
      break;
  }

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "UnlockRemotely(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return false;
  }
  ++num_rdma_atomic_;
  return true;
}

int DirectQueueLockClient::HandleWorkCompletion(
    struct ibv_wc* work_completion) {
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
    // completion of fetch-and-add, i.e. remote shared/exclusive locking

    LockRequest* request = (LockRequest*)work_completion->wr_id;
    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_rdma_atomic_);
    double time_taken = ((double)end_rdma_atomic_.tv_sec * 1e+9 +
                         (double)end_rdma_atomic_.tv_nsec) -
                        ((double)start_rdma_atomic_.tv_sec * 1e+9 +
                         (double)start_rdma_atomic_.tv_nsec);
    total_rdma_atomic_time_ += time_taken;

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

    request->exclusive = exclusive;
    request->shared = shared;

    if (request->task == LOCK) {
      if (request->lock_type == EXCLUSIVE) {
        if (exclusive == 0 && shared == 0) {
          // exclusive lock acquisition successful
          // wait_after_me_[request->obj_index] = 0;
          ++total_lock_success_;
          clock_gettime(CLOCK_MONOTONIC, &end_remote_exclusive_lock_);
          double time_taken =
              ((double)end_remote_exclusive_lock_.tv_sec * 1e+9 +
               (double)end_remote_exclusive_lock_.tv_nsec) -
              ((double)start_remote_exclusive_lock_.tv_sec * 1e+9 +
               (double)start_remote_exclusive_lock_.tv_nsec);
          total_exclusive_lock_remote_time_ += time_taken;
          ++num_exclusive_lock_;
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
          //} else if ((wait_after_me_[request->obj_index] & value) != 0) {
          // wait_after_me_[request->obj_index] =
          // (wait_after_me_[request->obj_index] & value);
          // this->UndoLocking(context_, request, true);
        } else {
          ++total_lock_contention_;
          ++request->contention_count;
          user_all_waiters_[request->user_id] = value;
          this->HandleExclusive(request);
        }
      } else {
        // shared lock
        if (exclusive == 0) {
          // it should have been successful since exclusive and shared was 0
          ++total_lock_success_;
          clock_gettime(CLOCK_MONOTONIC, &end_remote_shared_lock_);
          double time_taken = ((double)end_remote_shared_lock_.tv_sec * 1e+9 +
                               (double)end_remote_shared_lock_.tv_nsec) -
                              ((double)start_remote_shared_lock_.tv_sec * 1e+9 +
                               (double)start_remote_shared_lock_.tv_nsec);
          total_shared_lock_remote_time_ += time_taken;
          ++num_shared_lock_;
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
          //} else if ((wait_after_me_[request->obj_index] & value) != 0) {
          // wait_after_me_[request->obj_index] =
          // (wait_after_me_[request->obj_index] & value);
          // this->UndoLocking(context_, request, true);
        } else {
          ++total_lock_contention_;
          ++request->contention_count;
          user_all_waiters_[request->user_id] = value;
          this->HandleShared(request);
        }
      }
    } else if (request->task == UNLOCK) {
      if (request->is_undo) {
        user_retry_count_[request->user_id] = 0;
        local_manager_->NotifyLockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, request->contention_count,
            FAILURE);
      } else {
        // wait_after_me_[request->obj_index] = value;
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, SUCCESS);
      }
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_rdma_read_);
    double time_taken = ((double)end_rdma_read_.tv_sec * 1e+9 +
                         (double)end_rdma_read_.tv_nsec) -
                        ((double)start_rdma_read_.tv_sec * 1e+9 +
                         (double)start_rdma_read_.tv_nsec);
    total_rdma_read_time_ += time_taken;

    LockRequest* request = (LockRequest*)work_completion->wr_id;
    uint64_t prev_value = request->read_buffer2;
    uint64_t remaining = (user_all_waiters_[request->user_id] & prev_value);

    if (request->lock_type == SHARED) {
      // Polling on Ex -> proceed if value is zero
      if ((remaining >> 32) == 0) {
        user_all_waiters_[request->user_id] = 0;
        ++total_lock_success_with_poll_;
        sum_poll_when_success_ += user_retry_count_[request->user_id];
        user_retry_count_[request->user_id] = 0;
        local_manager_->NotifyLockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, request->contention_count,
            SUCCESS);
      } else {
        // otherwise, read/poll again (shared -> exclusive)
        ++request->contention_count;
        this->HandleShared(request);
      }
    } else {
      if (remaining == 0) {
        user_all_waiters_[request->user_id] = 0;
        ++total_lock_success_with_poll_;
        sum_poll_when_success_ += user_retry_count_[request->user_id];
        user_retry_count_[request->user_id] = 0;
        local_manager_->NotifyLockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, request->contention_count,
            SUCCESS);
      } else {
        ++request->contention_count;
        this->HandleExclusive(request);
      }
    }
  }

  return 0;
}

int DirectQueueLockClient::HandleShared(LockRequest* request) {
  if (user_retry_count_[request->user_id] >= LockManager::GetPollRetry()) {
    this->UndoLocking(context_, *request);
    return 0;
  }
  ++user_retry_count_[request->user_id];
  request->read_target = READ_ALL;
  this->ReadRemotely(context_, *request);

  return FUNC_SUCCESS;
}

int DirectQueueLockClient::HandleExclusive(LockRequest* request) {
  if (user_retry_count_[request->user_id] >= LockManager::GetPollRetry()) {
    this->UndoLocking(context_, *request);
    return 0;
  }
  ++user_retry_count_[request->user_id];
  request->read_target = READ_ALL;
  this->ReadRemotely(context_, *request);

  return FUNC_SUCCESS;
}

}  // namespace proto
}  // namespace rdma
