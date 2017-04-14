#include "direct_queue_lock_client.h"

namespace rdma { namespace proto {

// constructor
DirectQueueLockClient::DirectQueueLockClient(const string& work_dir, LockManager* local_manager,
    uint32_t local_user_count,
    uint32_t remote_lm_id) : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {
}

// destructor
DirectQueueLockClient::~DirectQueueLockClient() {
}

int DirectQueueLockClient::RequestLock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
    int lock_mode) {
  user_retry_count_[user_id] = 0;
  user_fail_[user_id]        = false;
  user_polling_[user_id]     = false;
  user_waiters_[user_id]     = 0;
  user_all_waiters_[user_id] = 0;

  // try locking remotely
  return this->LockRemotely(context_, seq_no, user_id, lock_type, obj_index);
}

int DirectQueueLockClient::RequestUnlock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
    int lock_mode) {
  return this->UnlockRemotely(context_, seq_no, user_id, lock_type, obj_index);
}

int DirectQueueLockClient::LockRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type,
    int obj_index) {

  if (lock_type == LockManager::SHARED) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_shared_lock_);
  } else if (lock_type == LockManager::EXCLUSIVE) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_exclusive_lock_);
  }

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  pthread_mutex_lock(&lock_mutex_);
  LockRequest* request   = lock_requests_[lock_request_idx_];
  request->seq_no        = seq_no;
  request->owner_node_id = local_owner_bitvector_id_;
  request->user_id       = user_id;
  request->lock_type     = lock_type;
  request->obj_index     = obj_index;
  request->task          = TASK_LOCK;
  lock_request_idx_      = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr   = (uint64_t)request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = request->original_value_mr->lkey;

  send_work_request.wr_id          = (uint64_t)request;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode     = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  if (lock_type == LockManager::SHARED) {
    exclusive = 0;
    shared = local_owner_bitvector_id_;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    send_work_request.wr.atomic.compare_add = new_value;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    exclusive = local_owner_bitvector_id_;
    shared = 0;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    send_work_request.wr.atomic.compare_add = new_value;
  }

  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  send_work_request.wr.atomic.rkey        =
    context->lock_table_mr->rkey;

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_atomic_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "LockRemotely(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  ++num_rdma_atomic_;
  pthread_mutex_unlock(&lock_mutex_);

  return 0;
}

int DirectQueueLockClient::UnlockRemotely(Context* context, int seq_no, uint32_t user_id,
    int lock_type, int obj_index, bool is_undo, bool retry) {

  if (lock_type == LockManager::SHARED) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_shared_lock_);
  } else if (lock_type == LockManager::EXCLUSIVE) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_exclusive_lock_);
  }

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));


  pthread_mutex_lock(&lock_mutex_);
  LockRequest* request = lock_requests_[lock_request_idx_];
  request->seq_no      = seq_no;
  request->user_id     = user_id;
  request->lock_type   = lock_type;
  request->obj_index   = obj_index;
  request->is_undo     = is_undo;
  request->is_retry    = retry;
  request->task        = TASK_UNLOCK;
  lock_request_idx_    = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr   = (uint64_t)request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = request->original_value_mr->lkey;

  send_work_request.wr_id          = (uint64_t)request;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  //send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED | IBV_EXP_SEND_INLINE;
  send_work_request.exp_opcode     = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  if (lock_type == LockManager::SHARED) {
    exclusive = 0;
    shared = local_owner_bitvector_id_;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    new_value = (-1) * new_value; // need to subtract for unlock
    send_work_request.wr.atomic.compare_add = new_value;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    exclusive = 0;
    shared = 0;
    uint64_t new_value = ((uint64_t)local_owner_bitvector_id_) << 32 | shared;
    new_value = (-1) * new_value; // need to subtract for unlock
    send_work_request.wr.atomic.compare_add = new_value;
  }
  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  send_work_request.wr.atomic.rkey        =
    context->lock_table_mr->rkey;

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_atomic_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "UnlockRemotely(): ibv_exp_post_send() failed: " << strerror(ret) <<
      endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }
  ++num_rdma_atomic_;
  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}


int DirectQueueLockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {

  if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "Work completion status is not IBV_WC_SUCCESS: " <<
      work_completion->status << endl;
    return -1;
  }

  if (work_completion->opcode == IBV_WC_RECV) {

    Context* context = (Context *)work_completion->wr_id;
    Message* message = context->receive_message_buffer->GetMessage();
    context->receive_message_buffer->Rotate();
    // post receive first.
    ReceiveMessage(context);

    // if received lock table MR info + current lock mode
    if (message->type == Message::LOCK_TABLE_MR) {
      //cout << "received lock table MR." << endl;
      // copy server rdma semaphore region
      local_manager_->UpdateLockModeTable(
          message->manager_id,
          message->lock_mode
          );
      context->lock_table_mr = new ibv_mr;
      memcpy(context->lock_table_mr,
          &message->lock_table_mr,
          sizeof(*context->lock_table_mr));
      if (context->lock_table_mr == NULL) {
        cerr << "lock table MR NULL" << endl;
        return -1;
      }
      initialized_ = true;
    } else if (message->type == Message::LOCK_MODE) {
      local_manager_->UpdateLockModeTable(
          message->manager_id,
          message->lock_mode
          );
    } else if (message->type == Message::LOCK_REQUEST_RESULT) {
      //cout << "received lock request result." << endl;
      local_manager_->NotifyLockRequestResult(
          message->seq_no,
          message->owner_user_id,
          message->lock_type,
          remote_lm_id_,
          message->obj_index,
          message->lock_result);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      //cout << "received unlock request result" << endl;
      local_manager_->NotifyUnlockRequestResult(
          message->seq_no,
          message->owner_user_id,
          message->lock_type,
          remote_lm_id_,
          message->obj_index,
          message->lock_result);
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

    LockRequest* request = (LockRequest *)work_completion->wr_id;
    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_rdma_atomic_);
    double time_taken = ((double)end_rdma_atomic_.tv_sec * 1e+9 +
        (double)end_rdma_atomic_.tv_nsec) -
      ((double)start_rdma_atomic_.tv_sec * 1e+9 +
          (double)start_rdma_atomic_.tv_nsec);
    total_rdma_atomic_time_ += time_taken;

    uint64_t prev_value = *request->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value)>>32);
    shared = (uint32_t)value;

    request->exclusive = exclusive;
    request->shared = shared;

    if (request->task == LockManager::TASK_LOCK) {
      if (request->lock_type == LockManager::EXCLUSIVE) {
        if (exclusive == 0 && shared == 0) {
          // exclusive lock acquisition successful
          //wait_after_me_[request->obj_index] = 0;
          ++total_lock_success_;
          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              remote_lm_id_,
              request->obj_index,
              LockManager::RESULT_SUCCESS);
        //} else if ((wait_after_me_[request->obj_index] & value) != 0) {
          //wait_after_me_[request->obj_index] = (wait_after_me_[request->obj_index] & value);
          //this->UndoLocking(context_, request, true);
        } else {
          ++total_lock_contention_;
          user_all_waiters_[request->user_id] = value;
          this->HandleExclusive(request);
        }
      } else {
        // shared lock
        if (exclusive == 0) {
          // it should have been successful since exclusive and shared was 0
          ++total_lock_success_;
          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              remote_lm_id_,
              request->obj_index,
              LockManager::RESULT_SUCCESS);
        //} else if ((wait_after_me_[request->obj_index] & value) != 0) {
          //wait_after_me_[request->obj_index] = (wait_after_me_[request->obj_index] & value);
          //this->UndoLocking(context_, request, true);
        } else {
          ++total_lock_contention_;
          user_all_waiters_[request->user_id] = exclusive;
          this->HandleShared(request);
        }
      }
    } else if (request->task == TASK_UNLOCK) {
      if (request->is_undo) {
        int result = RESULT_FAILURE;
        //if (request->is_retry) {
          //result = RESULT_RETRY;
        //}
        local_manager_->NotifyLockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            result);
      } else {
        //wait_after_me_[request->obj_index] = value;
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_SUCCESS);
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

    //uint64_t prev;
    //uint32_t value, exclusive, shared;
    LockRequest* request = (LockRequest *)work_completion->wr_id;
    //uint64_t all_value = *request->read_buffer2;
    uint64_t prev_value = *request->read_buffer2;
    uint64_t all_value = prev_value;
//#if __BYTE_ORDER == __LITTLE_ENDIAN
    //all_value = __bswap_constant_64(prev_value);  // Compiler builtin
//#else
    //all_value = prev_value;
//#endif

    // polling result
    //if (request->read_target == ALL) {
      //uint64_t prev_value = *request->read_buffer2;
      //prev = prev_value;
      //exclusive = (uint32_t)((all_value)>>32);
      //shared = (uint32_t)all_value;
//#if __BYTE_ORDER == __LITTLE_ENDIAN
      //all_value = __bswap_constant_64(prev_value);  // Compiler builtin
//#else
      //all_value = prev_value;
//#endif
      //all_value = prev_value;
    //} else {
      //value = *request->read_buffer;
    //}

    //if (request->read_target == EXCLUSIVE) {
      //// Polling on X -> proceed if value is zero
      //if ((context_->waiters & value) == 0) {
        //context_->waiters = 0;
        //++total_lock_success_with_poll_;
        //sum_poll_when_success_ += context_->retry;
        //local_manager_->NotifyLockRequestResult(
            //request->seq_no,
            //request->user_id,
            //request->lock_type,
            //request->obj_index,
            //LockManager::RESULT_SUCCESS);
      //} else {
        //// otherwise, read/poll again (shared -> exclusive)
        //this->HandleShared(request);
      //}
    //} else {
      //if ((context_->all_waiters & all_value) == 0) {
        //context_->all_waiters = 0;
        //++total_lock_success_with_poll_;
        //sum_poll_when_success_ += context_->retry;
        //local_manager_->NotifyLockRequestResult(
            //request->seq_no,
            //request->user_id,
            //request->lock_type,
            //request->obj_index,
            //LockManager::RESULT_SUCCESS);
      //} else {
        //this->HandleExclusive(request);
      //}
    //}

    uint64_t remaining = (user_all_waiters_[request->user_id] & all_value);
    //uint64_t remaining = (context_->all_waiters & all_value);
    //cout << "prev_value = " << all_value << endl;
    //cout << "prev_value2 = " << prev << endl;
    //cout << "waiters = " << context_->all_waiters << endl;
    //cout << "remaining = " << remaining << endl;
    //cout << "remaining2 = " << (context_->all_waiters & all_value) << endl;
    //cout << "remaining3 = " << (context_->all_waiters & prev) << endl;

    if (request->lock_type == SHARED) {
      // Polling on X -> proceed if value is zero
      if ((remaining >> 32) == 0) {
        user_all_waiters_[request->user_id] = 0;
        ++total_lock_success_with_poll_;
        sum_poll_when_success_ += user_retry_count_[request->user_id];
        local_manager_->NotifyLockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        // otherwise, read/poll again (shared -> exclusive)
        this->HandleShared(request);
      }
    } else {
      if (remaining == 0) {
        user_all_waiters_[request->user_id] = 0;
        ++total_lock_success_with_poll_;
        sum_poll_when_success_ += user_retry_count_[request->user_id];
        local_manager_->NotifyLockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        this->HandleExclusive(request);
      }
    }
  }

  return 0;
}

int DirectQueueLockClient::HandleShared(LockRequest* request) {
  if (user_retry_count_[request->user_id] >= LockManager::GetPollRetry()) {
    this->UndoLocking(context_, request);
    return 0;
  }
  ++user_retry_count_[request->user_id];
  //request->read_target = EXCLUSIVE;
  //this->ReadRemotely(context_,
      //request->seq_no,
      //request->user_id,
      //request->read_target,
      //request->lock_type,
      //request->obj_index);
  this->ReadRemotely(context_,
      request->seq_no,
      request->user_id,
      request->lock_type,
      request->obj_index);

  return FUNC_SUCCESS;
}

int DirectQueueLockClient::HandleExclusive(LockRequest* request) {
  if (user_retry_count_[request->user_id] >= LockManager::GetPollRetry()) {
    this->UndoLocking(context_, request);
    return 0;
  }
  ++user_retry_count_[request->user_id];
  this->ReadRemotely(context_,
      request->seq_no,
      request->user_id,
      request->lock_type,
      request->obj_index);

  return FUNC_SUCCESS;
}

}}
