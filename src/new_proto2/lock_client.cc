#include "lock_client.h"

namespace rdma { namespace proto {

// constructor
LockClient::LockClient(const string& work_dir, LockManager* local_manager,
    uint32_t local_user_count,
    uint32_t remote_lm_id) : Client(work_dir, local_manager, local_user_count, remote_lm_id) {
  message_in_progress_ = false;

  user_retry_count_ = new int[local_user_count+1];
  user_fail_        = new bool[local_user_count+1];
  user_polling_     = new bool[local_user_count+1];
  user_waiters_     = new uint32_t[local_user_count+1];
  user_all_waiters_ = new uint64_t[local_user_count+1];

  memset(user_retry_count_, 0x00, sizeof(int)*(local_user_count+1));
  memset(user_fail_, 0x00, sizeof(bool)*(local_user_count+1));
  memset(user_polling_, 0x00, sizeof(bool)*(local_user_count+1));
  memset(user_waiters_, 0x00, sizeof(uint32_t)*(local_user_count+1));
  memset(user_all_waiters_, 0x00, sizeof(uint64_t)*(local_user_count+1));
}

// destructor
LockClient::~LockClient() {
  delete[] user_retry_count_;
  delete[] user_fail_;
  delete[] user_polling_;
  delete[] user_waiters_;
  delete[] user_all_waiters_;
}

int LockClient::HandleConnection(Context* context) {
  //cout << "connected to server." << endl;
  context->connected = true;

  SendLockTableRequest(context);

  return 0;
}

int LockClient::HandleDisconnect(Context* context) {

  if (context->original_value_mr)
    ibv_dereg_mr(context->original_value_mr);

  delete context->send_message_buffer;
  delete context->receive_message_buffer;

  delete context;

  return 0;
}

int LockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {

  if (work_completion->status != IBV_WC_SUCCESS) {
    //if (work_completion->opcode == IBV_WC_COMP_SWAP ||
        //work_completion->opcode == IBV_WC_FETCH_ADD) {
      //SendLockModeRequest(context);
      //local_manager_->NotifyLockRequestResult(
          //context->last_seq_no,
          //context->last_user_id,
          //context->last_lock_type,
          //context->last_obj_index,
          //LockManager::RESULT_FAILURE);
      //return 0;
    //}
    //else {
      //cerr << "Work completion status is not IBV_WC_SUCCESS: " <<
        //work_completion->status << endl;
      //return -1;
    //}
    cerr << "(LockClient) Work completion status is not IBV_WC_SUCCESS: " <<
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
      pthread_mutex_lock(&lock_mutex_);
      message_in_progress_ = false;

      local_manager_->NotifyLockRequestResult(
          message->seq_no,
          message->owner_user_id,
          message->lock_type,
          remote_lm_id_,
          message->obj_index,
          message->lock_result);
      pthread_cond_signal(&lock_cond_);
      pthread_mutex_unlock(&lock_mutex_);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      //cout << "received unlock request result" << endl;
      pthread_mutex_lock(&lock_mutex_);
      message_in_progress_ = false;

      local_manager_->NotifyUnlockRequestResult(
          message->seq_no,
          message->owner_user_id,
          message->lock_type,
          remote_lm_id_,
          message->obj_index,
          message->lock_result);
      pthread_cond_signal(&lock_cond_);
      pthread_mutex_unlock(&lock_mutex_);
    }
  } else if (work_completion->opcode == IBV_WC_SEND) {
    clock_gettime(CLOCK_MONOTONIC, &end_send_message_);
    double time_taken = ((double)end_send_message_.tv_sec * 1e+9 +
        (double)end_send_message_.tv_nsec) -
      ((double)start_send_message_.tv_sec * 1e+9 +
       (double)start_send_message_.tv_nsec);
    total_send_message_time_ += time_taken;
    ++num_send_message_;
  } else if (work_completion->opcode == IBV_WC_COMP_SWAP) {
    // We don't see this for new system, which explicitly use FA instead
    // completion of compare-and-swap, i.e. remote exclusive locking

    LockRequest* request = (LockRequest *)work_completion->wr_id;
    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_remote_exclusive_lock_);
    double time_taken = ((double)end_remote_exclusive_lock_.tv_sec * 1e+9 +
        (double)end_remote_exclusive_lock_.tv_nsec) -
      ((double)start_remote_exclusive_lock_.tv_sec * 1e+9 +
          (double)start_remote_exclusive_lock_.tv_nsec);
    total_exclusive_lock_remote_time_ += time_taken;
    ++num_exclusive_lock_;

    uint64_t prev_value = *request->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value)>>32);
    shared = (uint32_t)value;

    if (request->task == LockManager::TASK_LOCK) {
      // it should have been successful since exclusive and shared was 0
      if (exclusive == 0 && shared == 0) {
        local_manager_->NotifyLockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        local_manager_->NotifyLockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_FAILURE);
      }
    } else if (request->task == LockManager::TASK_UNLOCK) {
      if (exclusive == request->user_id && shared == 0) {
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_SUCCESS);
      } else if (exclusive == request->user_id && shared != 0) {
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_RETRY);
      } else {
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_FAILURE);
      }
    }
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
          ++total_lock_success_;
          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              remote_lm_id_,
              request->obj_index,
              LockManager::RESULT_SUCCESS);
        } else if (exclusive == 0 && shared != 0) {
          // shared lock exists, handle shared -> exclusive
          ++total_lock_contention_;
          this->HandleSharedToExclusive(request);
        } else if (exclusive != 0 && shared == 0) {
          // exclusive lock exists, wait for others
          ++total_lock_contention_;
          this->HandleExclusiveToExclusive(request);
        } else {
          // lock acquisition failed, undoing FA
          ++total_lock_contention_;
          this->UndoLocking(context_, request);
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
        } else if (exclusive != 0 && shared == 0){
          // exclusive lock exists
          ++total_lock_contention_;
          this->HandleExclusiveToShared(request);
        } else {
          ++total_lock_contention_;
          this->UndoLocking(context_, request);
        }
      }
    } else if (request->task == TASK_UNLOCK) {
      if (user_fail_[request->user_id]) {
        user_fail_[request->user_id] = false;
        if (user_polling_[request->user_id]) {
          user_polling_[request->user_id] = false;
        } else {
          ++user_retry_count_[request->user_id];
          if (user_retry_count_[request->user_id] > LockManager::GetPollRetry()) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no,
                request->user_id,
                request->lock_type,
                remote_lm_id_,
                request->obj_index,
                RESULT_FAILURE);
          } else {
            local_manager_->NotifyLockRequestResult(
                request->seq_no,
                request->user_id,
                request->lock_type,
                remote_lm_id_,
                request->obj_index,
                RESULT_RETRY);
          }
        }
      } else {
        if (request->lock_type == EXCLUSIVE) {
          waitlist_[request->obj_index] = (exclusive - request->user_id);
        } else {
          waitlist_[request->obj_index] = exclusive;
        }
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_SUCCESS);
        // notify other nodes
        //int ret = NotifyWaitingNodes(context);

        //if (ret == ERR_MORE_THAN_ONE_NODE) {
          //this->ReadRemotely(context,
              //context->last_user_id,
              //context->last_obj_index);
        //} else {
          //// unlock always succeeds. (really?)
          //local_manager_->NotifyUnlockRequestResult(context->last_user_id,
              //context->last_lock_type,
              //context->last_obj_index,
              //LockManager::RESULT_SUCCESS);
        //}
      }
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {

    LockRequest* request = (LockRequest *)work_completion->wr_id;

    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_rdma_read_);
    double time_taken = ((double)end_rdma_read_.tv_sec * 1e+9 +
        (double)end_rdma_read_.tv_nsec) -
      ((double)start_rdma_read_.tv_sec * 1e+9 +
          (double)start_rdma_read_.tv_nsec);
    total_rdma_read_time_ += time_taken;
    // polling result
    uint32_t value = *request->read_buffer;
    //#if __BYTE_ORDER == __LITTLE_ENDIAN
    //uint32_t value = __bswap_constant_32(prev_value);  // Compiler builtin
    //#endif
    if (request->read_target == SHARED) {
      // Polling on Sh_X -> proceed if value is zero
      if (value == 0) {
        local_manager_->NotifyLockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        // otherwise, read/poll again (shared -> exclusive)
        this->PollSharedToExclusive(request);
      }
    } else {
      if (request->lock_type == EXCLUSIVE) {
        // exclusive -> exclusive
        if (user_retry_count_[request->user_id] > LockManager::GetPollRetry()) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              remote_lm_id_,
              request->obj_index,
              RESULT_FAILURE);
        } else {
          this->PollExclusiveToExclusive(request);
        }
      } else {
        // exclusive -> shared
        this->PollExclusiveToShared(request);
      }
    }
    //else if (purpose == READ_NOTIFYING) {

      //// read an object for notifying
      //uint64_t prev_value = *context->read_buffer2;
//#if __BYTE_ORDER == __LITTLE_ENDIAN
      //uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
//#endif
      //uint32_t exclusive, shared;
      //exclusive = (uint32_t)((value)>>32);
      //shared = (uint32_t)value;


    //}

  }

  return 0;
}

// Handle Shared -> Exclusive
int LockClient::HandleSharedToExclusive(LockRequest* request) {
  int rule = LockManager::GetSharedExclusiveRule();
  switch (rule) {
    case RULE_FAIL:
      this->UndoLocking(context_, request);
      break;
    case RULE_POLL:
      request->read_target = SHARED;
      this->ReadRemotely(context_,
          request->seq_no,
          request->user_id,
          request->read_target,
          request->lock_type,
          request->obj_index);
      break;
    default:
      cerr << "Unsupported Shared -> Exclusive rule: " <<  rule << endl;
      return FUNC_FAIL;
  }

  return FUNC_SUCCESS;
}

// Handle Exclusive -> Shared
int LockClient::HandleExclusiveToShared(LockRequest* request) {
  int rule = LockManager::GetExclusiveSharedRule();
  switch (rule) {
    case RULE_FAIL:
      this->UndoLocking(context_, request);
      break;
    case RULE_POLL:
      request->read_target = EXCLUSIVE;
      this->ReadRemotely(context_,
          request->seq_no,
          request->user_id,
          request->read_target,
          request->lock_type,
          request->obj_index);
      break;
    case RULE_QUEUE:
      request->read_target = EXCLUSIVE;
      if ((request->exclusive & waitlist_[request->obj_index]) == 0) {
        waitlist_[request->obj_index] = 0;
      }
      if (waitlist_[request->obj_index] == 0) {
        user_waiters_[request->user_id] = request->exclusive;
        request->read_target = EXCLUSIVE;
        this->ReadRemotely(context_,
          request->seq_no,
            request->user_id,
            request->read_target,
            request->lock_type,
            request->obj_index);
      } else {
        // lock acquisition failed, undoing FA
        this->UndoLocking(context_, request);
      }
      break;
    default:
      cerr << "Unsupported Exclusive -> Shared rule: " <<  rule << endl;
      return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

// Handle Exclusive -> Exclusive
int LockClient::HandleExclusiveToExclusive(LockRequest* request) {
  int rule = LockManager::GetExclusiveExclusiveRule();
  switch (rule) {
    case RULE_FAIL:
      this->UndoLocking(context_, request);
      break;
    case RULE_POLL:
      this->UndoLocking(context_, request, true);
      request->read_target = EXCLUSIVE;
      this->ReadRemotely(context_,
          request->seq_no,
          request->user_id,
          request->read_target,
          request->lock_type,
          request->obj_index);
      break;
    case RULE_QUEUE:
      request->read_target = EXCLUSIVE;
      if ((request->exclusive & waitlist_[request->obj_index]) == 0) {
        waitlist_[request->obj_index] = 0;
      }
      if (waitlist_[request->obj_index] == 0) {
        user_waiters_[request->user_id] = request->exclusive;
        request->read_target = EXCLUSIVE;
        this->ReadRemotely(context_,
            request->seq_no,
            request->user_id,
            request->read_target,
            request->lock_type,
            request->obj_index);
      } else {
        // lock acquisition failed, undoing FA
        this->UndoLocking(context_, request);
      }
      break;
    default:
      cerr << "Unsupported Exclusive -> Exclusive rule: " <<  rule << endl;
      return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

int LockClient::UndoLocking(Context* context, LockRequest* request, bool polling) {
  user_fail_[request->user_id] = true;
  user_polling_[request->user_id] = polling;
  this->UnlockRemotely(context,
      request->seq_no,
      request->user_id,
      request->lock_type,
      request->obj_index,
      true, polling
      );
  return FUNC_SUCCESS;
}

int LockClient::PollSharedToExclusive(LockRequest* request) {
  ++user_retry_count_[request->user_id];
  if (user_retry_count_[request->user_id] > LockManager::GetPollRetry()) {
    this->UndoLocking(context_, request);
    return FUNC_SUCCESS;
  }

  int rule = LockManager::GetSharedExclusiveRule();
  switch (rule) {
    case RULE_POLL:
      this->ReadRemotely(context_,
          request->seq_no,
          request->user_id,
          request->read_target,
          request->lock_type,
          request->obj_index);
      break;
    default:
      cerr << "Unsupported Shared -> Exclusive rule for polling: " <<  rule << endl;
      return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

int LockClient::PollExclusiveToShared(LockRequest* request) {
  ++user_retry_count_[request->user_id];
  if (user_retry_count_[request->user_id] > LockManager::GetPollRetry()) {
    this->UndoLocking(context_, request);
    return FUNC_SUCCESS;
  }
  int rule = LockManager::GetExclusiveSharedRule();
  uint32_t value = *request->read_buffer;
  switch (rule) {
    case RULE_POLL:
      if (value == 0) {
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
        this->ReadRemotely(context_,
            request->seq_no,
            request->user_id,
            request->read_target,
            request->lock_type,
            request->obj_index);
      }
      break;
    case RULE_QUEUE:
      if ((value & user_waiters_[request->user_id]) == 0) {
        local_manager_->NotifyLockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            RESULT_SUCCESS);
      } else {
        this->ReadRemotely(context_,
            request->seq_no,
            request->user_id,
            request->read_target,
            request->lock_type,
            request->obj_index);
      }
      break;
    default:
      cerr << "Unsupported Exclusive -> Shared rule for polling: " <<  rule << endl;
      return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

int LockClient::PollExclusiveToExclusive(LockRequest* request) {
  int rule = LockManager::GetExclusiveExclusiveRule();
  ++user_retry_count_[request->user_id];
  if (++user_retry_count_[request->user_id] > LockManager::GetPollRetry() &&
      rule == RULE_QUEUE) {
    this->UndoLocking(context_, request);
    return FUNC_SUCCESS;
  }
  uint32_t value = *request->read_buffer;
  switch (rule) {
    case RULE_POLL:
      if (value == 0) {
        //local_manager_->NotifyLockRequestResult(context->last_user_id,
            //context->last_lock_type,
            //context->last_obj_index,
            //LockManager::RESULT_SUCCESS);
        this->LockRemotely(context_,
            request->seq_no,
            request->user_id,
            request->lock_type,
            request->obj_index);
      } else {
        this->ReadRemotely(context_,
            request->seq_no,
            request->user_id,
            request->read_target,
            request->lock_type,
            request->obj_index);
      }
      break;
    case RULE_QUEUE:
      if ((value & user_waiters_[request->user_id]) == 0) {
        local_manager_->NotifyLockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            remote_lm_id_,
            request->obj_index,
            RESULT_SUCCESS);
      } else {
        this->ReadRemotely(context_,
            request->seq_no,
            request->user_id,
            request->read_target,
            request->lock_type,
            request->obj_index);
      }
      break;
    default:
      cerr << "Unsupported Exclusive -> Exclusive rule for polling: " <<  rule << endl;
      return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

// Requests lock mode of lock manager via IBV_WR_SEND op.
int LockClient::SendLockModeRequest(Context* context) {

  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

  Message* msg = context->send_message_buffer->GetMessage();

  msg->type       = Message::LOCK_MODE_REQUEST;
  msg->manager_id = local_manager_->GetID();

  if (SendMessage(context)) {
    cerr << "SendLockModeRequest(): SendMessage() failed." << endl;
    return FUNC_FAIL;
  }

  return FUNC_SUCCESS;
}

// Requests lock table MR region of from lock manager via IBV_WR_SEND op.
int LockClient::SendLockTableRequest(Context* context) {

  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

  Message* msg = context->send_message_buffer->GetMessage();

  msg->type       = Message::LOCK_TABLE_MR_REQUEST;
  msg->manager_id = local_manager_->GetID();

  if (SendMessage(context)) {
    cerr << "SendLockTableRequest(): SendMessage() failed." << endl;
    return -1;
  }

  return 0;
}

int LockClient::RequestLock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
    int lock_mode) {
  user_retry_count_[user_id] = 0;
  user_fail_[user_id]        = false;
  user_polling_[user_id]     = false;
  user_waiters_[user_id]     = 0;
  user_all_waiters_[user_id] = 0;

  //context_->fail        = false;
  //context_->polling     = false;
  //context_->retry       = 0;
  //context_->waiters     = 0;
  //context_->all_waiters = 0;
  if (lock_mode == LOCK_PROXY_RETRY ||
      lock_mode == LOCK_PROXY_QUEUE) {
    // ask lock manager to place the lock
    return this->SendLockRequest(context_, seq_no, user_id, lock_type, obj_index);
  } else if (lock_mode == LOCK_REMOTE_POLL ||
      lock_mode == LOCK_REMOTE_NOTIFY || lock_mode == LOCK_REMOTE_QUEUE) {
    // try locking remotely
    return this->LockRemotely(context_, seq_no, user_id, lock_type, obj_index);
  } else {
    cerr << "RequestLock(): Unknown lock mode: " << lock_mode << endl;
  }
  return FUNC_SUCCESS;
}

int LockClient::RequestUnlock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
    int lock_mode) {
  if (lock_mode == LOCK_PROXY_RETRY ||
      lock_mode == LOCK_PROXY_QUEUE) {
    return this->SendUnlockRequest(context_, seq_no, user_id, lock_type, obj_index);
  } else if (lock_mode == LOCK_REMOTE_POLL ||
      lock_mode == LOCK_REMOTE_NOTIFY ||
      lock_mode == LOCK_REMOTE_QUEUE) {
    return this->UnlockRemotely(context_, seq_no, user_id, lock_type, obj_index);
  } else {
    cerr << "RequestUnlock(): Unknown lock mode: " << lock_mode << endl;
  }
  return FUNC_SUCCESS;
}

int LockClient::LockRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type,
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

  //context->last_seq_no    = seq_no;
  //context->last_user_id   = user_id;
  //context->last_lock_type = lock_type;
  //context->last_obj_index = obj_index;
  //context->last_lock_task = LockManager::TASK_LOCK;

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
  //send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED | IBV_EXP_SEND_INLINE;
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

int LockClient::ReadRemotely(Context* context, int seq_no, uint32_t user_id, int read_target,
    int lock_type, int obj_index) {


  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  //context->last_user_id     = user_id;
  //context->last_obj_index   = obj_index;
  //context->last_read_target = read_target;
  //context->read_purpose     = READ_POLLING;

  pthread_mutex_lock(&lock_mutex_);
  LockRequest* request   = lock_requests_[lock_request_idx_];
  request->seq_no        = seq_no;
  request->owner_node_id = local_owner_id_;
  request->user_id       = user_id;
  request->read_target   = read_target;
  request->obj_index     = obj_index;
  request->lock_type     = lock_type;
  request->task          = TASK_READ;
  lock_request_idx_      = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr   = (uint64_t)request->read_buffer;
  sge.length = sizeof(uint32_t);
  sge.lkey   = request->read_buffer_mr->lkey;

  send_work_request.wr_id          = (uint64_t)request;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode     = IBV_EXP_WR_RDMA_READ;

  send_work_request.wr.rdma.rkey = context->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  if (read_target == LockManager::EXCLUSIVE) {
    // reading exclusive portion of the lock object
    // add 4 bytes here because of BIG-ENDIAN?
    send_work_request.wr.rdma.remote_addr += 4;
  } // otherwise, read exclusive portion.

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_read_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ReadRemotely(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  ++num_rdma_read_;
  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

// read both exclusive and shared portions of the lock object
int LockClient::ReadRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type,
    int obj_index) {
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  //context->last_user_id   = user_id;
  //context->last_obj_index = obj_index;
  //context->read_purpose   = READ_NOTIFYING;

  pthread_mutex_lock(&lock_mutex_);
  LockRequest* request = lock_requests_[lock_request_idx_];
  request->seq_no      = seq_no;
  request->user_id     = user_id;
  request->lock_type   = lock_type;
  request->obj_index   = obj_index;
  request->read_target = ALL;
  request->task        = TASK_READ;
  lock_request_idx_    = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr   = (uint64_t)request->read_buffer2;
  sge.length = sizeof(uint64_t);
  sge.lkey   = request->read_buffer2_mr->lkey;

  send_work_request.wr_id          = (uint64_t)request;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode     = IBV_EXP_WR_RDMA_READ;

  send_work_request.wr.rdma.rkey = context->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_read_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ReadRemotely(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  ++num_rdma_read_;
  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}


int LockClient::UnlockRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type,
    int obj_index, bool is_undo, bool retry) {

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

  //context->last_seq_no    = seq_no;
  //context->last_user_id   = user_id;
  //context->last_lock_type = lock_type;
  //context->last_obj_index = obj_index;
  //context->last_lock_task = LockManager::TASK_UNLOCK;

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

//int LockClient::SendSwitchToLocal() {
  //pthread_mutex_lock(&lock_mutex_);
  //context_->send_message->type = Message::SWITCH_TO_LOCAL;
  //context_->send_message->manager_id = local_manager_->GetID();

  //if (SendMessage(context_)) {
    //cerr << "SendSwitchToLocal(): SendMessage() failed." << endl;
    //pthread_mutex_unlock(&lock_mutex_);
    //return -1;
  //}

  //pthread_mutex_unlock(&lock_mutex_);
//}

//int LockClient::SendSwitchToRemote() {
  //pthread_mutex_lock(&lock_mutex_);
  //context_->send_message->type = Message::SWITCH_TO_REMOTE;
  //context_->send_message->manager_id = local_manager_->GetID();

  //if (SendMessage(context_)) {
    //cerr << "SendSwitchToRemote(): SendMessage() failed." << endl;
    //pthread_mutex_unlock(&lock_mutex_);
    //return -1;
  //}

  //pthread_mutex_unlock(&lock_mutex_);
//}

int LockClient::SendLockRequest(Context* context, int seq_no,
    uint32_t user_id, int lock_type, int obj_index) {

  pthread_mutex_lock(&lock_mutex_);
  while (message_in_progress_) {
    pthread_cond_wait(&lock_cond_, &lock_mutex_);
  }


  Message* msg = context->send_message_buffer->GetMessage();

  msg->type           = Message::LOCK_REQUEST;
  msg->seq_no         = seq_no;
  msg->target_node_id = remote_lm_id_;
  msg->owner_node_id  = local_owner_id_;
  msg->lock_type      = lock_type;
  msg->obj_index      = obj_index;
  msg->owner_user_id  = user_id;

  message_in_progress_ = true;

  if (SendMessage(context)) {
    cerr << "SendLockRequest(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  //cout << "SendLockRequest(): lock request sent." << endl;
  return 0;
}

int LockClient::SendUnlockRequest(Context* context, int seq_no,
    uint32_t user_id, int lock_type, int obj_index) {

  pthread_mutex_lock(&lock_mutex_);
  while (message_in_progress_) {
    pthread_cond_wait(&lock_cond_, &lock_mutex_);
  }

  Message* msg = context->send_message_buffer->GetMessage();

  msg->type           = Message::UNLOCK_REQUEST;
  msg->seq_no         = seq_no;
  msg->target_node_id = remote_lm_id_;
  msg->owner_node_id  = local_owner_id_;
  msg->lock_type      = lock_type;
  msg->obj_index      = obj_index;
  msg->owner_user_id  = user_id;

  message_in_progress_ = true;

  if (SendMessage(context)) {
    pthread_mutex_unlock(&lock_mutex_);
    cerr << "SendUnlockRequest(): SendMessage() failed." << endl;
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  //cout << "SendUnlockRequest(): memory region sent." << endl;
  return 0;
}

double LockClient::GetAverageRemoteSharedLockTime() const {
  return num_shared_lock_ > 0 ?
    total_shared_lock_remote_time_ / num_shared_lock_ : 0;
}

double LockClient::GetAverageRemoteExclusiveLockTime() const {
  return num_exclusive_lock_ > 0 ?
    total_exclusive_lock_remote_time_ / num_exclusive_lock_ : 0;
}

}}
