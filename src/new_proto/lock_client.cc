#include "lock_client.h"

namespace rdma { namespace proto {

// constructor
LockClient::LockClient(const string& work_dir, LockManager* local_manager,
    LockSimulator* local_user,
    int remote_lm_id) : Client(work_dir, local_manager, local_user, remote_lm_id) {
}

// destructor
LockClient::~LockClient() {
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
  Context* context = (Context *)work_completion->wr_id;

  if (work_completion->status != IBV_WC_SUCCESS) {
    if (work_completion->opcode == IBV_WC_COMP_SWAP ||
        work_completion->opcode == IBV_WC_FETCH_ADD) {
      SendLockModeRequest(context);
      local_manager_->NotifyLockRequestResult(context->last_user_id,
          context->last_lock_type,
          context->last_obj_index,
          LockManager::RESULT_FAILURE);
      return 0;
    }
    else {
      cerr << "Work completion status is not IBV_WC_SUCCESS: " <<
        work_completion->status << endl;
      return -1;
    }
  }

  if (work_completion->opcode == IBV_WC_RECV) {

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
    } else if (message->type == Message::LOCK_MODE) {
      local_manager_->UpdateLockModeTable(
          message->manager_id,
          message->lock_mode
          );
    } else if (message->type == Message::LOCK_REQUEST_RESULT) {
      //cout << "received lock request result." << endl;
      local_manager_->NotifyLockRequestResult(message->user_id,
          message->lock_type,
          message->obj_index,
          message->lock_result);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      //cout << "received unlock request result" << endl;
      local_manager_->NotifyUnlockRequestResult(
          message->user_id,
          message->lock_type,
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
  } else if (work_completion->opcode == IBV_WC_COMP_SWAP) {
    // We don't see this for Algorithm 1
    // completion of compare-and-swap, i.e. remote exclusive locking

    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_remote_exclusive_lock_);
    double time_taken = ((double)end_remote_exclusive_lock_.tv_sec * 1e+9 +
        (double)end_remote_exclusive_lock_.tv_nsec) -
      ((double)start_remote_exclusive_lock_.tv_sec * 1e+9 +
          (double)start_remote_exclusive_lock_.tv_nsec);
    total_exclusive_lock_remote_time_ += time_taken;
    ++num_exclusive_lock_;

    uint64_t prev_value = *context->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value)>>32);
    shared = (uint32_t)value;

    if (context->last_lock_task == LockManager::TASK_LOCK) {
      // it should have been successful since exclusive and shared was 0
      if (exclusive == 0 && shared == 0) {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_FAILURE);
      }
    } else if (context->last_lock_task == LockManager::TASK_UNLOCK) {
      if (exclusive == context->last_user_id && shared == 0) {
        local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else if (exclusive == context->last_user_id && shared != 0) {
        local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_RETRY);
      } else {
        local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_FAILURE);
      }
    }
  } else if (work_completion->opcode == IBV_WC_FETCH_ADD) {
    // completion of fetch-and-add, i.e. remote shared/exclusive locking

    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_remote_shared_lock_);
    double time_taken = ((double)end_remote_shared_lock_.tv_sec * 1e+9 +
        (double)end_remote_shared_lock_.tv_nsec) -
      ((double)start_remote_shared_lock_.tv_sec * 1e+9 +
          (double)start_remote_shared_lock_.tv_nsec);
    total_shared_lock_remote_time_ += time_taken;
    ++num_shared_lock_;

    uint64_t prev_value = *context->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value)>>32);
    shared = (uint32_t)value;

    context->exclusive = exclusive;
    context->shared = shared;

    if (context->last_lock_task == LockManager::TASK_LOCK) {
      if (context->last_lock_type == LockManager::EXCLUSIVE) {
        if (exclusive == 0 && shared == 0) {
          // exclusive lock acquisition successful
          local_manager_->NotifyLockRequestResult(context->last_user_id,
              context->last_lock_type,
              context->last_obj_index,
              LockManager::RESULT_SUCCESS);
        } else if (exclusive == 0 && shared != 0) {
          // shared lock exists, polling on shared portion of the lock object
          this->HandleSharedToExclusive(context);
        } else if (exclusive != 0 && shared == 0) {
          // exclusive lock exists, wait for others
          this->HandleExclusiveToExclusive(context);
        } else {
          // lock acquisition failed, undoing FA
          this->UndoLocking(context);
        }
      } else {
        // shared lock
        if (exclusive == 0) {
          // it should have been successful since exclusive and shared was 0
          local_manager_->NotifyLockRequestResult(context->last_user_id,
              context->last_lock_type,
              context->last_obj_index,
              LockManager::RESULT_SUCCESS);
        } else if (exclusive != 0 && shared == 0){
          // exclusive lock exists
          this->HandleExclusiveToShared(context);
        } else {
          this->UndoLocking(context);
        }
      }
    } else if (context->last_lock_task == TASK_UNLOCK) {
      if (context->fail) {
        context->fail = false;
        if (context->polling) {
          context->polling = false;
        } else {
          ++context->retry;
          if (context->retry > LockManager::GetPollRetry()) {
            local_manager_->NotifyLockRequestResult(context->last_user_id,
                context->last_lock_type,
                context->last_obj_index,
                RESULT_FAILURE);
          } else {
            local_manager_->NotifyLockRequestResult(context->last_user_id,
                context->last_lock_type,
                context->last_obj_index,
                RESULT_RETRY);
          }
        }
      } else {
        if (context->last_lock_type == EXCLUSIVE) {
          waitlist_[context->last_obj_index] = (exclusive - context->last_user_id);
        } else {
          waitlist_[context->last_obj_index] = exclusive;
        }
        // unlock always succeeds. (really?)
        local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_SUCCESS);
      }
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    // polling result
    uint32_t value = *context->read_buffer;
//#if __BYTE_ORDER == __LITTLE_ENDIAN
    //uint32_t value = __bswap_constant_32(prev_value);  // Compiler builtin
//#endif
    if (context->last_read_target == SHARED) {
      // Polling on Sh_X -> proceed if value is zero
      if (value == 0) {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        // otherwise, read/poll again (shared -> exclusive)
        this->PollSharedToExclusive(context);
      }
    } else {
      if (context->last_lock_type == EXCLUSIVE) {
        // exclusive -> exclusive
        if (context->retry > LockManager::GetPollRetry()) {
          local_manager_->NotifyLockRequestResult(context->last_user_id,
              context->last_lock_type,
              context->last_obj_index,
              RESULT_FAILURE);
        } else {
          this->PollExclusiveToExclusive(context);
        }
      } else {
        // exclusive -> shared
        this->PollExclusiveToShared(context);
      }
    }
  }

  return 0;
}

// Handle Shared -> Exclusive
int LockClient::HandleSharedToExclusive(Context* context) {
  int rule = LockManager::GetSharedExclusiveRule();
  switch (rule) {
    case RULE_FAIL:
      this->UndoLocking(context);
      break;
    case RULE_POLL:
      context->last_read_target = SHARED;
      this->ReadRemotely(context,
          context->last_user_id,
          context->last_read_target,
          context->last_obj_index);
      break;
    default:
      cerr << "Unsupported Shared -> Exclusive rule: " <<  rule << endl;
      return -1;
  }

  return 0;
}

// Handle Exclusive -> Shared
int LockClient::HandleExclusiveToShared(Context* context) {
  int rule = LockManager::GetExclusiveSharedRule();
  switch (rule) {
    case RULE_FAIL:
      this->UndoLocking(context);
      break;
    case RULE_POLL:
      context->last_read_target = EXCLUSIVE;
      this->ReadRemotely(context,
          context->last_user_id,
          context->last_read_target,
          context->last_obj_index);
      break;
    case RULE_QUEUE:
      context->last_read_target = EXCLUSIVE;
      if ((context->exclusive & waitlist_[context->last_obj_index]) == 0) {
        waitlist_[context->last_obj_index] = 0;
      }
      if (waitlist_[context->last_obj_index] == 0) {
        context->waiters = context->exclusive;
        context->last_read_target = EXCLUSIVE;
        this->ReadRemotely(context,
            context->last_user_id,
            context->last_read_target,
            context->last_obj_index);
      } else {
        // lock acquisition failed, undoing FA
        this->UndoLocking(context);
      }
      break;
    default:
      cerr << "Unsupported Exclusive -> Shared rule: " <<  rule << endl;
      return -1;
  }
}

// Handle Exclusive -> Exclusive
int LockClient::HandleExclusiveToExclusive(Context* context) {
  int rule = LockManager::GetExclusiveExclusiveRule();
  switch (rule) {
    case RULE_FAIL:
      this->UndoLocking(context);
      break;
    case RULE_POLL:
      this->UndoLocking(context, true);
      context->last_read_target = EXCLUSIVE;
      this->ReadRemotely(context,
          context->last_user_id,
          context->last_read_target,
          context->last_obj_index);
      break;
    case RULE_QUEUE:
      context->last_read_target = EXCLUSIVE;
      if ((context->exclusive & waitlist_[context->last_obj_index]) == 0) {
        waitlist_[context->last_obj_index] = 0;
      }
      if (waitlist_[context->last_obj_index] == 0) {
        context->waiters = context->exclusive;
        context->last_read_target = EXCLUSIVE;
        this->ReadRemotely(context,
            context->last_user_id,
            context->last_read_target,
            context->last_obj_index);
      } else {
        // lock acquisition failed, undoing FA
        this->UndoLocking(context);
      }
      break;
    default:
      cerr << "Unsupported Exclusive -> Exclusive rule: " <<  rule << endl;
      return -1;
  }
  return 0;
}

int LockClient::UndoLocking(Context* context, bool polling) {
  context->fail = true;
  context->polling = polling;
  this->UnlockRemotely(context,
      context->last_user_id,
      context->last_lock_type,
      context->last_obj_index
      );
  return 0;
}

int LockClient::PollSharedToExclusive(Context* context) {
  ++context->retry;
  if (context->retry > LockManager::GetPollRetry()) {
    this->UndoLocking(context);
    return 0;
  }

  int rule = LockManager::GetSharedExclusiveRule();
  switch (rule) {
    case RULE_POLL:
      this->ReadRemotely(context,
          context->last_user_id,
          context->last_read_target,
          context->last_obj_index);
      break;
    default:
      cerr << "Unsupported Shared -> Exclusive rule for polling: " <<  rule << endl;
      return -1;
  }
  return 0;
}

int LockClient::PollExclusiveToShared(Context* context) {
  ++context->retry;
  if (context->retry > LockManager::GetPollRetry()) {
    this->UndoLocking(context);
    return 0;
  }
  int rule = LockManager::GetExclusiveSharedRule();
  uint32_t value = *context->read_buffer;
  switch (rule) {
    case RULE_POLL:
      if (value == 0) {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        this->ReadRemotely(context,
            context->last_user_id,
            context->last_read_target,
            context->last_obj_index);
      }
      break;
    case RULE_QUEUE:
      if ((value & context->waiters) == 0) {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            RESULT_SUCCESS);
      } else {
        this->ReadRemotely(context,
            context->last_user_id,
            context->last_read_target,
            context->last_obj_index);
      }
      break;
    default:
      cerr << "Unsupported Exclusive -> Shared rule for polling: " <<  rule << endl;
      return -1;
  }
  return 0;
}

int LockClient::PollExclusiveToExclusive(Context* context) {
  int rule = LockManager::GetExclusiveExclusiveRule();
  ++context->retry;
  if (context->retry > LockManager::GetPollRetry() && rule == RULE_QUEUE) {
    this->UndoLocking(context);
    return 0;
  }
  uint32_t value = *context->read_buffer;
  switch (rule) {
    case RULE_POLL:
      if (value == 0) {
        //local_manager_->NotifyLockRequestResult(context->last_user_id,
            //context->last_lock_type,
            //context->last_obj_index,
            //LockManager::RESULT_SUCCESS);
        this->LockRemotely(context,
           context->last_user_id,
           context->last_lock_type,
           context->last_obj_index);
      } else {
        this->ReadRemotely(context,
            context->last_user_id,
            context->last_read_target,
            context->last_obj_index);
      }
      break;
    case RULE_QUEUE:
      if ((value & context->waiters) == 0) {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            RESULT_SUCCESS);
      } else {
        this->ReadRemotely(context,
            context->last_user_id,
            context->last_read_target,
            context->last_obj_index);
      }
      break;
    default:
      cerr << "Unsupported Exclusive -> Exclusive rule for polling: " <<  rule << endl;
      return -1;
  }
  return 0;
}

// Requests lock mode of lock manager via IBV_WR_SEND op.
int LockClient::SendLockModeRequest(Context* context) {

  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

  Message* msg = context->send_message_buffer->GetMessage();

  msg->type       = Message::LOCK_MODE_REQUEST;
  msg->manager_id = local_manager_->GetID();
  msg->user_id    = local_user_->GetID();

  if (SendMessage(context)) {
    cerr << "SendLockModeRequest(): SendMessage() failed." << endl;
    return -1;
  }

  return 0;
}

// Requests lock table MR region of from lock manager via IBV_WR_SEND op.
int LockClient::SendLockTableRequest(Context* context) {

  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

  Message* msg = context->send_message_buffer->GetMessage();

  msg->type       = Message::LOCK_TABLE_MR_REQUEST;
  msg->manager_id = local_manager_->GetID();
  msg->user_id    = local_user_->GetID();

  if (SendMessage(context)) {
    cerr << "SendLockTableRequest(): SendMessage() failed." << endl;
    return -1;
  }

  return 0;
}

int LockClient::RequestLock(int user_id, int lock_type, int obj_index,
    int lock_mode) {
  context_->fail = false;
  context_->polling = false;
  context_->retry = 0;
  if (lock_mode == LockManager::LOCK_LOCAL) {
    // ask lock manager to place the lock
    return this->SendLockRequest(context_, user_id, lock_type, obj_index);
  } else if (lock_mode == LockManager::LOCK_REMOTE) {
    // try locking remotely
    return this->LockRemotely(context_, user_id, lock_type, obj_index);
  } else {
    cerr << "RequestLock(): Unknown lock mode: " << lock_mode << endl;
  }
}

int LockClient::RequestUnlock(int user_id, int lock_type, int obj_index,
    int lock_mode) {
  if (lock_mode == LockManager::LOCK_LOCAL) {
    return this->SendUnlockRequest(context_, user_id, lock_type, obj_index);
  } else if (lock_mode == LockManager::LOCK_REMOTE) {
    return this->UnlockRemotely(context_, user_id, lock_type, obj_index);
  } else {
    cerr << "RequestUnlock(): Unknown lock mode: " << lock_mode << endl;
  }
}

int LockClient::LockRemotely(Context* context, int user_id, int lock_type,
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

  context->last_user_id   = user_id;
  context->last_lock_type = lock_type;
  context->last_obj_index = obj_index;
  context->last_lock_task = LockManager::TASK_LOCK;

  sge.addr   = (uint64_t)context->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = context->original_value_mr->lkey;

  send_work_request.wr_id          = (uint64_t)context;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode     = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  if (lock_type == LockManager::SHARED) {
    send_work_request.wr.atomic.compare_add = 1;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    exclusive = user_id;
    shared = 0;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    send_work_request.wr.atomic.compare_add = new_value;
  }

  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  send_work_request.wr.atomic.rkey        =
    context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "LockRemotely(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  ++num_rdma_atomic_;

  return 0;
}

int LockClient::ReadRemotely(Context* context, int user_id, int read_target, int obj_index) {
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  context->last_user_id     = user_id;
  context->last_obj_index   = obj_index;
  context->last_read_target = read_target;

  sge.addr   = (uint64_t)context->read_buffer;
  sge.length = sizeof(uint32_t);
  sge.lkey   = context->read_buffer_mr->lkey;

  send_work_request.wr_id          = (uint64_t)context;
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

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ReadRemotely(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  ++num_rdma_read_;
  return 0;
}


int LockClient::UnlockRemotely(Context* context, int user_id, int lock_type,
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

  context->last_user_id   = user_id;
  context->last_lock_type = lock_type;
  context->last_obj_index = obj_index;
  context->last_lock_task = LockManager::TASK_UNLOCK;

  sge.addr   = (uint64_t)context->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = context->original_value_mr->lkey;

  send_work_request.wr_id          = (uint64_t)context;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode     = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  if (lock_type == LockManager::SHARED) {
    send_work_request.wr.atomic.compare_add = -1;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    exclusive = 0;
    shared = 0;
    uint64_t new_value = ((uint64_t)user_id) << 32 | shared;
    new_value = (-1) * new_value; // need to subtract for unlock
    send_work_request.wr.atomic.compare_add = new_value;
  }
  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  send_work_request.wr.atomic.rkey        =
    context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "UnlockRemotely(): ibv_exp_post_send() failed: " << strerror(ret) <<
      endl;
    return -1;
  }
  ++num_rdma_atomic_;
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

int LockClient::SendLockRequest(Context* context, int user_id,
    int lock_type, int obj_index) {

  Message* msg = context->send_message_buffer->GetMessage();

  pthread_mutex_lock(&lock_mutex_);
  msg->type = Message::LOCK_REQUEST;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->user_id = user_id;

  if (SendMessage(context)) {
    cerr << "SendLockRequest(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  //cout << "SendLockRequest(): lock request sent." << endl;
  return 0;
}

int LockClient::SendUnlockRequest(Context* context, int user_id,
    int lock_type, int obj_index) {

  Message* msg = context->send_message_buffer->GetMessage();

  pthread_mutex_lock(&lock_mutex_);
  msg->type = Message::UNLOCK_REQUEST;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->user_id = user_id;
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

uint64_t LockClient::GetRDMAReadCount() const {
  return num_rdma_read_;
}

uint64_t LockClient::GetRDMAAtomicCount() const {
  return num_rdma_atomic_;
}

}}
