#include "lock_client.h"

namespace rdma {
namespace proto {

// constructor
LockClient::LockClient(const string& work_dir, LockManager* local_manager,
                       uint32_t local_user_count, uint32_t remote_lm_id)
    : Client(work_dir, local_manager, local_user_count, remote_lm_id) {
  message_in_progress_ = false;
}

// destructor
LockClient::~LockClient() {}

int LockClient::HandleConnection(Context* context) {
  // cout << "connected to server." << endl;
  context->connected = true;

  SendLockTableRequest(context);

  return 0;
}

int LockClient::HandleDisconnect(Context* context) {
  if (context->original_value_mr) ibv_dereg_mr(context->original_value_mr);

  delete context;

  return 0;
}

int LockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {
  if (work_completion->status != IBV_WC_SUCCESS) {
    // if (work_completion->opcode == IBV_WC_COMP_SWAP ||
    // work_completion->opcode == IBV_WC_FETCH_ADD) {
    // SendLockModeRequest(context);
    // local_manager_->NotifyLockRequestResult(
    // context->last_seq_no,
    // context->last_user_id,
    // context->last_lock_type,
    // context->last_obj_index,
    // LockManager::RESULT_FAILURE);
    // return 0;
    //}
    // else {
    // cerr << "Work completion status is not IBV_WC_SUCCESS: " <<
    // work_completion->status << endl;
    // return -1;
    //}
    cerr << "(LockClient) Work completion status is not IBV_WC_SUCCESS: "
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
      // Poco::Mutex::ScopedLock lock(lock_mutex_);
      //{
      // Poco::Mutex::ScopedLock lock(lock_mutex_);
      // cout << "received lock request result." << endl;
      // lock_cond_.signal();
      //}
      message_in_progress_ = false;

      local_manager_->NotifyLockRequestResult(
          message->seq_no, message->owner_user_id, message->lock_type,
          remote_lm_id_, message->obj_index, 0, message->lock_result);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      // cout << "received unlock request result" << endl;
      //{
      // Poco::Mutex::ScopedLock lock(lock_mutex_);
      // cout << "received unlock request result." << endl;
      // lock_cond_.signal();
      //}
      message_in_progress_ = false;

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
  } else if (work_completion->opcode == IBV_WC_COMP_SWAP) {
    // We don't see this for new system, which explicitly use FA instead
    // completion of compare-and-swap, i.e. remote exclusive locking

    LockRequest* request = (LockRequest*)work_completion->wr_id;
    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_remote_exclusive_lock_);
    double time_taken = ((double)end_remote_exclusive_lock_.tv_sec * 1e+9 +
                         (double)end_remote_exclusive_lock_.tv_nsec) -
                        ((double)start_remote_exclusive_lock_.tv_sec * 1e+9 +
                         (double)start_remote_exclusive_lock_.tv_nsec);
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

    if (request->task == LOCK) {
      // it should have been successful since exclusive and shared was 0
      if (exclusive == 0 && shared == 0) {
        ++total_lock_success_;
        total_exclusive_lock_remote_time_ += time_taken;
        ++num_exclusive_lock_;
        user_retry_count_[request->user_id] = 0;
        local_manager_->NotifyLockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, request->contention_count,
            SUCCESS);
      } else {
        ++request->contention_count;
        ++user_retry_count_[request->user_id];
        if (user_retry_count_[request->user_id] >=
            LockManager::GetFailRetry()) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              FAILURE);
        } else {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              RETRY);
        }
      }
    } else if (request->task == UNLOCK) {
      if (exclusive == request->user_id && shared == 0) {
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, SUCCESS);
      } else if (exclusive == request->user_id && shared != 0) {
        ++request->contention_count;
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, RETRY);
      } else {
        ++request->contention_count;
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, FAILURE);
      }
    }
  } else if (work_completion->opcode == IBV_WC_FETCH_ADD) {
    // completion of fetch-and-add, i.e. remote shared locking

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
        cerr << "FA should not be used for an exclusive lock." << endl;
        exit(ERROR_FA_FOR_EXCLUSIVE);
        // if (exclusive == 0 && shared == 0) {
        //// exclusive lock acquisition successful
        //++total_lock_success_;
        // total_exclusive_lock_remote_time_ += time_taken;
        //++num_exclusive_lock_;

        // user_retry_count_[request->user_id] = 0;
        // local_manager_->NotifyLockRequestResult(
        // request->seq_no, request->user_id, request->lock_type,
        // remote_lm_id_, request->obj_index, request->contention_count,
        // SUCCESS);
        //} else {
        //++total_lock_contention_;
        //++request->contention_count;
        // if (shared == 0 && exclusive != 0) {
        // this->HandleExclusiveToExclusive(request);
        //} else if (shared != 0 && exclusive == 0) {
        // this->HandleSharedToExclusive(request);
        //} else {
        // this->UndoLocking(context_, *request);
        //}
        //}
      } else {
        // shared lock
        if (exclusive == 0) {
          // it should have been successful since exclusive and shared was 0
          ++total_lock_success_;
          total_shared_lock_remote_time_ += time_taken;
          ++num_shared_lock_;
          user_retry_count_[request->user_id] = 0;
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
        } else if (exclusive != 0 && shared == 0) {
          // exclusive lock exists
          ++total_lock_contention_;
          ++request->contention_count;
          this->HandleExclusiveToShared(request);
        } else {
          ++total_lock_contention_;
          ++request->contention_count;
          this->UndoLocking(context_, *request);
        }
      }
    } else if (request->task == UNLOCK) {
      if (user_fail_[request->user_id]) {
        user_fail_[request->user_id] = false;
        if (user_polling_[request->user_id]) {
          user_polling_[request->user_id] = false;
        } else {
          ++user_retry_count_[request->user_id];
          if (user_retry_count_[request->user_id] >=
              LockManager::GetPollRetry()) {
            user_retry_count_[request->user_id] = 0;
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                FAILURE);
          } else {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                RETRY);
          }
        }
      } else {
        waitlist_[request->obj_index] = exclusive;
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, SUCCESS);
      }
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    LockRequest* request = (LockRequest*)work_completion->wr_id;

    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_rdma_read_);
    double time_taken = ((double)end_rdma_read_.tv_sec * 1e+9 +
                         (double)end_rdma_read_.tv_nsec) -
                        ((double)start_rdma_read_.tv_sec * 1e+9 +
                         (double)start_rdma_read_.tv_nsec);
    total_rdma_read_time_ += time_taken;
    // polling result
    uint64_t value = request->read_buffer2;
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value) >> 32);
    shared = (uint32_t)value;
    request->exclusive = exclusive;
    request->shared = shared;
    if (request->read_target == READ_SHARED) {
      // Polling on Sh_X -> proceed if value is zero
      if (shared == 0) {
        user_retry_count_[request->user_id] = 0;
        local_manager_->NotifyLockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, request->contention_count,
            SUCCESS);
      } else {
        // otherwise, read/poll again (shared -> exclusive)
        this->PollSharedToExclusive(request);
      }
    } else {
      if (request->lock_type == EXCLUSIVE) {
        // exclusive -> exclusive
        if (user_retry_count_[request->user_id] >=
            LockManager::GetPollRetry()) {
          user_retry_count_[request->user_id] = 0;
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              FAILURE);
        } else {
          this->PollExclusiveToExclusive(request);
        }
      } else {
        // exclusive -> shared
        this->PollExclusiveToShared(request);
      }
    }
  }

  return 0;
}

// Handle Shared -> Exclusive
int LockClient::HandleSharedToExclusive(LockRequest* request) {
  const string& rule = LockManager::GetSharedExclusiveRule();
  if (rule == "fail") {
    this->UndoLocking(context_, *request);
  } else if (rule == "poll") {
    request->read_target = READ_SHARED;
    this->ReadRemotely(context_, *request);
  } else {
    cerr << "Unsupported Shared -> Exclusive rule: " << rule << endl;
    return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

// Handle Exclusive -> Shared
int LockClient::HandleExclusiveToShared(LockRequest* request) {
  const string& rule = LockManager::GetExclusiveSharedRule();
  if (rule == "fail") {
    this->UndoLocking(context_, *request);
  } else if (rule == "poll") {
    this->UndoLocking(context_, *request, true);
    request->read_target = READ_EXCLUSIVE;
    this->ReadRemotely(context_, *request);
  } else if (rule == "queue") {
    request->read_target = READ_EXCLUSIVE;
    if ((request->exclusive & waitlist_[request->obj_index]) == 0) {
      waitlist_[request->obj_index] = 0;
    }
    if (waitlist_[request->obj_index] == 0) {
      user_waiters_[request->user_id] = request->exclusive;
      request->read_target = READ_EXCLUSIVE;
      this->ReadRemotely(context_, *request);
    } else {
      // lock acquisition failed, undo FA.
      this->UndoLocking(context_, *request);
    }
  } else {
    cerr << "Unsupported Exclusive -> Shared rule: " << rule << endl;
    return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

// Handle Exclusive -> Exclusive
int LockClient::HandleExclusiveToExclusive(LockRequest* request) {
  const string& rule = LockManager::GetExclusiveExclusiveRule();
  if (rule == "fail") {
    this->UndoLocking(context_, *request);
  } else if (rule == "poll") {
    this->UndoLocking(context_, *request, true);
    request->read_target = READ_EXCLUSIVE;
    this->ReadRemotely(context_, *request);
  } else if (rule == "queue") {
    request->read_target = READ_EXCLUSIVE;
    if ((request->exclusive & waitlist_[request->obj_index]) == 0) {
      waitlist_[request->obj_index] = 0;
    }
    if (waitlist_[request->obj_index] == 0) {
      user_waiters_[request->user_id] = request->exclusive;
      request->read_target = READ_EXCLUSIVE;
      this->ReadRemotely(context_, *request);
    } else {
      // lock acquisition failed, undoing FA
      this->UndoLocking(context_, *request);
    }
  } else {
    cerr << "Unsupported Exclusive -> Exclusive rule: " << rule << endl;
    return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

int LockClient::UndoLocking(Context* context, const LockRequest& request,
                            bool polling) {
  user_fail_[request.user_id] = true;
  user_polling_[request.user_id] = polling;
  this->UnlockRemotely(context, request, true, polling);
  return FUNC_SUCCESS;
}

int LockClient::PollSharedToExclusive(LockRequest* request) {
  ++user_retry_count_[request->user_id];
  if (user_retry_count_[request->user_id] > LockManager::GetPollRetry()) {
    this->UndoLocking(context_, *request);
    return FUNC_SUCCESS;
  }

  const string& rule = LockManager::GetSharedExclusiveRule();
  if (rule == "poll") {
    this->ReadRemotely(context_, *request);
  } else {
    cerr << "Unsupported Shared -> Exclusive rule for polling: " << rule
         << endl;
    return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

int LockClient::PollExclusiveToShared(LockRequest* request) {
  ++user_retry_count_[request->user_id];
  const string& rule = LockManager::GetExclusiveSharedRule();
  uint32_t value = request->exclusive;
  if (rule == "poll") {
    if (value == 0) {
      //++total_lock_success_with_poll_;
      // sum_poll_when_success_ += user_retry_count_[request->user_id];
      // local_manager_->NotifyLockRequestResult(
      // request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
      // request->obj_index, request->contention_count, SUCCESS);
      this->LockRemotely(context_, *request);
    } else {
      this->ReadRemotely(context_, *request);
    }
  } else if (rule == "queue") {
    if ((value & user_waiters_[request->user_id]) == 0) {
      local_manager_->NotifyLockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, request->contention_count, SUCCESS);
    } else {
      this->ReadRemotely(context_, *request);
    }
  } else {
    cerr << "Unsupported Exclusive -> Shared rule for polling: " << rule
         << endl;
    return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

int LockClient::PollExclusiveToExclusive(LockRequest* request) {
  const string& rule = LockManager::GetExclusiveExclusiveRule();
  ++user_retry_count_[request->user_id];
  if (++user_retry_count_[request->user_id] > LockManager::GetPollRetry() &&
      rule == "queue") {
    this->UndoLocking(context_, *request);
    return FUNC_SUCCESS;
  }
  uint32_t value = request->read_buffer;
  if (rule == "poll") {
    if (value == 0) {
      this->LockRemotely(context_, *request);
    } else {
      this->ReadRemotely(context_, *request);
    }
  } else if (rule == "queue") {
    if ((value & user_waiters_[request->user_id]) == 0) {
      local_manager_->NotifyLockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, request->contention_count, SUCCESS);
    } else {
      this->ReadRemotely(context_, *request);
    }

  } else {
    cerr << "Unsupported Exclusive -> Exclusive rule for polling: " << rule
         << endl;
    return FUNC_FAIL;
  }
  return FUNC_SUCCESS;
}

// Requests lock mode of lock manager via IBV_WR_SEND op.
int LockClient::SendLockModeRequest(Context* context) {
  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

  Message* msg = context->send_message_buffer->GetMessage();

  msg->type = Message::LOCK_MODE_REQUEST;
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

  msg->type = Message::LOCK_TABLE_MR_REQUEST;
  msg->manager_id = local_manager_->GetID();

  if (SendMessage(context)) {
    cerr << "SendLockTableRequest(): SendMessage() failed." << endl;
    return -1;
  }

  return 0;
}

bool LockClient::RequestLock(const LockRequest& request, LockMode lock_mode) {
  user_fail_[request.user_id] = false;
  user_polling_[request.user_id] = false;
  user_waiters_[request.user_id] = 0;
  user_all_waiters_[request.user_id] = 0;
  waiters_before_me_[request.obj_index] = 0;
  queued_user_[request.obj_index] = 0;

  if (lock_mode == PROXY_RETRY || lock_mode == PROXY_QUEUE ||
      lock_mode == PROXY_QUEUE2) {
    // ask lock manager to place the lock
    return this->SendLockRequest(context_, request);
  } else if (lock_mode == REMOTE_POLL || lock_mode == REMOTE_DRTM ||
             lock_mode == REMOTE_NOTIFY || lock_mode == REMOTE_D2LM_V1 ||
             lock_mode == REMOTE_D2LM_V2) {
    // try locking remotely
    return this->LockRemotely(context_, request);
  } else {
    cerr << "RequestLock(): Unknown lock mode: " << lock_mode << endl;
    return false;
  }
}

bool LockClient::RequestUnlock(const LockRequest& request, LockMode lock_mode) {
  user_fail_[request.user_id] = false;
  user_polling_[request.user_id] = false;
  if (lock_mode == PROXY_RETRY || lock_mode == PROXY_QUEUE ||
      lock_mode == PROXY_QUEUE2) {
    return this->SendUnlockRequest(context_, request);
  } else if (lock_mode == REMOTE_POLL || lock_mode == REMOTE_DRTM ||
             lock_mode == REMOTE_NOTIFY || lock_mode == REMOTE_D2LM_V1 ||
             lock_mode == REMOTE_D2LM_V2) {
    return this->UnlockRemotely(context_, request);
  } else {
    cerr << "RequestUnlock(): Unknown lock mode: " << lock_mode << endl;
    return false;
  }
}

bool LockClient::LockRemotely(Context* context, const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = LOCK;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;

  // send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
  // switch (current_request->lock_type) {
  // case SHARED: {
  // exclusive = 0;
  // shared = local_owner_id_;
  // uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
  // send_work_request.wr.atomic.compare_add = new_value;
  // break;
  //}
  // case EXCLUSIVE: {
  // exclusive = local_owner_id_;
  // shared = 0;
  // uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
  // send_work_request.wr.atomic.compare_add = new_value;
  // break;
  //}
  // default:
  // break;
  //}

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

int LockClient::ReadRemotely(Context* context, const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = READ;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->read_buffer2;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->read_buffer2_mr->lkey;

  send_work_request.wr_id = (uint64_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_RDMA_READ;

  send_work_request.wr.rdma.rkey = context->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  // if (current_request->read_target == READ_EXCLUSIVE) {
  //// reading exclusive portion of the lock object
  //// add 4 bytes here because of BIG-ENDIAN?
  // send_work_request.wr.rdma.remote_addr += 4;
  //}  // otherwise, read exclusive portion.

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

bool LockClient::UnlockRemotely(Context* context, const LockRequest& request,
                                bool is_undo, bool retry) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
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

  // send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
  // switch (current_request->lock_type) {
  // case SHARED: {
  // exclusive = 0;
  // shared = local_owner_id_;
  // uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
  // new_value = (-1) * new_value;  // need to subtract for unlock
  // send_work_request.wr.atomic.compare_add = new_value;
  // break;
  //}
  // case EXCLUSIVE: {
  // exclusive = 0;
  // shared = 0;
  // uint64_t new_value = ((uint64_t)local_owner_id_) << 32 | shared;
  // new_value = (-1) * new_value;  // need to subtract for unlock
  // send_work_request.wr.atomic.compare_add = new_value;
  // break;
  //}
  // default:
  // break;
  //}

  if (current_request->lock_type == SHARED) {
    send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    send_work_request.wr.atomic.compare_add = -1;
    ++num_rdma_atomic_fa_;
  } else if (current_request->lock_type == EXCLUSIVE) {
    send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
    shared = 0;
    uint64_t prev_value = ((uint64_t)current_request->user_id) << 32 | shared;
    send_work_request.wr.atomic.compare_add = prev_value;
    send_work_request.wr.atomic.swap = 0;
    ++num_rdma_atomic_cas_;
  }
  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  clock_gettime(CLOCK_MONOTONIC, &start_rdma_atomic_);

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "UnlockRemotely(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return false;
  }
  return true;
}

bool LockClient::SendLockRequest(Context* context, const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  Message* msg = context->send_message_buffer->GetMessage();

  msg->type = Message::LOCK_REQUEST;
  msg->seq_no = request.seq_no;
  msg->target_node_id = remote_lm_id_;
  msg->owner_node_id = local_owner_id_;
  msg->lock_type = request.lock_type;
  msg->obj_index = request.obj_index;
  msg->owner_user_id = request.user_id;

  if (SendMessage(context)) {
    cerr << "SendLockRequest(): SendMessage() failed." << endl;
    return false;
  }
  return true;
}

bool LockClient::SendNCOSEDLockRelease(const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type = Message::NCOSED_LOCK_RELEASE;
  msg->lock_type = request.lock_type;
  msg->seq_no = request.seq_no;
  msg->node_id = request.lm_id;
  msg->obj_index = request.obj_index;
  msg->owner_user_id = request.user_id;
  msg->owner_node_id = request.owner_node_id;

  if (SendMessage(context_)) {
    cerr << "SendLockRequest(): SendMessage() failed." << endl;
    return false;
  }

  return true;
}

bool LockClient::SendNCOSEDLockReleaseSuccess(const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);
  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type = Message::NCOSED_LOCK_RELEASE_SUCCESS;
  msg->lock_type = request.lock_type;
  msg->seq_no = request.seq_no;
  msg->node_id = request.lm_id;
  msg->obj_index = request.obj_index;
  msg->owner_user_id = request.user_id;
  msg->owner_node_id = request.owner_node_id;

  if (SendMessage(context_)) {
    cerr << "SendLockRequest(): SendMessage() failed." << endl;
    return false;
  }

  return true;
}

bool LockClient::SendNCOSEDLockRequest(int seq_no, int node_id, int obj_index,
                                       int request_node_id,
                                       uintptr_t request_user_id,
                                       int shared_remaining,
                                       LockType lock_type) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type = Message::NCOSED_LOCK_REQUEST;
  msg->lock_type = lock_type;
  msg->seq_no = seq_no;
  msg->node_id = node_id;
  msg->obj_index = obj_index;
  msg->request_node_id = request_node_id;
  msg->request_user_id = request_user_id;
  msg->shared_remaining = shared_remaining;

  if (SendMessage(context_)) {
    cerr << "SendLockRequest(): SendMessage() failed." << endl;
    return false;
  }

  return true;
}

bool LockClient::SendNCOSEDLockGrant(int seq_no, uintptr_t user_id, int node_id,
                                     int obj_index, LockType lock_type) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type = Message::NCOSED_LOCK_GRANT;
  msg->lock_type = lock_type;
  msg->seq_no = seq_no;
  msg->node_id = node_id;
  msg->obj_index = obj_index;
  msg->owner_user_id = user_id;

  if (SendMessage(context_)) {
    cerr << "SendLockRequest(): SendMessage() failed." << endl;
    return false;
  }

  return true;
}

bool LockClient::SendUnlockRequest(Context* context,
                                   const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(lock_mutex_);

  Message* msg = context->send_message_buffer->GetMessage();

  msg->type = Message::UNLOCK_REQUEST;
  msg->seq_no = request.seq_no;
  msg->target_node_id = remote_lm_id_;
  msg->owner_node_id = local_owner_id_;
  msg->lock_type = request.lock_type;
  msg->obj_index = request.obj_index;
  msg->owner_user_id = request.user_id;

  if (SendMessage(context)) {
    cerr << "SendUnlockRequest(): SendMessage() failed." << endl;
    return false;
  }

  return true;
}

double LockClient::GetAverageRemoteSharedLockTime() const {
  return num_shared_lock_ > 0
             ? total_shared_lock_remote_time_ / num_shared_lock_
             : 0;
}

double LockClient::GetAverageRemoteExclusiveLockTime() const {
  return num_exclusive_lock_ > 0
             ? total_exclusive_lock_remote_time_ / num_exclusive_lock_
             : 0;
}

}  // namespace proto
}  // namespace rdma
