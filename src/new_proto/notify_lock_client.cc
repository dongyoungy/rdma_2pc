#include "notify_lock_client.h"

namespace rdma { namespace proto {

// constructor
NotifyLockClient::NotifyLockClient(const string& work_dir, LockManager* local_manager,
    LockSimulator* local_user,
    uint32_t remote_lm_id) : LockClient(work_dir, local_manager, local_user, remote_lm_id) {
  pthread_mutex_init(&wait_mutex_, NULL);
  pthread_cond_init(&wait_cond_, NULL);
}

// destructor
NotifyLockClient::~NotifyLockClient() {
}

int NotifyLockClient::RequestLock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
    int lock_mode) {
  return this->LockRemotely(context_, seq_no, user_id, lock_type, obj_index);
}

int NotifyLockClient::RequestUnlock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
    int lock_mode) {
  return this->UnlockRemotely(context_, seq_no, user_id, lock_type, obj_index);
  //return this->ReadForUnlock(context_, seq_no, user_id, lock_type, obj_index);
}

// read lock object for unlocking
int NotifyLockClient::ReadForUnlock(Context* context, int seq_no, uint32_t user_id, int lock_type,
    int obj_index) {

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  pthread_mutex_lock(&lock_mutex_);
  LockRequest* request = lock_requests_[lock_request_idx_];
  request->seq_no    = seq_no;
  request->user_id   = user_id;
  request->obj_index = obj_index;
  request->lock_type = lock_type;
  request->task      = TASK_READ_UNLOCK;
  lock_request_idx_  = (lock_request_idx_ + 1) % 16;

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

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ReadForUnlock(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  ++num_rdma_read_;
  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int NotifyLockClient::UnlockRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type,
    int obj_index, bool is_undo) {

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  pthread_mutex_lock(&lock_mutex_);
  LockRequest* request = lock_requests_[lock_request_idx_];
  request->seq_no    = seq_no;
  request->user_id   = user_id;
  request->lock_type = lock_type;
  request->obj_index = obj_index;
  request->is_undo   = is_undo;
  request->task      = TASK_UNLOCK;
  lock_request_idx_  = (lock_request_idx_ + 1) % 16;

  sge.addr   = (uint64_t)request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = request->original_value_mr->lkey;

  send_work_request.wr_id          = (uint64_t)request;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode     = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  if (lock_type == SHARED) {
    exclusive = 0;
    shared = user_id;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    new_value = (-1) * new_value; // need to subtract for unlock
    send_work_request.wr.atomic.compare_add = new_value;
  } else if (lock_type == EXCLUSIVE) {
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
    cerr << "NotifyLockClient::UnlockRemotely(): ibv_exp_post_send() failed: " << strerror(ret) <<
      endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }
  ++num_rdma_atomic_;
  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int NotifyLockClient::UnlockRemotelyCS(Context* context, int seq_no, uint32_t user_id,
    int lock_type, int obj_index, uint64_t prev_value, uint64_t new_value) {

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  pthread_mutex_lock(&lock_mutex_);
  LockRequest* request = lock_requests_[lock_request_idx_];
  request->seq_no     = seq_no;
  request->user_id    = user_id;
  request->lock_type  = lock_type;
  request->obj_index  = obj_index;
  request->task       = TASK_UNLOCK;
  request->prev_value = prev_value;
  lock_request_idx_   = (lock_request_idx_ + 1) % 16;

  sge.addr   = (uint64_t)request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = request->original_value_mr->lkey;

  send_work_request.wr_id                 = (uint64_t)request;
  send_work_request.num_sge               = 1;
  send_work_request.sg_list               = &sge;
  send_work_request.exp_send_flags        = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
  send_work_request.wr.atomic.compare_add = prev_value;
  send_work_request.wr.atomic.swap        = new_value;
  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  send_work_request.wr.atomic.rkey        =
    context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "NotifyLockClient::UnlockRemotely(): ibv_exp_post_send() failed: " << strerror(ret) <<
      endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }
  ++num_rdma_atomic_;
  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int NotifyLockClient::TryLock(int seq_no, int user_id, int lock_type, int obj_index) {
  //pthread_mutex_lock(&PRINT_MUTEX);
  //cout << "TryLock(): " << seq_no << "," << user_id << "," << obj_index << "," << lock_type << endl;
  //pthread_mutex_unlock(&PRINT_MUTEX);

  return this->ReadRemotely(context_, seq_no, user_id, lock_type, obj_index);
}

int NotifyLockClient::GetNumberOfLockWaiters(uint32_t value) {
  value = value - ((value >> 1) & 0x55555555);
  value = (value & 0x33333333) + ((value >> 2) & 0x33333333);
  return (((value + (value >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
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
  exclusive = (uint32_t)((value)>>32);
  shared = (uint32_t)value;

  int nodes[64];
  int sz;
  int num_user = local_manager_->GetNumUser();
  if (request->lock_type == SHARED) {
    // notify nodes for exclusive lock.
    memset(nodes, 0x00, sizeof(int)*64);
    sz = FindNodePositions(exclusive, nodes);
    for (int i = 0; i < sz; ++i) {
      uint32_t node_id = (uint32_t)pow(2.0, nodes[i]);
      local_manager_->GrantLock(request->seq_no,
          node_id, remote_lm_id_, nodes[i]/num_user, EXCLUSIVE, request->obj_index);
    }
  } else {
    // notify nodes for shared lock.
    memset(nodes, 0x00, sizeof(int)*64);
    sz = FindNodePositions(shared, nodes);
    for (int i = 0; i < sz; ++i) {
      uint32_t node_id = (uint32_t)pow(2.0, nodes[i]);
      local_manager_->GrantLock(request->seq_no,
          node_id, remote_lm_id_, nodes[i]/num_user, SHARED, request->obj_index);
    }

    // notify nodes for exclusive lock.
    memset(nodes, 0x00, sizeof(int)*64);
    sz = FindNodePositions(exclusive, nodes);
    for (int i = 0; i < sz; ++i) {
      uint32_t node_id = (uint32_t)pow(2.0, nodes[i]);
      local_manager_->GrantLock(request->seq_no,
          node_id, remote_lm_id_, nodes[i]/num_user, EXCLUSIVE, request->obj_index);
    }
  }

  return 0;
}

int NotifyLockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {

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
    } else if (message->type == Message::LOCK_MODE) {
      local_manager_->UpdateLockModeTable(
          message->manager_id,
          message->lock_mode
          );
    } else if (message->type == Message::LOCK_REQUEST_RESULT) {
      //cout << "received lock request result." << endl;
      local_manager_->NotifyLockRequestResult(
          message->seq_no,
          message->user_id,
          message->lock_type,
          message->obj_index,
          message->lock_result);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      //cout << "received unlock request result" << endl;
      local_manager_->NotifyUnlockRequestResult(
          message->seq_no,
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
  } else if (work_completion->opcode == IBV_WC_FETCH_ADD) {
    LockRequest* request = (LockRequest *)work_completion->wr_id;
    // completion of fetch-and-add, i.e. remote shared/exclusive locking
    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_remote_shared_lock_);
    double time_taken = ((double)end_remote_shared_lock_.tv_sec * 1e+9 +
        (double)end_remote_shared_lock_.tv_nsec) -
      ((double)start_remote_shared_lock_.tv_sec * 1e+9 +
          (double)start_remote_shared_lock_.tv_nsec);
    total_shared_lock_remote_time_ += time_taken;
    ++num_shared_lock_;

    uint64_t prev_value = *request->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value)>>32);
    shared = (uint32_t)value;

    request->exclusive = exclusive;
    request->shared = shared;

    //pthread_mutex_lock(&PRINT_MUTEX);
    //cout << "work completion: " << request->task << "," << request->seq_no << "," <<
      //request->user_id << "," << request->obj_index  << "," <<
      //request->lock_type << endl;
    //pthread_mutex_unlock(&PRINT_MUTEX);

    if (request->task == TASK_LOCK) {
      if (request->lock_type == EXCLUSIVE) {
        if (exclusive == 0 && shared == 0) {
          // exclusive lock acquisition successful
          wait_after_me_[request->obj_index] = 0;
          wait_seq_no_ = -1;
          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              RESULT_SUCCESS);
        } else {
          pthread_mutex_lock(&wait_mutex_);
          wait_before_me_[request->obj_index] = value;
          wait_after_me_[request->obj_index]  = 0;
          wait_seq_no_                        = request->seq_no;
          wait_user_id_                       = request->user_id;
          wait_lock_type_                     = request->lock_type;
          wait_obj_index_                     = request->obj_index;
          //pthread_cond_signal(&wait_cond_);
          pthread_mutex_unlock(&wait_mutex_);

          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              RESULT_QUEUED);
        }
      } else if (request->lock_type == SHARED) {
        if (exclusive == 0) {
          // shared lock acquisition successful
          wait_after_me_[request->obj_index] = 0;
          wait_seq_no_ = -1;
          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              LockManager::RESULT_SUCCESS);
        //} else if ((wait_after_me_[request->obj_index] & value) != 0){
          //this->UndoLocking(context_, request);
          //wait_seq_no_ = -1;
          //local_manager_->NotifyLockRequestResult(
              //request->seq_no,
              //request->user_id,
              //request->lock_type,
              //request->obj_index,
              //RESULT_FAILURE);
        } else {
          //pthread_mutex_lock(&PRINT_MUTEX);
          //cerr << "wait shared: " << local_user_->GetID() << endl;
          //pthread_mutex_unlock(&PRINT_MUTEX);
          pthread_mutex_lock(&wait_mutex_);
          wait_after_me_[request->obj_index]  = 0;
          wait_before_me_[request->obj_index] = value;
          wait_seq_no_                        = request->seq_no;
          wait_user_id_                       = request->user_id;
          wait_lock_type_                     = request->lock_type;
          wait_obj_index_                     = request->obj_index;
          //pthread_cond_signal(&wait_cond_);
          pthread_mutex_unlock(&wait_mutex_);

          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              RESULT_QUEUED);
        }
      }
    } else if (request->task == TASK_UNLOCK) {
      if (!request->is_undo) {
        if (request->lock_type == EXCLUSIVE) {
          //if (request->user_id > exclusive) {
          //pthread_mutex_lock(&PRINT_MUTEX);
          //cerr << "wrong1" << endl;
          //pthread_mutex_unlock(&PRINT_MUTEX);
          //}
          prev_value = ((uint64_t)(exclusive - request->user_id)) << 32 | shared;
        } else {
          //if (request->user_id > shared) {
          //pthread_mutex_lock(&PRINT_MUTEX);
          //cerr << "wrong2" << endl;
          //pthread_mutex_unlock(&PRINT_MUTEX);
          //}
          prev_value = ((uint64_t)exclusive) << 32 | (shared - request->user_id);
        }
        pthread_mutex_lock(&wait_mutex_);
        wait_after_me_[request->obj_index] = prev_value;
        pthread_mutex_unlock(&wait_mutex_);

        int ret = NotifyWaitingNodes(request, prev_value);

        local_manager_->NotifyUnlockRequestResult(
            request->seq_no,
            request->user_id,
            request->lock_type,
            request->obj_index,
            RESULT_SUCCESS);
      }
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    LockRequest* request = (LockRequest *)work_completion->wr_id;
    uint64_t value = *request->read_buffer2;
    uint64_t prev_value;
    if (request->task == TASK_READ) {
      // Read for retrying lock
      if (wait_seq_no_ == -1) {
        return 0;
      }
      if ((wait_before_me_[wait_obj_index_] & value) == 0) {
        pthread_mutex_lock(&wait_mutex_);
        wait_before_me_[wait_obj_index_] = 0;
        pthread_mutex_unlock(&wait_mutex_);

        local_manager_->NotifyLockRequestResult(
            wait_seq_no_,
            wait_user_id_,
            wait_lock_type_,
            wait_obj_index_,
            RESULT_SUCCESS_FROM_QUEUED);
      }
    } else if (request->task == TASK_READ_UNLOCK) {
       // Read for unlocking
      uint32_t exclusive, shared;
      int user_id   = request->user_id;
      int lock_type = request->lock_type;
      exclusive     = (uint32_t)((value)>>32);
      shared        = (uint32_t)value;

      if (lock_type == SHARED) {
        if ((shared & user_id) != 0) {
          // needs to unlock
          uint64_t new_value = (uint64_t)exclusive << 32 | (shared - user_id);
          this->UnlockRemotelyCS(context_,
             request->seq_no,
             request->user_id,
             request->lock_type,
             request->obj_index,
             value, new_value);
        } else {
          // shared lock already 0 --> unlock successful
          pthread_mutex_lock(&wait_mutex_);
          wait_after_me_[request->obj_index] = value;
          pthread_mutex_unlock(&wait_mutex_);

          int ret = NotifyWaitingNodes(request, value);
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              RESULT_SUCCESS);
        }
      } else {
        if ((exclusive & user_id) != 0) {
          // needs to unlock
          uint64_t new_value = (uint64_t)(exclusive - user_id) << 32 | shared;
          this->UnlockRemotelyCS(context_,
             request->seq_no,
             request->user_id,
             request->lock_type,
             request->obj_index,
             value, new_value);

        } else {
          // exclusive lock already 0 --> unlock successful
          pthread_mutex_lock(&wait_mutex_);
          wait_after_me_[request->obj_index] = value;
          pthread_mutex_unlock(&wait_mutex_);

          int ret = NotifyWaitingNodes(request, value);
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              RESULT_SUCCESS);
        }
      }
    }
  } else if (work_completion->opcode == IBV_WC_COMP_SWAP) {
    // tried unlock
    LockRequest* request = (LockRequest *)work_completion->wr_id;
    uint32_t exclusive, shared;
    uint64_t prev_value = *request->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    int user_id         = request->user_id;
    int lock_type       = request->lock_type;
    exclusive = (uint32_t)((value)>>32);
    shared    = (uint32_t)value;

    if (value == request->prev_value) {
      // Unlock successful
      pthread_mutex_lock(&wait_mutex_);
      wait_after_me_[request->obj_index] = value;
      pthread_mutex_unlock(&wait_mutex_);

      if (request->lock_type == EXCLUSIVE) {
        prev_value = ((uint64_t)(exclusive - request->user_id)) << 32 | shared;
      } else {
        prev_value = ((uint64_t)exclusive) << 32 | (shared - request->user_id);
      }
      if ((prev_value & request->user_id) != 0) {
         cout <<"WRONG" << endl;
         sleep(100000);
      }
      int ret = NotifyWaitingNodes(request, prev_value);
      local_manager_->NotifyUnlockRequestResult(
          request->seq_no,
          request->user_id,
          request->lock_type,
          request->obj_index,
          RESULT_SUCCESS);
    } else {
      // unlock failed --> re-read the lock object
      //this->ReadForUnlock(context_,
          //request->seq_no,
          //request->user_id,
          //request->lock_type,
          //request->obj_index);
      if (lock_type == SHARED) {
        if ((shared & user_id) != 0) {
          // needs to unlock
          uint64_t new_value = (uint64_t)exclusive << 32 | (shared - user_id);
          this->UnlockRemotelyCS(context_,
             request->seq_no,
             request->user_id,
             request->lock_type,
             request->obj_index,
             value, new_value);
        } else {
          // shared lock already 0 --> unlock successful
          pthread_mutex_lock(&wait_mutex_);
          wait_after_me_[request->obj_index] = value;
          pthread_mutex_unlock(&wait_mutex_);

          int ret = NotifyWaitingNodes(request, value);
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              RESULT_SUCCESS);
        }
      } else {
        if ((exclusive & user_id) != 0) {
          // needs to unlock
          uint64_t new_value = (uint64_t)(exclusive - user_id) << 32 | shared;
          this->UnlockRemotelyCS(context_,
             request->seq_no,
             request->user_id,
             request->lock_type,
             request->obj_index,
             value, new_value);
        } else {
          // exclusive lock already 0 --> unlock successful
          pthread_mutex_lock(&wait_mutex_);
          wait_after_me_[request->obj_index] = value;
          pthread_mutex_unlock(&wait_mutex_);

          int ret = NotifyWaitingNodes(request, value);
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              RESULT_SUCCESS);
        }
      }
    }
  }
  return 0;
}

}}
