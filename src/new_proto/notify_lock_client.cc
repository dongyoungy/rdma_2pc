#include "notify_lock_client.h"

namespace rdma { namespace proto {

// constructor
NotifyLockClient::NotifyLockClient(const string& work_dir, LockManager* local_manager,
    LockSimulator* local_user,
    int remote_lm_id) : LockClient(work_dir, local_manager, local_user, remote_lm_id) {
  pthread_mutex_init(&wait_mutex_, NULL);
  pthread_cond_init(&wait_cond_, NULL);
}

// destructor
NotifyLockClient::~NotifyLockClient() {
}

int NotifyLockClient::RequestLock(int seq_no, int user_id, int lock_type, int obj_index,
    int lock_mode) {
  return this->LockRemotely(context_, seq_no, user_id, lock_type, obj_index);
}

int NotifyLockClient::RequestUnlock(int seq_no, int user_id, int lock_type, int obj_index,
    int lock_mode) {
  return this->UnlockRemotely(context_, seq_no, user_id, lock_type, obj_index);
}

int NotifyLockClient::TryLock(int seq_no, int user_id, int lock_type, int obj_index) {
  //if (user_id != wait_user_id_ ||
      //lock_type != wait_lock_type_ ||
      //obj_index != wait_obj_index_) {
    //cerr << "NotifyLockClient::TryLock() : Information do not match, " <<
      //local_user_->GetID() << endl;
    //return -1;
  //} else {
    //cerr << "NotifyLockClient::TryLock(), " << local_user_->GetID() << endl;
  //}
  pthread_mutex_lock(&wait_mutex_);
  if (wait_before_me_[obj_index] == 0) {
    pthread_cond_wait(&wait_cond_, &wait_mutex_);
  }
  pthread_mutex_unlock(&wait_mutex_);
  //pthread_mutex_lock(&PRINT_MUTEX);
  //cout << "TryLock(): " << seq_no << "," << user_id << "," << obj_index << "," << lock_type << endl;
  //pthread_mutex_unlock(&PRINT_MUTEX);

  return this->ReadRemotely(context_, user_id, obj_index);
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

  // notify nodes for shared lock.
  int nodes[64];
  memset(nodes, 0x00, sizeof(int)*64);
  int sz = FindNodePositions(shared, nodes);
  for (int i = 0; i < sz; ++i) {
    int node_id = (int)pow(2.0, nodes[i]);
    local_manager_->GrantLock(request->seq_no,
        node_id, remote_lm_id_, SHARED, request->obj_index);
  }

  // notify nodes for exclusive lock.
  memset(nodes, 0x00, sizeof(int)*64);
  sz = FindNodePositions(exclusive, nodes);
  for (int i = 0; i < sz; ++i) {
    int node_id = (int)pow(2.0, nodes[i]);
    local_manager_->GrantLock(request->seq_no,
        node_id, remote_lm_id_, EXCLUSIVE, request->obj_index);
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

    if (request->task == TASK_LOCK) {
      if (request->lock_type == EXCLUSIVE) {
        if (exclusive == 0 && shared == 0) {
          // exclusive lock acquisition successful
          wait_after_me_[request->obj_index] = 0;
          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              RESULT_SUCCESS);
        //} else if ((wait_after_me_[context->last_obj_index] & value) != 0){
          //this->UndoLocking(context);
          //local_manager_->NotifyLockRequestResult(
              //context->last_seq_no,
              //context->last_user_id,
              //context->last_lock_type,
              //context->last_obj_index,
              //RESULT_FAILURE);
        } else {
          //pthread_mutex_lock(&PRINT_MUTEX);
          //cerr << "wait exclusive: " << local_user_->GetID() << endl;
          //pthread_mutex_unlock(&PRINT_MUTEX);
          pthread_mutex_lock(&wait_mutex_);
          wait_before_me_[request->obj_index] = value;
          wait_after_me_[request->obj_index]  = 0;
          wait_seq_no_                        = request->seq_no;
          wait_user_id_                       = request->user_id;
          wait_lock_type_                     = request->lock_type;
          wait_obj_index_                     = request->obj_index;
          pthread_cond_signal(&wait_cond_);
          pthread_mutex_unlock(&wait_mutex_);

        }
      } else if (request->lock_type == SHARED) {
        if (exclusive == 0) {
          // shared lock acquisition successful
          wait_after_me_[request->obj_index] = 0;
          local_manager_->NotifyLockRequestResult(
              request->seq_no,
              request->user_id,
              request->lock_type,
              request->obj_index,
              LockManager::RESULT_SUCCESS);
        //} else if ((wait_after_me_[context->last_obj_index] & value) != 0){
          //this->UndoLocking(context);
          //local_manager_->NotifyLockRequestResult(
              //context->last_seq_no,
              //context->last_user_id,
              //context->last_lock_type,
              //context->last_obj_index,
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
          pthread_cond_signal(&wait_cond_);
          pthread_mutex_unlock(&wait_mutex_);
        }
      }
    } else if (request->task == TASK_UNLOCK) {
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
      //pthread_mutex_lock(&wait_mutex_);
      //wait_after_me_[context->last_obj_index] = prev_value;
      //pthread_mutex_unlock(&wait_mutex_);

      int ret = NotifyWaitingNodes(request, prev_value);

      local_manager_->NotifyUnlockRequestResult(
          request->seq_no,
          request->user_id,
          request->lock_type,
          request->obj_index,
          RESULT_SUCCESS);
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    LockRequest* request = (LockRequest *)work_completion->wr_id;
    uint64_t value = *request->read_buffer2;
    if ((wait_before_me_[wait_obj_index_] & value) == 0) {
      pthread_mutex_lock(&wait_mutex_);
      wait_before_me_[wait_obj_index_] = 0;
      pthread_mutex_unlock(&wait_mutex_);
      //pthread_mutex_lock(&PRINT_MUTEX);
      //cerr << "Getting lock from notification: " <<
        //wait_seq_no_ << "," << wait_user_id_ << "," <<
        //wait_obj_index_ << "," << wait_lock_type_ <<
        //endl;
      //pthread_mutex_unlock(&PRINT_MUTEX);
      local_manager_->NotifyLockRequestResult(
          wait_seq_no_,
          wait_user_id_,
          wait_lock_type_,
          wait_obj_index_,
          RESULT_SUCCESS);
      //local_manager_->NotifyLockRequestResult(
          //context->last_seq_no,
          //context->last_user_id,
          //context->last_lock_type,
          //context->last_obj_index,
          //LockManager::RESULT_SUCCESS);
    }
  }
  return 0;
}

}}
