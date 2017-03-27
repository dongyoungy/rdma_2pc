#include "direct_queue_lock_client.h"

namespace rdma { namespace proto {

// constructor
DirectQueueLockClient::DirectQueueLockClient(const string& work_dir, LockManager* local_manager,
    LockSimulator* local_user,
    uint32_t remote_lm_id) : LockClient(work_dir, local_manager, local_user, remote_lm_id) {
}

// destructor
DirectQueueLockClient::~DirectQueueLockClient() {
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
          context_->all_waiters = value;
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
          context_->waiters = exclusive;
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

    uint64_t all_value;
    uint64_t prev;
    uint32_t value, exclusive, shared;
    LockRequest* request = (LockRequest *)work_completion->wr_id;

    // polling result
    if (request->read_target == ALL) {
      uint64_t prev_value = *request->read_buffer2;
      prev = prev_value;
//#if __BYTE_ORDER == __LITTLE_ENDIAN
      //all_value = __bswap_constant_64(prev_value);  // Compiler builtin
//#else
      //all_value = prev_value;
//#endif
      all_value = prev_value;
      exclusive = (uint32_t)((all_value)>>32);
      shared = (uint32_t)all_value;
    } else {
      value = *request->read_buffer;
    }

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

    uint64_t remaining = (context_->all_waiters & all_value);
    //cout << "prev_value = " << all_value << endl;
    //cout << "prev_value2 = " << prev << endl;
    //cout << "waiters = " << context_->all_waiters << endl;
    //cout << "remaining = " << remaining << endl;
    //cout << "remaining2 = " << (context_->all_waiters & all_value) << endl;
    //cout << "remaining3 = " << (context_->all_waiters & prev) << endl;

    if (request->lock_type == SHARED) {
      // Polling on X -> proceed if value is zero
      if (remaining == 0 || (remaining >> 32) == 0) {
        context_->waiters = 0;
        ++total_lock_success_with_poll_;
        sum_poll_when_success_ += context_->retry;
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
        context_->all_waiters = 0;
        ++total_lock_success_with_poll_;
        sum_poll_when_success_ += context_->retry;
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
  if (context_->retry >= LockManager::GetPollRetry()) {
    this->UndoLocking(context_, request);
    return 0;
  }
  ++context_->retry;
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
}

int DirectQueueLockClient::HandleExclusive(LockRequest* request) {
  if (context_->retry >= LockManager::GetPollRetry()) {
    this->UndoLocking(context_, request);
    return 0;
  }
  ++context_->retry;
  this->ReadRemotely(context_,
      request->seq_no,
      request->user_id,
      request->lock_type,
      request->obj_index);
}

}}
