#include "drtm_lock_client.h"

namespace rdma {
namespace proto {

// constructor
DRTMLockClient::DRTMLockClient(const string& work_dir,
                               LockManager* local_manager,
                               uint32_t local_user_count, uint32_t remote_lm_id)
    : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {}

// destructor
DRTMLockClient::~DRTMLockClient() {}

int DRTMLockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {
  if (work_completion->status != IBV_WC_SUCCESS) {
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
      // Poco::Mutex::ScopedLock lock(lock_mutex_);
      message_in_progress_ = false;

      local_manager_->NotifyLockRequestResult(
          message->seq_no, message->owner_user_id, message->lock_type,
          remote_lm_id_, message->obj_index, 0, message->lock_result);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      // cout << "received unlock request result" << endl;
      // Poco::Mutex::ScopedLock lock(lock_mutex_);
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
      if (exclusive == request->owner_node_id && shared == 0) {
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, SUCCESS);
      } else if (exclusive == request->owner_node_id && shared != 0) {
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
      } else {
        // shared lock
        if (exclusive == 0 && shared < kDRTMSharedLimit) {
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

    if (user_retry_count_[request->user_id] >= LockManager::GetPollRetry()) {
      user_retry_count_[request->user_id] = 0;
      local_manager_->NotifyLockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, request->contention_count, FAILURE);
    } else {
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
          this->PollExclusiveToExclusive(request);
        } else {
          // exclusive -> shared
          this->PollExclusiveToShared(request);
        }
      }
    }
  }
  return 0;
}

}  // namespace proto
}  // namespace rdma
