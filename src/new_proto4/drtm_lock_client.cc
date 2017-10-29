#include "drtm_lock_client.h"
#include <chrono>

namespace rdma {
namespace proto {

// constructor
DRTMLockClient::DRTMLockClient(const string& work_dir,
                               LockManager* local_manager,
                               uint32_t local_user_count, uint32_t remote_lm_id)
    : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {}

// destructor
DRTMLockClient::~DRTMLockClient() {}

bool DRTMLockClient::RequestLock(const LockRequest& request,
                                 LockMode lock_mode) {
  // try locking remotely
  return this->Lock(context_, request, 0);
}

bool DRTMLockClient::RequestUnlock(const LockRequest& request,
                                   LockMode lock_mode) {
  return this->Unlock(context_, request);
}

bool DRTMLockClient::Lock(Context* context, const LockRequest& request,
                          uint64_t state) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = LOCK;
  current_request->prev_value = state;
  current_request->deadlock_count = 0;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uint64_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;

  uint64_t value = 1;
  int bits_to_shift = 0;
  uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
  uint64_t end_time = now + kDRTMSharedLeaseTime;
  uint64_t new_value = 0;

  switch (current_request->lock_type) {
    case SHARED: {
      new_value = (end_time & kDRTMEndTimeBitMask);
      break;
    }
    case EXCLUSIVE: {
      new_value = (uint64_t)1 << kDRTMLockBitShift |
                  ((uint64_t)current_request->user_id) << kDRTMOwnerBitShift;
      break;
    }
    default: {
      cerr << "Invalid lock type: " << current_request->lock_type << endl;
      return false;
      break;
    }
  }
  send_work_request.wr.atomic.compare_add = state;
  send_work_request.wr.atomic.swap = new_value;

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
  ++num_rdma_atomic_fa_;

  return true;
}

bool DRTMLockClient::Unlock(Context* context, const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = UNLOCK;
  current_request->deadlock_count = 0;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uint64_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;

  current_request->prev_value = (uint64_t)1 << kDRTMLockBitShift |
                                ((uint64_t)current_request->user_id)
                                    << kDRTMOwnerBitShift;
  send_work_request.wr.atomic.compare_add = current_request->prev_value;
  send_work_request.wr.atomic.swap = 0;

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
  ++num_rdma_atomic_fa_;

  return true;
}

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

    uint64_t lock_bit = value >> 63;
    uint64_t end_time = (value & kDRTMEndTimeBitMask);
    uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

    if (request->task == LOCK) {
      if (request->lock_type == EXCLUSIVE) {
        if (value == request->prev_value) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
        } else if (lock_bit == 1) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              FAILURE);
        } else {
          if (end_time < now) {
            this->Lock(context_, *request, value);
          } else {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                FAILURE);
          }
        }
      } else if (request->lock_type == SHARED) {
        if (value == request->prev_value) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              SUCCESS);
        } else if (lock_bit == 1) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              FAILURE);
        } else {
          if (end_time < now) {
            this->Lock(context_, *request, value);
          } else {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          }
        }
      }
    } else if (request->task == UNLOCK) {
      if (value == request->prev_value) {
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, SUCCESS);
      } else {
        local_manager_->NotifyUnlockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, FAILURE);
      }
    }
  }
  return 0;
}

}  // namespace proto
}  // namespace rdma
