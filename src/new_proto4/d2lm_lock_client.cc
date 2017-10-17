#include "d2lm_lock_client.h"

namespace rdma {
namespace proto {

int D2LMLockClient::kD2LMDeadlockLimit = 100000;
bool D2LMLockClient::kDoReadBackoff = false;
// constructor
D2LMLockClient::D2LMLockClient(const string& work_dir,
                               LockManager* local_manager,
                               uint32_t local_user_count, uint32_t remote_lm_id)
    : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {
  rng_.seed();
}

// destructor
D2LMLockClient::~D2LMLockClient() {}

void D2LMLockClient::SetDeadLockLimit(int limit) { kD2LMDeadlockLimit = limit; }

void D2LMLockClient::SetReadBackoff(bool backoff) { kDoReadBackoff = backoff; }

uint64_t D2LMLockClient::GetLockValue(uint16_t exclusive_number,
                                      uint16_t shared_number,
                                      uint16_t exclusive_max,
                                      uint16_t shared_max) const {
  return (uint64_t)exclusive_number << kExclusiveNumberBitShift |
         (uint64_t)shared_number << kSharedNumberBitShift |
         (uint64_t)exclusive_max << kExclusiveMaxBitShift |
         (uint64_t)shared_max;
}
bool D2LMLockClient::RequestLock(const LockRequest& request,
                                 LockMode lock_mode) {
  // try locking remotely
  return this->Lock(context_, request);
}

bool D2LMLockClient::RequestUnlock(const LockRequest& request,
                                   LockMode lock_mode) {
  return this->Unlock(context_, request);
}

void D2LMLockClient::PerformReadBackoff(const LockRequest& request) {
  if (!kDoReadBackoff) return;

  double sleep_time =
      (request.deadlock_count > 0)
          ? kD2LMBaseReadBackoff * pow(2.0, request.deadlock_count - 1)
          : 0;

  if (sleep_time > 0) {
    int time = rng_.next(std::min((int)kD2LMMaxReadBackoff, (int)sleep_time));
    std::this_thread::sleep_for(std::chrono::microseconds(time));
  }
}

bool D2LMLockClient::Lock(Context* context, const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = LOCK;
  current_request->deadlock_count = 0;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uint64_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  uint64_t value = 1;
  int bits_to_shift = 0;

  switch (current_request->lock_type) {
    case SHARED: {
      bits_to_shift = kSharedMaxBitShift;
      break;
    }
    case EXCLUSIVE: {
      bits_to_shift = kExclusiveMaxBitShift;
      break;
    }
    default: {
      cerr << "Invalid lock type: " << current_request->lock_type << endl;
      return false;
      break;
    }
  }
  uint64_t new_value = value << bits_to_shift;
  send_work_request.wr.atomic.compare_add = new_value;

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

bool D2LMLockClient::Unlock(Context* context, const LockRequest& request) {
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

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  uint64_t value = 1;
  int bits_to_shift = 0;
  switch (current_request->lock_type) {
    case SHARED: {
      bits_to_shift = kSharedNumberBitShift;
      break;
    }
    case EXCLUSIVE: {
      bits_to_shift = kExclusiveNumberBitShift;
      break;
    }
    default: {
      cerr << "Invalid lock type: " << current_request->lock_type << endl;
      return false;
      break;
    }
  }
  uint64_t new_value = value << bits_to_shift;
  send_work_request.wr.atomic.compare_add = new_value;

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Unlock(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return false;
  }
  ++num_rdma_atomic_fa_;
  return true;
}

bool D2LMLockClient::Read(Context* context, const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = READ;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uint64_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_RDMA_READ;

  send_work_request.wr.rdma.rkey = context->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Read(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }
  ++num_rdma_read_;

  return 0;
}

bool D2LMLockClient::ReadForReset(Context* context,
                                  const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = RESET;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uint64_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_RDMA_READ;

  send_work_request.wr.rdma.rkey = context->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Read(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }
  ++num_rdma_read_;

  return 0;
}

bool D2LMLockClient::Reset(Context* context, const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = RESET;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;

  send_work_request.wr.atomic.compare_add =
      reset_value_[current_request->user_id][current_request->obj_index];
  send_work_request.wr.atomic.swap = 0;

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Reset(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return false;
  }
  ++num_rdma_atomic_cas_;
  return true;
}

bool D2LMLockClient::ResetForDeadlock(Context* context,
                                      const LockRequest& request, uint64_t from,
                                      uint64_t to) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = RESET_FOR_DEADLOCK;
  current_request->reset_from = from;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uintptr_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;

  send_work_request.wr.atomic.compare_add = from;
  send_work_request.wr.atomic.swap = to;

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "ResetForDeadlock(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return false;
  }
  ++num_rdma_atomic_cas_;
  return true;
}

bool D2LMLockClient::Undo(Context* context, const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = UNDO;
  lock_request_idx_ = (lock_request_idx_ + 1) % MAX_LOCAL_THREADS;

  sge.addr = (uint64_t)&current_request->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = current_request->original_value_mr->lkey;

  send_work_request.wr_id = (uintptr_t)current_request;
  send_work_request.num_sge = 1;
  send_work_request.sg_list = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;
  send_work_request.exp_opcode = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;

  uint64_t value = 1;
  int bits_to_shift = 0;

  switch (current_request->lock_type) {
    case SHARED: {
      bits_to_shift = kSharedMaxBitShift;
      break;
    }
    case EXCLUSIVE: {
      bits_to_shift = kExclusiveMaxBitShift;
      break;
    }
    default: {
      cerr << "Invalid lock type: " << current_request->lock_type << endl;
      return false;
      break;
    }
  }
  uint64_t new_value = value << bits_to_shift;
  send_work_request.wr.atomic.compare_add = (-1) * new_value;

  send_work_request.wr.atomic.remote_addr =
      (uint64_t)context->lock_table_mr->addr +
      (current_request->obj_index * sizeof(uint64_t));
  send_work_request.wr.atomic.rkey = context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Undo(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return false;
  }
  ++num_rdma_atomic_fa_;

  return true;
}

int D2LMLockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {
  Context* context = (Context*)work_completion->wr_id;
  if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "Work completion status is not IBV_WC_SUCCESS: "
         << work_completion->status << endl;
    return -1;
  }

  if (work_completion->opcode == IBV_WC_RECV) {
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
      local_manager_->NotifyLockRequestResult(
          message->seq_no, message->owner_user_id, message->lock_type,
          remote_lm_id_, message->obj_index, 0, message->lock_result);
    } else if (message->type == Message::UNLOCK_REQUEST_RESULT) {
      local_manager_->NotifyUnlockRequestResult(
          message->seq_no, message->owner_user_id, message->lock_type,
          remote_lm_id_, message->obj_index, message->lock_result);
    }
  } else if (work_completion->opcode == IBV_WC_COMP_SWAP) {
    LockRequest* request = (LockRequest*)work_completion->wr_id;

    uint64_t prev_value = request->original_value;
    uint64_t value = prev_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    if (LockManager::IsAtomicHCAReplyBe()) {
      value = __bswap_constant_64(prev_value);  // Compiler builtin
    }
#endif
    if (request->task == RESET) {
      if (value != reset_value_[request->user_id][request->obj_index]) {
        this->Reset(context_, *request);
      } else {
        do_reset_[request->user_id].erase(request->obj_index);
        reset_value_[request->user_id].erase(request->obj_index);
      }
    } else if (request->task == RESET_FOR_DEADLOCK) {
      uint16_t current_exclusive_number =
          (value & kExclusiveNumberBitMask) >> kExclusiveNumberBitShift;
      uint16_t current_shared_number =
          (value & kSharedNumberBitMask) >> kSharedNumberBitShift;
      uint16_t current_exclusive_max =
          (value & kExclusiveMaxBitMask) >> kExclusiveMaxBitShift;
      uint16_t current_shared_max = value & kSharedMaxBitMask;
      if (current_exclusive_number != request->last_exclusive_number ||
          current_shared_number != request->last_shared_number) {
        request->deadlock_count = 0;
        request->last_exclusive_number = current_exclusive_number;
        request->last_shared_number = current_shared_number;
        this->Read(context_, *request);
      } else {
        if (request->reset_from == value) {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              FAILURE);
          if (do_reset_[request->user_id][request->obj_index]) {
            this->Reset(context_, *request);
          }
        } else {
          uint64_t from, to;
          if (request->lock_type == EXCLUSIVE) {
            from = GetLockValue(request->last_exclusive_number,
                                request->last_shared_number,
                                current_exclusive_max, current_shared_max);
            to = GetLockValue(request->exclusive_max + 1, request->shared_max,
                              current_exclusive_max, current_shared_max);
          } else if (request->lock_type == SHARED) {
            from = GetLockValue(request->last_exclusive_number,
                                request->last_shared_number,
                                current_exclusive_max, current_shared_max);
            to = GetLockValue(request->exclusive_max, request->shared_max + 1,
                              current_exclusive_max, current_shared_max);
          } else {
            cerr << "Invalid lock type for deadlock resolution" << endl;
            exit(ERROR_INVALID_LOCK_TYPE);
          }
          this->ResetForDeadlock(context_, *request, from, to);
        }
      }

    } else {
      cerr << "Invalid task for compare and swap: " << request->task << endl;
      exit(ERROR_INVALID_TASK);
    }
  } else if (work_completion->opcode == IBV_WC_FETCH_ADD) {
    // completion of fetch-and-add
    // counter has been increased.

    LockRequest* request = (LockRequest*)work_completion->wr_id;

    uint64_t prev_value = request->original_value;
    uint64_t value = prev_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    if (LockManager::IsAtomicHCAReplyBe()) {
      value = __bswap_constant_64(prev_value);  // Compiler builtin
    }
#endif

    request->exclusive_number =
        (value & kExclusiveNumberBitMask) >> kExclusiveNumberBitShift;
    request->shared_number =
        (value & kSharedNumberBitMask) >> kSharedNumberBitShift;
    request->exclusive_max =
        (value & kExclusiveMaxBitMask) >> kExclusiveMaxBitShift;
    request->shared_max = value & kSharedMaxBitMask;

    if (request->task == UNDO) {
      local_manager_->NotifyLockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, request->contention_count, RETRY);
    } else if (request->task == LOCK) {
      if (request->lock_type == EXCLUSIVE) {
        if (request->exclusive_max >= kMaxPossibleNumber ||
            request->shared_max >= kMaxPossibleNumber) {
          ++request->contention_count;
          this->Undo(context_, *request);
        } else {
          if (request->exclusive_max + 1 == kMaxPossibleNumber) {
            do_reset_[request->user_id][request->obj_index] = true;
            reset_value_[request->user_id][request->obj_index] =
                (uint64_t)kMaxPossibleNumber << kExclusiveNumberBitShift |
                (uint64_t)request->shared_max << kSharedNumberBitShift |
                (uint64_t)kMaxPossibleNumber << kExclusiveMaxBitShift |
                (uint64_t)request->shared_max;
          }
          if (request->exclusive_max == request->exclusive_number &&
              request->shared_max == request->shared_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else {
            ++request->contention_count;
            PerformReadBackoff(*request);
            this->Read(context_, *request);
          }
        }
      } else if (request->lock_type == SHARED) {
        if (request->shared_max >= kMaxPossibleNumber ||
            request->exclusive_max >= kMaxPossibleNumber) {
          ++request->contention_count;
          this->Undo(context_, *request);
        } else {
          if (request->shared_max + 1 == kMaxPossibleNumber) {
            do_reset_[request->user_id][request->obj_index] = true;
            reset_value_[request->user_id][request->obj_index] =
                (uint64_t)request->exclusive_max << kExclusiveNumberBitShift |
                (uint64_t)kMaxPossibleNumber << kSharedNumberBitShift |
                (uint64_t)request->exclusive_max << kExclusiveMaxBitShift |
                (uint64_t)kMaxPossibleNumber;
          }
          if (request->exclusive_max == request->exclusive_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else {
            ++request->contention_count;
            PerformReadBackoff(*request);
            this->Read(context_, *request);
          }
        }
      } else {
        cerr << "Invalid lock type: " << request->lock_type << endl;
        exit(ERROR_INVALID_LOCK_TYPE);
      }
    } else if (request->task == UNLOCK) {
      local_manager_->NotifyUnlockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, SUCCESS);
      if (do_reset_[request->user_id][request->obj_index]) {
        this->Reset(context_, *request);
      }
    } else {
      cerr << "Invalid task_1: " << request->task << endl;
      exit(ERROR_INVALID_TASK);
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    LockRequest* request = (LockRequest*)work_completion->wr_id;
    uint64_t value = request->original_value;

    request->exclusive_number =
        (value & kExclusiveNumberBitMask) >> kExclusiveNumberBitShift;
    request->shared_number =
        (value & kSharedNumberBitMask) >> kSharedNumberBitShift;
    uint16_t current_exclusive_max =
        (value & kExclusiveMaxBitMask) >> kExclusiveMaxBitShift;
    uint16_t current_shared_max = value & kSharedMaxBitMask;

    if (request->task == READ) {
      switch (request->lock_type) {
        case EXCLUSIVE: {
          if (request->exclusive_max == request->exclusive_number &&
              request->shared_max == request->shared_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else if (request->exclusive_max < request->exclusive_number ||
                     request->shared_max < request->shared_number) {
            // I need to fail -> retry.
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                FAILURE);
          } else {
            if (request->exclusive_number == request->last_exclusive_number &&
                request->shared_number == request->last_shared_number) {
              ++request->deadlock_count;
            } else {
              request->deadlock_count = 0;
            }
            request->last_exclusive_number = request->exclusive_number;
            request->last_shared_number = request->shared_number;
            ++request->contention_count;
            if (request->deadlock_count >= kD2LMDeadlockLimit) {
              // Handle deadlock
              uint64_t from = GetLockValue(
                  request->last_exclusive_number, request->last_shared_number,
                  current_exclusive_max, current_shared_max);
              uint64_t to =
                  GetLockValue(request->exclusive_max + 1, request->shared_max,
                               current_exclusive_max, current_shared_max);
              this->ResetForDeadlock(context_, *request, from, to);
            } else {
              PerformReadBackoff(*request);
              this->Read(context_, *request);
            }
          }
          break;
        }
        case SHARED: {
          if (request->exclusive_max == request->exclusive_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else if (request->exclusive_max < request->exclusive_number ||
                     request->shared_max < request->shared_number) {
            // I need to fail -> retry.
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                FAILURE);
          } else {
            if (request->exclusive_number == request->last_exclusive_number &&
                request->shared_number == request->last_shared_number) {
              ++request->deadlock_count;
            } else {
              request->deadlock_count = 0;
            }
            request->last_exclusive_number = request->exclusive_number;
            request->last_shared_number = request->shared_number;
            ++request->contention_count;
            if (request->deadlock_count >= kD2LMDeadlockLimit) {
              // Handle deadlock
              uint64_t from = GetLockValue(
                  request->last_exclusive_number, request->last_shared_number,
                  current_exclusive_max, current_shared_max);
              uint64_t to =
                  GetLockValue(request->exclusive_max, request->shared_max + 1,
                               current_exclusive_max, current_shared_max);
              this->ResetForDeadlock(context_, *request, from, to);
            } else {
              PerformReadBackoff(*request);
              this->Read(context_, *request);
            }
          }
          break;
        }
        default: {
          cerr << "Invalid lock type: " << request->lock_type << endl;
          exit(ERROR_INVALID_LOCK_TYPE);
          break;
        }
      }
    } else {  // reading for reset.
      cerr << "Invalid task_2: " << request->task << endl;
      exit(ERROR_INVALID_TASK);
    }
  }

  return 0;
}  // namespace proto

}  // namespace proto
}  // namespace rdma
