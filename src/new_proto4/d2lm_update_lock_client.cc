#include "d2lm_update_lock_client.h"

namespace rdma {
namespace proto {

int D2LMUpdateLockClient::kD2LMDeadlockLimit = 100000;
bool D2LMUpdateLockClient::kDoReadBackoff = false;
double D2LMUpdateLockClient::kD2LMFailRate = 0;
// constructor
D2LMUpdateLockClient::D2LMUpdateLockClient(const string& work_dir,
                                           LockManager* local_manager,
                                           uint32_t local_user_count,
                                           uint32_t remote_lm_id)
    : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {
  rng_.seed();
}

// destructor
D2LMUpdateLockClient::~D2LMUpdateLockClient() {}

void D2LMUpdateLockClient::SetDeadLockLimit(int limit) {
  kD2LMDeadlockLimit = limit;
}

void D2LMUpdateLockClient::SetReadBackoff(bool backoff) {
  kDoReadBackoff = backoff;
}

void D2LMUpdateLockClient::SetFailRate(double rate) { kD2LMFailRate = rate; }

int D2LMUpdateLockClient::GetDeadlockLimit() { return kD2LMDeadlockLimit; }

double D2LMUpdateLockClient::GetFailRate() { return kD2LMFailRate; }

bool D2LMUpdateLockClient::GetDoReset(uintptr_t user_id, int obj_index) {
  return do_reset_[user_id][obj_index];
}

uint64_t D2LMUpdateLockClient::GetLockValue(
    uint16_t update_number, uint16_t exclusive_number, uint16_t shared_number,
    uint16_t update_max, uint16_t exclusive_max, uint16_t shared_max) const {
  return (uint64_t)update_number << kD2LMUpdateNumberBitShift |
         (uint64_t)exclusive_number << kD2LMExclusiveNumberBitShift |
         (uint64_t)shared_number << kD2LMSharedNumberBitShift |
         (uint64_t)update_max << kD2LMUpdateMaxBitShift |
         (uint64_t)exclusive_max << kD2LMExclusiveMaxBitShift |
         (uint64_t)shared_max;
}
bool D2LMUpdateLockClient::RequestLock(const LockRequest& request,
                                       LockMode lock_mode) {
  // try locking remotely
  return this->Lock(context_, request);
}

bool D2LMUpdateLockClient::RequestUnlock(const LockRequest& request,
                                         LockMode lock_mode) {
  if (request.is_failed == false) {
    return this->Unlock(context_, request);
  } else if (request.is_failed == true &&
             do_reset_[request.user_id][request.obj_index]) {
    return this->Unlock(context_, request);
  }
  return true;
}

void D2LMUpdateLockClient::PerformReadBackoff(const LockRequest& request) {
  if (!kDoReadBackoff) return;

  // double sleep_time =
  //(request.deadlock_count > 0)
  //? kD2LMBaseReadBackoff * pow(2.0, request.deadlock_count - 1)
  //: 0;

  // if (sleep_time > 0) {
  // int time = rng_.next(std::min((int)kD2LMMaxReadBackoff, (int)sleep_time));
  // std::this_thread::sleep_for(std::chrono::microseconds(time));
  //}
  int sleep_time = 0;
  int interval = 0;
  if (request.lock_type == EXCLUSIVE) {
    interval =
        (request.exclusive_max - request.exclusive_number) +
        (request.update_max - request.update_number) +
        ceil((double)(request.shared_max - request.shared_number) / (double)2);
  } else if (request.lock_type == SHARED || request.lock_type == UPDATE) {
    interval = (request.exclusive_max - request.exclusive_number) +
               (request.update_max - request.update_number);
  }
  if (interval > 0) {
    interval = interval;
    sleep_time = std::min(kD2LMMaxReadBackoff, interval * kD2LMBaseReadBackoff);
    // std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
    Poco::Timestamp stamp;
    while (stamp.elapsed() < sleep_time) {
      // busy-wait
    }
  }
}

bool D2LMUpdateLockClient::Lock(Context* context, const LockRequest& request) {
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

  uint64_t value = current_request->d2lm_increment;
  // lock_increment_[current_request->obj_index] = value;
  int bits_to_shift = 0;

  switch (current_request->lock_type) {
    case SHARED: {
      bits_to_shift = kD2LMSharedMaxBitShift;
      // extend_count_[current_request->obj_index] = 0;
      break;
    }
    case SHARED_EXTEND: {
      value = 1;
      // extend_count_[current_request->obj_index]++;
      bits_to_shift = kSharedNumberBitShift;
      break;
    }
    case EXCLUSIVE: {
      bits_to_shift = kD2LMExclusiveMaxBitShift;
      // extend_count_[current_request->obj_index] = 0;
      break;
    }
    case EXCLUSIVE_EXTEND: {
      value = 1;
      // extend_count_[current_request->obj_index]++;
      bits_to_shift = kExclusiveNumberBitShift;
      break;
    }
    case UPDATE: {
      value = 1;
      bits_to_shift = kD2LMUpdateMaxBitShift;
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

bool D2LMUpdateLockClient::Unlock(Context* context,
                                  const LockRequest& request) {
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

  uint64_t value = current_request->d2lm_increment;
  int bits_to_shift = 0;
  switch (current_request->lock_type) {
    case SHARED: {
      bits_to_shift = kD2LMSharedNumberBitShift;
      // value -= extend_count_[current_request->obj_index];
      break;
    }
    case EXCLUSIVE: {
      bits_to_shift = kD2LMExclusiveNumberBitShift;
      // value -= extend_count_[current_request->obj_index];
      break;
    }
    case UPDATE: {
      bits_to_shift = kD2LMUpdateNumberBitShift;
      // value -= extend_count_[current_request->obj_index];
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

bool D2LMUpdateLockClient::Read(Context* context, const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = READ;
  // current_request->timestamp.update();
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

bool D2LMUpdateLockClient::ReadForReset(Context* context,
                                        const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = RESET;
  // current_request->timestamp.update();
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

bool D2LMUpdateLockClient::Reset(Context* context, const LockRequest& request) {
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

bool D2LMUpdateLockClient::ResetForDeadlock(Context* context,
                                            const LockRequest& request,
                                            uint64_t from, uint64_t to) {
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
  // cout << "ResetForDeadlock" << endl;
  ++num_rdma_atomic_cas_;
  return true;
}

bool D2LMUpdateLockClient::Undo(Context* context, const LockRequest& request) {
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

  uint64_t value = current_request->d2lm_increment;
  int bits_to_shift = 0;

  switch (current_request->lock_type) {
    case SHARED: {
      bits_to_shift = kD2LMSharedMaxBitShift;
      break;
    }
    case EXCLUSIVE: {
      bits_to_shift = kD2LMExclusiveMaxBitShift;
      break;
    }
    case UPDATE: {
      bits_to_shift = kD2LMUpdateMaxBitShift;
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

bool D2LMUpdateLockClient::UndoNumber(Context* context,
                                      const LockRequest& request) {
  Poco::FastMutex::ScopedLock lock(fast_mutex_);

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
  *current_request = request;
  current_request->task = UNDO_NUMBER;
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
      bits_to_shift = kD2LMSharedNumberBitShift;
      break;
    }
    case EXCLUSIVE: {
      bits_to_shift = kD2LMExclusiveNumberBitShift;
      break;
    }
    case UPDATE: {
      bits_to_shift = kD2LMUpdateNumberBitShift;
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
    cerr << "UndoNumber(): ibv_exp_post_send() failed: " << strerror(ret)
         << endl;
    return false;
  }
  ++num_rdma_atomic_fa_;

  return true;
}

int D2LMUpdateLockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {
  Context* context = (Context*)work_completion->wr_id;

  if (work_completion->status == IBV_WC_REM_INV_REQ_ERR ||
      work_completion->status == IBV_WC_WR_FLUSH_ERR ||
      work_completion->status == IBV_WC_RETRY_EXC_ERR) {
    // LM failed.
    // It should let local manager know that this particular remote lock manager
    // is unavailable + any request that has been made also failed.
    cerr << "(D2LMUpdateLockClient) Detected Node Failure: "
         << work_completion->status << " from LM " << remote_lm_id_ << endl;
    local_manager_->SetRemoteManagerAvailability(remote_lm_id_);

    // Let the simulator know of the node failure.
    if (work_completion->opcode == IBV_WC_FETCH_ADD) {
      // either locking or unlocking
      LockRequest* request = (LockRequest*)work_completion->wr_id;

      switch (request->task) {
        case LOCK: {
          local_manager_->NotifyLockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, request->contention_count,
              NODE_FAILURE);
          break;
        }
        case UNLOCK: {
          local_manager_->NotifyUnlockRequestResult(
              request->seq_no, request->user_id, request->lock_type,
              remote_lm_id_, request->obj_index, NODE_FAILURE);
          break;
        }
        default: { break; }
      }

    } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
      // Must be reading for locking...
      LockRequest* request = (LockRequest*)work_completion->wr_id;
      local_manager_->NotifyLockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, request->contention_count, NODE_FAILURE);
    }
    return -1;
  } else if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "(D2LMUpdateLockClient) Work completion status is not "
            "IBV_WC_SUCCESS: "
         << work_completion->status << " from LM " << remote_lm_id_ << endl;
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
          (value & kD2LMExclusiveNumberBitMask) >> kD2LMExclusiveNumberBitShift;
      uint16_t current_shared_number =
          (value & kD2LMSharedNumberBitMask) >> kD2LMSharedNumberBitShift;
      uint16_t current_update_number =
          (value & kD2LMUpdateNumberBitMask) >> kD2LMUpdateNumberBitShift;
      uint16_t current_exclusive_max =
          (value & kD2LMExclusiveMaxBitMask) >> kD2LMExclusiveMaxBitShift;
      uint16_t current_shared_max = value & kD2LMSharedMaxBitMask;
      uint16_t current_update_max = value & kD2LMUpdateMaxBitMask;
      if (current_exclusive_number != request->last_exclusive_number ||
          current_shared_number != request->last_shared_number) {
        request->deadlock_count = 0;
        request->timestamp.update();
        request->last_exclusive_number = current_exclusive_number;
        request->last_shared_number = current_shared_number;
        request->last_update_number = current_shared_number;
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
            from = GetLockValue(request->last_update_number,
                                request->last_exclusive_number,
                                request->last_shared_number, current_update_max,
                                current_exclusive_max, current_shared_max);
            to = GetLockValue(request->update_max,
                              request->exclusive_max + request->d2lm_increment,
                              request->shared_max, current_update_max,
                              current_exclusive_max, current_shared_max);
          } else if (request->lock_type == SHARED) {
            from = GetLockValue(request->last_update_number,
                                request->last_exclusive_number,
                                request->last_shared_number, current_update_max,
                                current_exclusive_max, current_shared_max);
            to = GetLockValue(request->update_max, request->exclusive_max,
                              request->shared_max + request->d2lm_increment,
                              current_update_number, current_exclusive_max,
                              current_shared_max);
          } else if (request->lock_type == UPDATE) {
            from = GetLockValue(request->last_update_number,
                                request->last_exclusive_number,
                                request->last_shared_number, current_update_max,
                                current_exclusive_max, current_shared_max);
            to = GetLockValue(request->update_max + request->d2lm_increment,
                              request->exclusive_max, request->shared_max,
                              current_update_number, current_exclusive_max,
                              current_shared_max);
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

    request->update_number =
        (value & kD2LMUpdateNumberBitMask) >> kD2LMUpdateNumberBitShift;
    request->exclusive_number =
        (value & kD2LMExclusiveNumberBitMask) >> kD2LMExclusiveNumberBitShift;
    request->shared_number =
        (value & kD2LMSharedNumberBitMask) >> kD2LMSharedNumberBitShift;
    request->update_max =
        (value & kD2LMUpdateMaxBitMask) >> kD2LMUpdateMaxBitShift;
    request->exclusive_max =
        (value & kD2LMExclusiveMaxBitMask) >> kD2LMExclusiveMaxBitShift;
    request->shared_max = value & kD2LMSharedMaxBitMask;

    if (request->task == UNDO) {
      local_manager_->NotifyLockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, request->contention_count, RETRY);
    } else if (request->task == LOCK) {
      if (request->lock_type == EXCLUSIVE) {
        if (request->update_max >= kD2LMMaxPossibleNumber ||
            request->exclusive_max >= kD2LMMaxPossibleNumber ||
            request->shared_max >= kD2LMMaxPossibleNumber) {
          ++request->contention_count;
          this->Undo(context_, *request);
        } else {
          // if (request->exclusive_max + 1 == kMaxPossibleNumber) {
          if (request->exclusive_max + request->d2lm_increment >=
              kD2LMMaxPossibleNumber) {
            int number_to_reset =
                request->exclusive_max + request->d2lm_increment;
            do_reset_[request->user_id][request->obj_index] = true;
            reset_value_[request->user_id][request->obj_index] =
                (uint64_t)request->update_max << kD2LMUpdateNumberBitShift |
                (uint64_t)number_to_reset << kD2LMExclusiveNumberBitShift |
                (uint64_t)request->shared_max << kD2LMSharedNumberBitShift |
                (uint64_t)request->update_max << kD2LMUpdateMaxBitShift |
                (uint64_t)number_to_reset << kD2LMExclusiveMaxBitShift |
                (uint64_t)request->shared_max;
          }
          if (request->exclusive_max == request->exclusive_number &&
              request->shared_max == request->shared_number &&
              request->update_max == request->update_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else {
            ++request->contention_count;
            PerformReadBackoff(*request);
            request->timestamp.update();
            this->Read(context_, *request);
          }
        }
      } else if (request->lock_type == SHARED) {
        if (request->shared_max >= kD2LMMaxPossibleNumber ||
            request->exclusive_max >= kD2LMMaxPossibleNumber ||
            request->update_max >= kD2LMMaxPossibleNumber) {
          ++request->contention_count;
          this->Undo(context_, *request);
        } else {
          // if (request->shared_max + 1 == kMaxPossibleNumber) {
          if (request->shared_max + request->d2lm_increment >=
              kD2LMMaxPossibleNumber) {
            int number_to_reset = request->shared_max + request->d2lm_increment;
            do_reset_[request->user_id][request->obj_index] = true;
            reset_value_[request->user_id][request->obj_index] =
                (uint64_t)request->update_max << kD2LMUpdateNumberBitShift |
                (uint64_t)request->exclusive_max
                    << kD2LMExclusiveNumberBitShift |
                (uint64_t)number_to_reset << kD2LMSharedNumberBitShift |
                (uint64_t)request->update_max << kD2LMUpdateMaxBitShift |
                (uint64_t)request->exclusive_max << kD2LMExclusiveMaxBitShift |
                (uint64_t)number_to_reset;
          }
          if (request->exclusive_max == request->exclusive_number &&
              request->update_max == request->update_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else {
            ++request->contention_count;
            PerformReadBackoff(*request);
            request->timestamp.update();
            this->Read(context_, *request);
          }
        }
      } else if (request->lock_type == UPDATE) {
        if (request->shared_max >= kD2LMMaxPossibleNumber ||
            request->exclusive_max >= kD2LMMaxPossibleNumber ||
            request->update_max >= kD2LMMaxPossibleNumber) {
          ++request->contention_count;
          this->Undo(context_, *request);
        } else {
          // if (request->shared_max + 1 == kMaxPossibleNumber) {
          if (request->update_max + request->d2lm_increment >=
              kD2LMMaxPossibleNumber) {
            int number_to_reset = request->update_max + request->d2lm_increment;
            do_reset_[request->user_id][request->obj_index] = true;
            reset_value_[request->user_id][request->obj_index] =
                (uint64_t)number_to_reset << kD2LMUpdateNumberBitShift |
                (uint64_t)request->exclusive_max
                    << kD2LMExclusiveNumberBitShift |
                (uint64_t)request->shared_max << kD2LMSharedNumberBitShift |
                (uint64_t)number_to_reset << kD2LMUpdateMaxBitShift |
                (uint64_t)request->exclusive_max << kD2LMExclusiveMaxBitShift |
                (uint64_t)request->shared_max;
          }
          if (request->exclusive_max == request->exclusive_number &&
              request->update_max == request->update_number &&
              request->shared_max == request->shared_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else {
            ++request->contention_count;
            PerformReadBackoff(*request);
            request->timestamp.update();
            this->Read(context_, *request);
          }
        }
      } else if (request->lock_type == SHARED_EXTEND ||
                 request->lock_type == EXCLUSIVE_EXTEND) {
        local_manager_->NotifyLockRequestResult(
            request->seq_no, request->user_id, request->lock_type,
            remote_lm_id_, request->obj_index, request->contention_count,
            SUCCESS);
      } else {
        cerr << "Invalid lock type: " << request->lock_type << endl;
        exit(ERROR_INVALID_LOCK_TYPE);
      }
    } else if (request->task == UNLOCK) {
      if (request->lock_type == SHARED &&
          request->shared_number >= request->shared_max) {
        this->UndoNumber(context_, *request);
      } else if (request->lock_type == EXCLUSIVE &&
                 request->exclusive_number >= request->exclusive_max) {
        this->UndoNumber(context_, *request);
      } else if (request->lock_type == UPDATE &&
                 request->update_number >= request->update_max) {
        this->UndoNumber(context_, *request);
      }
      local_manager_->NotifyUnlockRequestResult(
          request->seq_no, request->user_id, request->lock_type, remote_lm_id_,
          request->obj_index, SUCCESS);
      if (do_reset_[request->user_id][request->obj_index]) {
        ++num_reset_;
        this->Reset(context_, *request);
      }
    } else if (request->task != UNDO_NUMBER) {
      cerr << "Invalid task_1: " << request->task << endl;
      exit(ERROR_INVALID_TASK);
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    LockRequest* request = (LockRequest*)work_completion->wr_id;
    uint64_t value = request->original_value;

    request->update_number =
        (value & kD2LMUpdateNumberBitMask) >> kD2LMUpdateNumberBitShift;
    request->exclusive_number =
        (value & kD2LMExclusiveNumberBitMask) >> kD2LMExclusiveNumberBitShift;
    request->shared_number =
        (value & kD2LMSharedNumberBitMask) >> kD2LMSharedNumberBitShift;
    uint16_t current_update_max =
        (value & kD2LMUpdateMaxBitMask) >> kD2LMUpdateMaxBitShift;
    uint16_t current_exclusive_max =
        (value & kD2LMExclusiveMaxBitMask) >> kD2LMExclusiveMaxBitShift;
    uint16_t current_shared_max = value & kD2LMSharedMaxBitMask;

    if (request->task == READ) {
      switch (request->lock_type) {
        case EXCLUSIVE: {
          if (request->exclusive_max == request->exclusive_number &&
              request->shared_max == request->shared_number &&
              request->update_max == request->update_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else if (request->exclusive_max < request->exclusive_number ||
                     request->shared_max < request->shared_number ||
                     request->update_max < request->update_number) {
            // I need to fail -> retry.
            LockStat s;
            s.contention_count = request->contention_count;
            s.contention_count3 = 1;
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, s, FAILURE);
          } else {
            if (request->exclusive_number == request->last_exclusive_number &&
                request->shared_number == request->last_shared_number &&
                request->update_number == request->last_update_number) {
              ++request->deadlock_count;
            } else {
              request->deadlock_count = 0;
              request->timestamp.update();
            }
            request->last_exclusive_number = request->exclusive_number;
            request->last_shared_number = request->shared_number;
            request->last_update_number = request->update_number;
            ++request->contention_count;
            int exp_count =
                (request->exclusive_max - request->exclusive_number) +
                (request->update_max - request->update_number) +
                (request->shared_max - request->shared_number);
            if (exp_count < 0) {
              exp_count = 1;
            }
            // if (request->deadlock_count >= kD2LMDeadlockLimit) {
            if (request->timestamp.elapsed() >=
                exp_count * kD2LMDeadlockLimit * 2) {
              // cout << "DEADLOCK OR FAIL." << endl;
              // Handle deadlock
              uint64_t from = GetLockValue(
                  request->last_update_number, request->last_exclusive_number,
                  request->last_shared_number, current_update_max,
                  current_exclusive_max, current_shared_max);
              uint64_t to =
                  GetLockValue(request->update_max,
                               request->exclusive_max + request->d2lm_increment,
                               request->shared_max, current_update_max,
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
          if (request->exclusive_max == request->exclusive_number &&
              request->update_max == request->update_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else if (request->exclusive_max < request->exclusive_number ||
                     request->shared_max < request->shared_number ||
                     request->update_max < request->update_number) {
            // I need to fail -> retry.
            LockStat s;
            s.contention_count = request->contention_count;
            s.contention_count3 = 1;
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, s, FAILURE);
          } else {
            if (request->exclusive_number == request->last_exclusive_number &&
                request->shared_number == request->last_shared_number &&
                request->update_number == request->last_update_number) {
              ++request->deadlock_count;
            } else {
              request->deadlock_count = 0;
              request->timestamp.update();
            }
            request->last_exclusive_number = request->exclusive_number;
            request->last_shared_number = request->shared_number;
            request->last_update_number = request->update_number;
            ++request->contention_count;
            int exp_count =
                (request->exclusive_max - request->exclusive_number) +
                (request->update_max - request->update_number) +
                (request->shared_max - request->shared_number);
            if (exp_count < 0) {
              exp_count = 1;
            }
            // if (request->deadlock_count >= kD2LMDeadlockLimit) {
            if (request->timestamp.elapsed() >=
                exp_count * kD2LMDeadlockLimit * 2) {
              // cout << "DEADLOCK OR FAIL." << endl;
              // Handle deadlock
              uint64_t from = GetLockValue(
                  request->last_shared_number, request->last_exclusive_number,
                  request->last_shared_number, current_update_max,
                  current_exclusive_max, current_shared_max);
              uint64_t to =
                  GetLockValue(request->update_max, request->exclusive_max,
                               request->shared_max + request->d2lm_increment,
                               current_update_max, current_exclusive_max,
                               current_shared_max);
              this->ResetForDeadlock(context_, *request, from, to);
            } else {
              PerformReadBackoff(*request);
              this->Read(context_, *request);
            }
          }
          break;
        }
        case UPDATE: {
          if (request->exclusive_max == request->exclusive_number &&
              request->shared_max == request->shared_number &&
              request->update_max == request->update_number) {
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, request->contention_count,
                SUCCESS);
          } else if (request->exclusive_max < request->exclusive_number ||
                     request->shared_max < request->shared_number ||
                     request->update_max < request->update_number) {
            // I need to fail -> retry.
            LockStat s;
            s.contention_count = request->contention_count;
            s.contention_count3 = 1;
            local_manager_->NotifyLockRequestResult(
                request->seq_no, request->user_id, request->lock_type,
                remote_lm_id_, request->obj_index, s, FAILURE);
          } else {
            if (request->exclusive_number == request->last_exclusive_number &&
                request->shared_number == request->last_shared_number &&
                request->update_number == request->last_update_number) {
              ++request->deadlock_count;
            } else {
              request->deadlock_count = 0;
              request->timestamp.update();
            }
            request->last_exclusive_number = request->exclusive_number;
            request->last_shared_number = request->shared_number;
            request->last_update_number = request->update_number;
            ++request->contention_count;
            int exp_count =
                (request->exclusive_max - request->exclusive_number) +
                (request->update_max - request->update_number) +
                (request->shared_max - request->shared_number);
            if (exp_count < 0) {
              exp_count = 1;
            }
            // if (request->deadlock_count >= kD2LMDeadlockLimit) {
            if (request->timestamp.elapsed() >=
                exp_count * kD2LMDeadlockLimit * 2) {
              // cout << "DEADLOCK OR FAIL." << endl;
              // Handle deadlock
              uint64_t from = GetLockValue(
                  request->last_update_number, request->last_exclusive_number,
                  request->last_shared_number, current_update_max,
                  current_exclusive_max, current_shared_max);
              uint64_t to =
                  GetLockValue(request->update_max + request->d2lm_increment,
                               request->exclusive_max, request->shared_max,
                               current_update_max, current_exclusive_max,
                               current_shared_max);
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
