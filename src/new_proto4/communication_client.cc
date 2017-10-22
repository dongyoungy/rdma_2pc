#include "communication_client.h"

namespace rdma {
namespace proto {

// constructor
CommunicationClient::CommunicationClient(const string& work_dir,
                                         LockManager* local_manager,
                                         uint32_t local_user_count,
                                         int remote_lm_id)
    : LockClient(work_dir, local_manager, local_user_count, remote_lm_id) {
  is_waiting_ack_ = false;
  is_remote_node_dead_ = false;
}

// destructor
CommunicationClient::~CommunicationClient() {}

bool CommunicationClient::IsRemoteNodeDead() {
  Poco::Mutex::ScopedLock lock(communication_mutex_);
  return is_remote_node_dead_;
}

int CommunicationClient::GrantLock(int seq_no, int target_node_id,
                                   uintptr_t user_id, int obj_index,
                                   LockType lock_type) {
  Poco::Mutex::ScopedLock lock(communication_mutex_);
  while (is_waiting_ack_) {
    communication_cond_.wait(communication_mutex_);
  }
  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type = Message::GRANT_LOCK;
  msg->seq_no = seq_no;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->owner_user_id = user_id;
  msg->target_node_id = target_node_id;

  is_waiting_ack_ = true;
  if (SendMessage(context_)) {
    cerr << "GrantLock(): SendMessage() failed." << endl;
    return -1;
  }

  return 0;
}

int CommunicationClient::RejectLock(int seq_no, int target_node_id,
                                    uintptr_t owner_user_id, int obj_index,
                                    LockType lock_type) {
  Poco::Mutex::ScopedLock lock(communication_mutex_);
  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type = Message::REJECT_LOCK;
  msg->seq_no = seq_no;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->target_node_id = target_node_id;
  msg->owner_user_id = owner_user_id;

  if (SendMessage(context_)) {
    cerr << "GrantLock(): SendMessage() failed." << endl;
    return -1;
  }

  return 0;
}

int CommunicationClient::SendTakeover(int from, int to) {
  Poco::Mutex::ScopedLock lock(communication_mutex_);
  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type = Message::TAKEOVER;
  msg->node_from = from;
  msg->node_to = to;

  if (SendMessage(context_)) {
    cerr << "SendTakeover(): SendMessage() failed." << endl;
    return -1;
  }

  return 0;
}

int CommunicationClient::SendHeartbeat() {
  // Poco::Mutex::ScopedLock lock(communication_mutex_);
  // Message* msg = context_->send_message_buffer->GetMessage();

  // msg->type = Message::HEARTBEAT;

  // if (SendMessage(context_)) {
  // cerr << "GrantLock(): SendMessage() failed." << endl;
  // return -1;
  //}

  Poco::Mutex::ScopedLock lock(communication_mutex_);
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  LockRequest* current_request = lock_requests_[lock_request_idx_].get();
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

  send_work_request.wr.rdma.rkey = context_->lock_table_mr->rkey;
  send_work_request.wr.rdma.remote_addr =
      (uint64_t)context_->lock_table_mr->addr;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context_->queue_pair, &send_work_request,
                               &bad_work_request))) {
    cerr << "Read(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }
  ++num_rdma_read_;

  return 0;
}

int CommunicationClient::HandleWorkCompletion(struct ibv_wc* wc) {
  if (wc->status == IBV_WC_RETRY_EXC_ERR || wc->status == IBV_WC_WR_FLUSH_ERR) {
    Poco::Mutex::ScopedLock lock(communication_mutex_);
    cerr << "Dead node detected: " << remote_lm_id_ << endl;
    is_remote_node_dead_ = true;
    return -1;
  } else if (wc->status != IBV_WC_SUCCESS) {
    cerr
        << "CommunicationClient: Work completion status is not IBV_WC_SUCCESS: "
        << wc->status << endl;
    return -1;
  }

  if (wc->opcode == IBV_WC_RECV) {
    Context* context = (Context*)wc->wr_id;
    Message* message = context->receive_message_buffer->GetMessage();
    context->receive_message_buffer->Rotate();
    // post receive first.
    ReceiveMessage(context);
    if (message->type == Message::GRANT_LOCK_ACK) {
      Poco::Mutex::ScopedLock lock(communication_mutex_);
      is_waiting_ack_ = false;
      communication_cond_.signal();
    } else if (message->type == Message::LOCK_TABLE_MR) {
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
    }
  }
  return 0;
}

}  // namespace proto
}  // namespace rdma
