#include "communication_client.h"

namespace rdma {
namespace proto {

// constructor
CommunicationClient::CommunicationClient(const string& work_dir,
                                         LockManager* local_manager,
                                         uint32_t local_user_count,
                                         int remote_lm_id)
    : Client(work_dir, local_manager, local_user_count, remote_lm_id) {
  is_waiting_ack_ = false;
}

// destructor
CommunicationClient::~CommunicationClient() {}

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

int CommunicationClient::HandleWorkCompletion(struct ibv_wc* wc) {
  if (wc->status != IBV_WC_SUCCESS) {
    cerr
        << "CommunicationClient: Work completion status is not IBV_WC_SUCCESS: "
        << wc->status << endl;
    return -1;
  }

  if (wc->opcode == IBV_WC_RECV) {
    Poco::Mutex::ScopedLock lock(communication_mutex_);
    Context* context = (Context*)wc->wr_id;
    Message* message = context->receive_message_buffer->GetMessage();
    context->receive_message_buffer->Rotate();
    // post receive first.
    ReceiveMessage(context);
    if (message->type == Message::GRANT_LOCK_ACK) {
      is_waiting_ack_ = false;
      communication_cond_.signal();
    }
  }
  return 0;
}

}  // namespace proto
}  // namespace rdma
