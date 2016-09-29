#include "communication_client.h"

namespace rdma { namespace proto {

// constructor
CommunicationClient::CommunicationClient(const string& work_dir, LockManager* local_manager,
    LockSimulator* local_user,
    int remote_lm_id) : Client(work_dir, local_manager, local_user, remote_lm_id) {
  pthread_mutex_init(&communication_mutex_, NULL);
  is_waiting_ack_ = false;
}

// destructor
CommunicationClient::~CommunicationClient() {

}

int CommunicationClient::GrantLock(int seq_no, int user_id, int home_id,
    int obj_index, int lock_type) {
  while (is_waiting_ack_) {
    usleep(100); // busy-wait
  }
  pthread_mutex_lock(&communication_mutex_);
  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type      = Message::GRANT_LOCK;
  msg->seq_no    = seq_no;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->home_id   = home_id;
  msg->user_id   = user_id;

  //pthread_mutex_lock(&PRINT_MUTEX);
  //cout << "Grant: " << msg->seq_no << "," << msg->user_id << "," <<
    //msg->obj_index << "," << msg->lock_type << endl;
  //pthread_mutex_unlock(&PRINT_MUTEX);

  is_waiting_ack_ = true;
  if (SendMessage(context_)) {
    cerr << "GrantLock(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&communication_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&communication_mutex_);
  return 0;
}

int CommunicationClient::RejectLock(int seq_no, int user_id, int home_id,
    int obj_index, int lock_type) {
  pthread_mutex_lock(&communication_mutex_);
  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type      = Message::REJECT_LOCK;
  msg->seq_no    = seq_no;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->home_id   = home_id;
  msg->user_id   = user_id;

  if (SendMessage(context_)) {
    cerr << "GrantLock(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&communication_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&communication_mutex_);
  return 0;
}


int CommunicationClient::HandleWorkCompletion(struct ibv_wc* wc) {
  if (wc->status != IBV_WC_SUCCESS) {
    cerr << "CommunicationClient: Work completion status is not IBV_WC_SUCCESS: " <<
      wc->status << endl;
    return -1;
  }

  if (wc->opcode == IBV_WC_RECV) {
    Context* context = (Context *)wc->wr_id;
    Message* message = context->receive_message_buffer->GetMessage();
    context->receive_message_buffer->Rotate();
    // post receive first.
    ReceiveMessage(context);
    pthread_mutex_lock(&communication_mutex_);
    if (message->type == Message::GRANT_LOCK_ACK) {
      //pthread_mutex_lock(&PRINT_MUTEX);
      //cout << "Grant ACK: " << message->seq_no << "," << message->user_id << "," <<
        //message->obj_index << "," << message->lock_type << endl;
      //pthread_mutex_unlock(&PRINT_MUTEX);
      is_waiting_ack_ = false;
    }
    pthread_mutex_unlock(&communication_mutex_);
  }
  return 0;
}


}}
