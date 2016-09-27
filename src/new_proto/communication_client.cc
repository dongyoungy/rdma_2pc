#include "communication_client.h"

namespace rdma { namespace proto {

// constructor
CommunicationClient::CommunicationClient(const string& work_dir, LockManager* local_manager,
    LockSimulator* local_user,
    int remote_lm_id) : Client(work_dir, local_manager, local_user, remote_lm_id) {
  pthread_mutex_init(&communication_mutex_, NULL);
}

// destructor
CommunicationClient::~CommunicationClient() {

}

int CommunicationClient::GrantLock(int seq_no, int user_id, int home_id,
    int obj_index, int lock_type) {
  pthread_mutex_lock(&communication_mutex_);
  Message* msg = context_->send_message_buffer->GetMessage();

  msg->type      = Message::GRANT_LOCK;
  msg->seq_no    = seq_no;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->home_id   = home_id;
  msg->user_id   = user_id;

  int retry = 0;
  while (SendMessage(context_)) {
    //cerr << "GrantLock(): SendMessage() failed." << endl;
    //pthread_mutex_unlock(&communication_mutex_);
    //return -1;
    ++retry;
    if (retry > 3)
      break;
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
  return 0;
}


}}
