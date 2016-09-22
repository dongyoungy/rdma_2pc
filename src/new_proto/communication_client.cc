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

int CommunicationClient::GrantLock(int home_id, int obj_index, int lock_type) {

}


}}
