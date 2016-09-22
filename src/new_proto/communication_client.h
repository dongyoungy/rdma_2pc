#ifndef RDMA_PROTO_COMMUNICATION_CLIENT_H
#define RDMA_PROTO_COMMUNICATION_CLIENT_H

#include "client.h"

using namespace std;

namespace rdma { namespace proto {

class CommunicationClient : public Client {

  public:
    CommunicationClient(const string& work_dir, LockManager* local_manager,
        LockSimulator* local_user,
        int remote_lm_id);
    ~CommunicationClient();

    int GrantLock(int home_id, int obj_index, int lock_type);
    int RejectLock(int home_id, int obj_index, int lock_type);

  protected:
    pthread_mutex_t communication_mutex_;
};

}}

#endif
