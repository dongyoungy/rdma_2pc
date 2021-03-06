#ifndef RDMA_PROTO_DIRECT_QUEUE_LOCK_CLIENT_TWO_H
#define RDMA_PROTO_DIRECT_QUEUE_LOCK_CLIENT_TWO_H

#include <cmath>
#include "lock_client.h"

using namespace std;

namespace rdma { namespace proto {

class DirectQueueLockClientTwo : public LockClient {
  public:
    DirectQueueLockClientTwo(const string& work_dir, LockManager* local_manager,
        LockSimulator* local_user,
        uint32_t remote_lm_id);
    ~DirectQueueLockClientTwo();

  protected:
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleShared(LockRequest* request);
    int HandleExclusive(LockRequest* request);

  private:
    map<uint32_t, uint64_t> wait_after_me_;
    map<uint32_t, uint64_t> wait_before_me_;

    int wait_seq_no_;
    uint32_t wait_user_id_;
    int wait_lock_type_;
    int wait_obj_index_;
    pthread_mutex_t wait_mutex_;
    pthread_cond_t wait_cond_;
};

}}


#endif
