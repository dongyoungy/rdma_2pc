#ifndef RDMA_PROTO_NOTIFY_LOCK_CLIENT_H
#define RDMA_PROTO_NOTIFY_LOCK_CLIENT_H

#include <cmath>
#include "lock_client.h"

using namespace std;

namespace rdma { namespace proto {

class NotifyLockClient : public LockClient {
  public:
    NotifyLockClient(const string& work_dir, LockManager* local_manager,
        uint32_t local_user_count,
        uint32_t remote_lm_id);
    ~NotifyLockClient();
    virtual int RequestLock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
        int lock_mode);
    virtual int RequestUnlock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
        int lock_mode);

    int TryLock(int seq_no, int user_id, int lock_type, int obj_index);
  protected:
    int ReadForLock(Context* context, int seq_no, uint32_t user_id, int lock_type, int obj_index);
    int ReadForUnlock(Context* context, int seq_no, uint32_t user_id, int lock_type, int obj_index);
    int UnlockRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type, int obj_index,
        bool is_undo = false);
    int UnlockRemotelyCS(Context* context, int seq_no, uint32_t user_id, int lock_type,
        int obj_index, uint64_t prev_value, uint64_t new_value);
    int LockRemotelyCS(Context* context, int seq_no, uint32_t user_id, int lock_type,
        int obj_index, uint64_t prev_value);
    int NotifyWaitingNodes(LockRequest* request, uint64_t value);
    int GetNumberOfLockWaiters(uint32_t value);
    int FindNodePosition(uint32_t value);
    int FindNodePositions(uint32_t value, int* nodes);

    int HandleWorkCompletion(struct ibv_wc* work_completion);
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
