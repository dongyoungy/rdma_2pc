#ifndef RDMA_PROTO_LOCK_CLIENT_H
#define RDMA_PROTO_LOCK_CLIENT_H

#include <unordered_map>
#include "client.h"

using namespace std;

namespace rdma { namespace proto {

class LockClient : public Client {

  public:
    LockClient(const string& work_dir, LockManager* local_manager,
        uint32_t local_user_count,
        uint32_t remote_lm_id);
    ~LockClient();
    virtual int RequestLock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
        int lock_mode);
    virtual int RequestUnlock(int seq_no, uint32_t user_id, int lock_type, int obj_index,
        int lock_mode);
    double GetAverageRemoteExclusiveLockTime() const;
    double GetAverageRemoteSharedLockTime() const;

  protected:
    virtual int LockRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type,
        int obj_index);
    virtual int UnlockRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type,
        int obj_index, bool is_undo = false, bool retry = false);
    virtual int ReadRemotely(Context* context, int seq_no, uint32_t user_id, int read_target,
        int lock_type, int obj_index);
    virtual int ReadRemotely(Context* context, int seq_no, uint32_t user_id, int lock_type,
        int obj_index);
    int SendLockTableRequest(Context* context);
    int SendLockModeRequest(Context* context);
    int SendLockRequest(Context* context, int seq_no, uint32_t user_id,
        int lock_type, int obj_index);
    int SendUnlockRequest(Context* context, int seq_no, uint32_t user_id,
        int lock_type, int obj_index);

    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);
    int HandleWorkCompletion(struct ibv_wc* work_completion);

    int HandleSharedToExclusive(LockRequest* request);
    int HandleExclusiveToShared(LockRequest* request);
    int HandleExclusiveToExclusive(LockRequest* request);

    int PollSharedToExclusive(LockRequest* request);
    int PollExclusiveToShared(LockRequest* request);
    int PollExclusiveToExclusive(LockRequest* request);

    int UndoLocking(Context* context, LockRequest* request, bool polling = false);

    int* user_retry_count_;
    bool* user_fail_;
    bool* user_polling_;
    uint32_t* user_waiters_;
    uint64_t* user_all_waiters_;

  private:
    volatile bool message_in_progress_;
    map<uint32_t, uint32_t> waitlist_;
};

}}

#endif
