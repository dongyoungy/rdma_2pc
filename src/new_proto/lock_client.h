#ifndef RDMA_PROTO_LOCK_CLIENT_H
#define RDMA_PROTO_LOCK_CLIENT_H

#include "client.h"

using namespace std;

namespace rdma { namespace proto {

class LockClient : public Client {

  public:
    LockClient(const string& work_dir, LockManager* local_manager,
        LockSimulator* local_user,
        int remote_lm_id);
    ~LockClient();
    int RequestLock(int user_id, int lock_type, int obj_index, int lock_mode);
    int RequestUnlock(int user_id, int lock_type, int obj_index, int lock_mode);
    double GetAverageRemoteExclusiveLockTime() const;
    double GetAverageRemoteSharedLockTime() const;

    uint64_t GetRDMAReadCount() const;
    uint64_t GetRDMAAtomicCount() const;

  protected:
    int LockRemotely(Context* context, int user_id, int lock_type,
        int obj_index);
    int ReadRemotely(Context* context, int user_id, int read_target,
        int obj_index);
    int UnlockRemotely(Context* context, int user_id, int lock_type,
        int obj_index);
    int SendLockTableRequest(Context* context);
    int SendLockModeRequest(Context* context);
    int SendLockRequest(Context* context, int user_id,
        int lock_type, int obj_index);
    int SendUnlockRequest(Context* context, int user_id,
        int lock_type, int obj_index);

    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);
    int HandleWorkCompletion(struct ibv_wc* work_completion);

    int HandleSharedToExclusive(Context* context);
    int HandleExclusiveToShared(Context* context);
    int HandleExclusiveToExclusive(Context* context);

    int PollSharedToExclusive(Context* context);
    int PollExclusiveToShared(Context* context);
    int PollExclusiveToExclusive(Context* context);

    int UndoLocking(Context* context, bool polling = false);
};

}}

#endif
