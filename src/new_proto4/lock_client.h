#ifndef RDMA_PROTO_LOCK_CLIENT_H
#define RDMA_PROTO_LOCK_CLIENT_H

#include <unordered_map>
#include "Poco/Mutex.h"
#include "client.h"

using namespace std;

namespace rdma {
namespace proto {

class LockClient : public Client {
 public:
  LockClient(const string& work_dir, LockManager* local_manager,
             uint32_t local_user_count, uint32_t remote_lm_id);
  ~LockClient();
  virtual bool RequestLock(const LockRequest& request, int lock_mode);
  virtual bool RequestUnlock(const LockRequest& request, int lock_mode);
  double GetAverageRemoteExclusiveLockTime() const;
  double GetAverageRemoteSharedLockTime() const;

 protected:
  virtual bool LockRemotely(Context* context, const LockRequest& request);
  virtual bool UnlockRemotely(Context* context, const LockRequest& request,
                              bool is_undo = false, bool retry = false);
  virtual int ReadRemotely(Context* context, int seq_no, uint32_t user_id,
                           int read_target, int lock_type, int obj_index);
  virtual int ReadRemotely(Context* context, int seq_no, uint32_t user_id,
                           int lock_type, int obj_index);
  int SendLockTableRequest(Context* context);
  int SendLockModeRequest(Context* context);
  bool SendLockRequest(Context* context, const LockRequest& request);
  bool SendUnlockRequest(Context* context, const LockRequest& request);

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

  Poco::Mutex lock_mutex_;

 private:
  volatile bool message_in_progress_;
  map<uint32_t, uint32_t> waitlist_;
};

}  // namespace proto
}  // namespace rdma

#endif
