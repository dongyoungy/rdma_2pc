#ifndef RDMA_PROTO_LOCK_CLIENT_H
#define RDMA_PROTO_LOCK_CLIENT_H

#include <unordered_map>
#include "Poco/Condition.h"
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
  virtual bool RequestLock(const LockRequest& request, LockMode lock_mode);
  virtual bool RequestUnlock(const LockRequest& request, LockMode lock_mode);
  double GetAverageRemoteExclusiveLockTime() const;
  double GetAverageRemoteSharedLockTime() const;

 protected:
  virtual bool LockRemotely(Context* context, const LockRequest& request);
  virtual bool UnlockRemotely(Context* context, const LockRequest& request,
                              bool is_undo = false, bool retry = false);
  virtual int ReadRemotely(Context* context, const LockRequest& request);
  virtual int ReadRemotely(Context* context, int seq_no, uintptr_t user_id,
                           LockType lock_type, int obj_index);
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

  int UndoLocking(Context* context, const LockRequest& request,
                  bool polling = false);

  std::map<uintptr_t, bool> user_retry_count_;
  std::map<uintptr_t, bool> user_fail_;
  std::map<uintptr_t, bool> user_polling_;
  std::map<uintptr_t, uint32_t> user_waiters_;
  std::map<uintptr_t, uint64_t> user_all_waiters_;

  Poco::Mutex lock_mutex_;

 private:
  volatile bool message_in_progress_;
  map<uint32_t, uint32_t> waitlist_;
};

}  // namespace proto
}  // namespace rdma

#endif
