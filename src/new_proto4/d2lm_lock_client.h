#ifndef RDMA_PROTO_D2LM_LOCK_CLIENT_H
#define RDMA_PROTO_D2LM_LOCK_CLIENT_H

#include <cmath>
#include "lock_client.h"

using namespace std;

namespace rdma {
namespace proto {

class D2LMLockClient : public LockClient {
 public:
  D2LMLockClient(const string& work_dir, LockManager* local_manager,
                 uint32_t local_user_count, uint32_t remote_lm_id);
  ~D2LMLockClient();
  virtual bool RequestLock(const LockRequest& request, LockMode lock_mode);
  virtual bool RequestUnlock(const LockRequest& request, LockMode lock_mode);

  static void SetDeadLockLimit(int limit);

 protected:
  bool Lock(Context* context, const LockRequest& request);
  bool Unlock(Context* context, const LockRequest& request);
  bool Read(Context* context, const LockRequest& request);
  bool ReadForReset(Context* context, const LockRequest& request);
  bool Reset(Context* context, const LockRequest& request);
  bool ResetForDeadlock(Context* context, const LockRequest& request,
                        uint64_t from, uint64_t to);
  bool Undo(Context* context, const LockRequest& request);
  uint64_t GetLockValue(uint16_t exclusive_number, uint16_t shared_number,
                        uint16_t exclusive_max, uint16_t shared_max) const;
  virtual int HandleWorkCompletion(struct ibv_wc* work_completion);

  static int kD2LMDeadlockLimit;

  std::map<uintptr_t, std::map<int, bool>> do_reset_;
  std::map<uintptr_t, std::map<int, uint64_t>> reset_value_;
};

}  // namespace proto
}  // namespace rdma

#endif
