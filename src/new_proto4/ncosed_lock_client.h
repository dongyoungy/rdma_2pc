#ifndef RDMA_PROTO_NCOSED_LOCK_CLIENT_H
#define RDMA_PROTO_NCOSED_LOCK_CLIENT_H

#include <cmath>
#include "lock_client.h"

using namespace std;

namespace rdma {
namespace proto {

class NCOSEDLockClient : public LockClient {
 public:
  NCOSEDLockClient(const string& work_dir, LockManager* local_manager,
                   uint32_t local_user_count, uint32_t remote_lm_id);
  ~NCOSEDLockClient();
  virtual bool RequestLock(const LockRequest& request, LockMode lock_mode);
  virtual bool RequestUnlock(const LockRequest& request, LockMode lock_mode);

  bool UnlockShared(const Message& msg, int shared_count);

 protected:
  bool Lock(Context* context, const LockRequest& request);
  bool Lock(Context* context, const LockRequest& request, uint64_t prev_value);
  bool Unlock(Context* context, const LockRequest& request);
  bool UnlockShared(Context* context, const LockRequest& request,
                    uint32_t shared);
  bool UnlockBoth(Context* context, const LockRequest& request,
                  uint32_t exclusive, uint32_t shared);
  virtual int HandleWorkCompletion(struct ibv_wc* work_completion);
};

}  // namespace proto
}  // namespace rdma

#endif
