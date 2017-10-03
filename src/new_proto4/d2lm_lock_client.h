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

 protected:
  bool Lock(Context* context, const LockRequest& request);
  bool Unlock(Context* context, const LockRequest& request);
  bool Read(Context* context, const LockRequest& request);
  bool ReadForReset(Context* context, const LockRequest& request);
  bool Reset(Context* context, const LockRequest& request);
  bool Undo(Context* context, const LockRequest& request);
  bool Leave(Context* context, const LockRequest& request);
  virtual int HandleWorkCompletion(struct ibv_wc* work_completion);

  std::map<int, uint16_t> other_max_map_;
  std::map<int, LockType> do_reset_;
  std::map<int, uint64_t> reset_value_;
};

}  // namespace proto
}  // namespace rdma

#endif
