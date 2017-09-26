#ifndef RDMA_PROTO_NOTIFY_LOCK_CLIENT_H
#define RDMA_PROTO_NOTIFY_LOCK_CLIENT_H

#include <cmath>
#include "lock_client.h"

using namespace std;

namespace rdma {
namespace proto {

class NotifyLockClient : public LockClient {
 public:
  NotifyLockClient(const string& work_dir, LockManager* local_manager,
                   uint32_t local_user_count, uint32_t remote_lm_id);
  ~NotifyLockClient();

  int TryLock(const Message& message);
  int ReadRemotely(Context* context, const Message& message);

 protected:
  int ReadForLock(Context* context, int seq_no, uint32_t user_id,
                  LockType lock_type, int obj_index);
  int ReadForUnlock(Context* context, int seq_no, uint32_t user_id,
                    LockType lock_type, int obj_index);
  int NotifyWaitingNodes(LockRequest* request, uint64_t value);
  int FindNodePosition(uint32_t value);
  int FindNodePositions(uint32_t value, int* nodes);

  int HandleWorkCompletion(struct ibv_wc* work_completion);
};

}  // namespace proto
}  // namespace rdma

#endif
