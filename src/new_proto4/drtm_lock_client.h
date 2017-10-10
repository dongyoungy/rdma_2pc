#ifndef RDMA_PROTO_DRTM_LOCK_CLIENT_H
#define RDMA_PROTO_DRTM_LOCK_CLIENT_H

#include <cmath>
#include "lock_client.h"

using namespace std;

namespace rdma {
namespace proto {

class DRTMLockClient : public LockClient {
 public:
  DRTMLockClient(const string& work_dir, LockManager* local_manager,
                 uint32_t local_user_count, uint32_t remote_lm_id);
  ~DRTMLockClient();

 protected:
  virtual int HandleWorkCompletion(struct ibv_wc* work_completion);
};

}  // namespace proto
}  // namespace rdma

#endif
