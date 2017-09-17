#ifndef RDMA_PROTO_COMMUNICATION_CLIENT_H
#define RDMA_PROTO_COMMUNICATION_CLIENT_H

#include "client.h"

using namespace std;

namespace rdma {
namespace proto {

class CommunicationClient : public Client {
 public:
  CommunicationClient(const string& work_dir, LockManager* local_manager,
                      uint32_t local_user_count, int remote_lm_id);
  ~CommunicationClient();

  int GrantLock(int seq_no, int target_node_id, int owner_user_id,
                int obj_index, LockType lock_type);
  int RejectLock(int seq_no, int target_node_id, int owner_user_id,
                 int obj_index, LockType lock_type);

 protected:
  pthread_mutex_t communication_mutex_;
  virtual int HandleWorkCompletion(struct ibv_wc* work_completion);

 private:
  volatile bool is_waiting_ack_;
};

}  // namespace proto
}  // namespace rdma

#endif
