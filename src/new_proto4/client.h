#ifndef RDMA_PROTO_CLIENT_H
#define RDMA_PROTO_CLIENT_H

#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <iostream>
#include <map>

#include "constants.h"
#include "context.h"
#include "lock_manager.h"
#include "lock_request.h"

using namespace std;

namespace rdma {
namespace proto {

class LockSimulator;
class LockManager;

class Client {
 public:
  Client(const string& work_dir, LockManager* local_manager,
         uint32_t local_user_count, uint32_t remote_lm_id);
  ~Client();
  int Run();
  void Stop();
  bool IsInitialized() const;
  Context* GetContext();
  double GetAverageSendMessageTime() const;
  double GetAverageReceiveMessageTime() const;

  static void* PollCompletionQueue(void* context);

  uint64_t GetRDMASendCount() const;
  uint64_t GetRDMARecvCount() const;
  uint64_t GetRDMAReadCount() const;
  uint64_t GetRDMAWriteCount() const;
  uint64_t GetRDMAAtomicCount() const;
  uint64_t GetNumLockContention() const;
  uint64_t GetNumLockSuccess() const;
  uint64_t GetNumLockSuccessWithPoll() const;
  uint64_t GetSumPollWhenSuccess() const;

  double GetAveragePollWhenSuccess() const;
  double GetAverageRDMAReadTime() const;
  double GetAverageRDMAAtomicTime() const;
  double GetTotalRDMAReadTime() const;
  double GetTotalRDMAAtomicTime() const;

 protected:
  Context* BuildContext(struct rdma_cm_id* id);
  void BuildQueuePairAttr(Context* context,
                          struct ibv_exp_qp_init_attr* attributes);
  int BuildConnectionManagerParams(struct rdma_conn_param* params);
  int RegisterMemoryRegion(Context* context);
  int ReceiveMessage(Context* context);
  int SendMessage(Context* context);
  int ReadServerAddress();

  int HandleEvent(struct rdma_cm_event* event);
  int HandleAddressResolved(struct rdma_cm_id* id);
  int HandleRouteResolved(struct rdma_cm_id* id);
  virtual int HandleConnection(Context* context);
  virtual int HandleDisconnect(Context* context);

  virtual int HandleWorkCompletion(struct ibv_wc* work_completion) = 0;

  uint64_t num_rdma_send_;
  uint64_t num_rdma_recv_;
  uint64_t num_rdma_atomic_;
  uint64_t num_rdma_read_;
  uint64_t num_rdma_write_;

  bool initialized_;
  bool terminate_;
  std::vector<std::unique_ptr<LockRequest>> lock_requests_;
  LockManager* local_manager_;
  LockSimulator* local_user_;
  uint32_t local_user_count_;
  Context* context_;
  uint32_t local_owner_id_;
  uint32_t local_owner_bitvector_id_;
  uint32_t remote_lm_id_;
  int test_duration_;
  int lock_request_idx_;
  string work_dir_;
  double total_send_message_time_;
  double num_send_message_;
  double total_receive_message_time_;
  double num_receive_message_;
  double total_exclusive_lock_remote_time_;
  double total_shared_lock_remote_time_;
  double total_rdma_atomic_time_;
  double total_rdma_read_time_;
  double num_exclusive_lock_;
  double num_shared_lock_;
  int test_mode_;
  int num_trial_;
  string server_name_;
  string server_port_;
  struct rdma_event_channel* event_channel_;
  struct rdma_cm_id* connection_;
  struct addrinfo* address_;
  struct timespec start_;
  struct timespec end_;
  struct timespec start_remote_exclusive_lock_;
  struct timespec end_remote_exclusive_lock_;
  struct timespec start_remote_shared_lock_;
  struct timespec end_remote_shared_lock_;
  struct timespec start_send_message_;
  struct timespec end_send_message_;
  struct timespec start_receive_message_;
  struct timespec end_receive_message_;
  struct timespec start_rdma_read_;
  struct timespec end_rdma_read_;
  struct timespec start_rdma_atomic_;
  struct timespec end_rdma_atomic_;
  uint64_t current_semaphore_;
  uint64_t total_lock_success_;
  uint64_t total_lock_contention_;
  uint64_t sum_poll_when_success_;
  uint64_t total_lock_success_with_poll_;
  size_t data_size_;
  time_t test_start_;
  time_t test_end_;
  pthread_mutex_t mutex_;
  pthread_cond_t lock_cond_;
  pthread_mutex_t msg_mutex_;
};

}  // namespace proto
}  // namespace rdma

#endif
