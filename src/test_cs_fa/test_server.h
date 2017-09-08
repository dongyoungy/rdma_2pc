#ifndef RDMA_TEST_SERVER_H
#define RDMA_TEST_SERVER_H

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <vector>

#include "Poco/Runnable.h"
#include "constants.h"
#include "context.h"

using namespace std;

namespace rdma {
namespace test {

const size_t BUFFER_SIZE = 1024 * 1024;
const size_t MAX_CLIENT = 4096;

class TestServer : public Poco::Runnable {
 public:
  TestServer(const string& work_dir, const string& test_mode_);
  ~TestServer();
  int Run();
  void Stop();
  // uint64_t GetSemaphore() const;
  static void* PollCompletionQueue(void* context);
  static void* PollSemaphore(void* client);
  uint64_t** semaphore_;
  struct ibv_mr** semaphore_mr_;
  uint64_t prev_semaphore_;
  inline void SetDone(bool done) { is_done_ = done; }
  inline bool IsDone() const { return is_done_; }
  virtual void run() { this->Run(); }

 private:
  Context* BuildContext(struct rdma_cm_id* id);
  int PrintInfo();
  int GetInfinibandIP(string& ip_address);
  void BuildQueuePairAttr(Context* context,
                          struct ibv_exp_qp_init_attr* attributes);
  void BuildQueuePairAttr(Context* context,
                          struct ibv_qp_init_attr* attributes);
  int BuildConnectionManagerParams(struct rdma_conn_param* params);
  int RegisterMemoryRegion(Context* context);
  int ReceiveMessage(Context* context);
  int ReceiveEightBytes(Context* context);
  // int WriteSemaphore(Context* context);
  int SendMessage(Context* context);
  int SendBuffer(Context* context);
  int SendDataMemoryRegion(Context* context);
  int HandleWorkCompletion(struct ibv_wc* work_completion);
  int HandleEvent(struct rdma_cm_event* event);
  int HandleConnectRequest(struct rdma_cm_id* id);
  int HandleConnection(Context* context);
  int HandleDisconnect(Context* context);
  void DestroyListener();

  bool is_done_;
  string work_dir_;
  string test_mode_;
  char* buffer_;
  struct ibv_mr* registered_memory_region_;
  struct rdma_cm_id* listener_;
  struct rdma_cm_id* id_;
  struct rdma_event_channel* event_channel_;
  struct sockaddr_in6 address_;
  uint16_t port_;
  size_t data_size_;
  int count_;
  int count2_;
  size_t client_count_;

  std::vector<std::unique_ptr<TestBuffer>> buffers_;

  // Context* context_;

  // testing single protection domain
  struct ibv_pd* pd_;
  struct ibv_srq* srq_;
  pthread_t poll_thread_;
  pthread_mutex_t mutex_;
};

}  // namespace test
}  // namespace rdma

#endif
