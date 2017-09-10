#ifndef RDMA_TEST_CLIENT_H
#define RDMA_TEST_CLIENT_H

#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <cmath>
#include <iostream>
#include <vector>

#include "Poco/Runnable.h"
#include "Poco/Timestamp.h"
#include "constants.h"
#include "context.h"

using namespace std;

namespace rdma {
namespace test {

class TestClient : public Poco::Runnable {
 public:
  TestClient(const string& work_dir, const string& test_mode);
  ~TestClient();
  int Run();
  void Stop();
  void StopAddingSem();
  uint64_t GetNumAddedSemaphore() const;
  bool IsSemReset() const;
  static void* PollCompletionQueue(void* context);
  static void* PollSendCompletionQueue(void* context);
  static void* PollSemaphore(void* client);

  int StartWorkers();
  static void* RunWorker(void* worker);

  Context* context_;
  uint64_t semaphore_;
  uint64_t count_;
  uint64_t max_count_;
  uint64_t read_value_;
  struct timespec start_;
  struct timespec end_;
  struct timespec prev_;
  struct timespec now_;

  inline string GetTestMode() const { return test_mode_; }
  inline uint64_t GetCount() { return count_; }
  inline double GetTotalTimeTaken() { return total_time_taken_; }
  inline bool isDone() const { return is_done_; }
  inline bool isReady() const { return is_ready_; }
  virtual void run() { this->Run(); }

 private:
  Context* BuildContext(struct rdma_cm_id* id);
  void BuildQueuePairAttr(Context* context,
                          struct ibv_exp_qp_init_attr* attributes);
  void BuildQueuePairAttr(Context* context,
                          struct ibv_qp_init_attr* attributes);
  int BuildConnectionManagerParams(struct rdma_conn_param* params);
  int RegisterMemoryRegion(Context* context);
  int ReceiveMessage(Context* context);
  int AddSemaphore(Context* context);
  int SwapSemaphore(Context* context);
  int WriteSemaphore(Context* context);
  int SendSemaphore(Context* context);
  int ReadServerAddress();
  int ReadData(Context* context);
  int ReadSemaphore(Context* context);
  int RequestBuffer(Context* context);
  int HandleEvent(struct rdma_cm_event* event);
  int HandleAddressResolved(struct rdma_cm_id* id);
  int HandleRouteResolved(struct rdma_cm_id* id);
  int HandleWorkCompletion(struct ibv_wc* work_completion);
  int HandleConnection(Context* context);
  int HandleDisconnect(Context* context);
  int RepeatAddingSemaphore(Context* context);

  static const int TOTAL_TRIAL = 1000000;

  bool is_ready_;
  bool is_done_;
  bool is_sem_reset_;
  bool is_adding_sem_;
  int test_duration_;
  int last_add_value_;
  int num_workers_;
  uint64_t num_added_sem_;
  string work_dir_;
  double total_cas_time_;
  double total_read_time_;
  double total_time_taken_;
  double* latency_;
  string test_mode_;
  int num_trial_;
  string server_name_;
  string server_port_;
  struct rdma_event_channel* event_channel_;
  struct rdma_cm_id* connection_;
  struct addrinfo* address_;
  uint64_t current_semaphore_;
  size_t data_size_;
  time_t test_start_;
  time_t test_end_;
  pthread_t poll_thread_;
  vector<pthread_t*> worker_threads_;

  Poco::Timestamp start_timestamp_;
};

}  // namespace test
}  // namespace rdma

#endif
