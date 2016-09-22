#ifndef RDMA_TEST_CLIENT_H
#define RDMA_TEST_CLIENT_H

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <iostream>
#include <cmath>

#include "constants.h"
#include "context.h"

using namespace std;

namespace rdma { namespace test {

class TestClient {

  public:
    TestClient(const string& work_dir, int* data, size_t data_size);
    //TestClient(const string& server_name, const string& server_port,
        //int test_mode, size_t data_size);
    ~TestClient();
    int Run();
    void Stop();
    void StopAddingSem();
    uint64_t GetNumAddedSemaphore() const;
    bool IsSemReset() const;
    static void* PollCompletionQueue(void* context);
    static void* PollSemaphore(void* client);

    Context* context_;
    uint64_t semaphore_;
    uint64_t count_;
    uint64_t max_count_;
    uint64_t read_value_;
    struct timespec start_;
    struct timespec end_;
    struct timespec prev_;
    struct timespec now_;
    double* time_taken_;

    int* GetDataFromRangeLocal(int min, int max);
    int* GetDataFromRangeRemote(int min, int max);

    inline int GetTestMode() const {
      return test_mode_;
    }

  private:
    Context* BuildContext(struct rdma_cm_id* id);
    void BuildQueuePairAttr(Context* context,
        struct ibv_exp_qp_init_attr* attributes);
    int BuildConnectionManagerParams(struct rdma_conn_param* params);
    int RegisterMemoryRegion(Context* context);
    int ReceiveMessage(Context* context);
    int AddSemaphore(Context* context);
    int WriteSemaphore(Context* context);
    int ReadServerAddress();
    int ReadData(Context* context);
    int ReadSemaphore(Context* context);
    int RequestSemaphore(Context* context);
    int RequestDataMemoryRegion(Context* context);
    int RequestSortedData(Context* context, int min, int max);
    int HandleEvent(struct rdma_cm_event* event);
    int HandleAddressResolved(struct rdma_cm_id* id);
    int HandleRouteResolved(struct rdma_cm_id* id);
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);
    int RepeatAddingSemaphore(Context* context);

    int* result_;
    int* data_;
    size_t data_size_;
    int* sorted_data_;

    bool is_sem_reset_;
    bool is_adding_sem_;
    int test_duration_;
    uint64_t num_added_sem_;
    string work_dir_;
    double total_cas_time_;
    double total_read_time_;
    int test_mode_;
    int num_trial_;
    string server_name_;
    string server_port_;
    struct rdma_event_channel* event_channel_;
    struct rdma_cm_id* connection_;
    struct addrinfo* address_;
    uint64_t current_semaphore_;
    time_t test_start_;
    time_t test_end_;
    pthread_t poll_thread_;
};

}}

#endif
