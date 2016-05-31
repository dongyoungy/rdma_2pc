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

#include "context.h"

using namespace std;

namespace rdma { namespace test {

class TestClient {

  public:
    TestClient(const string& server_name, const string& server_port,
        int test_mode, size_t data_size);
    ~TestClient();
    int Run();
    void Stop();
    static void* PollCompletionQueue(void* context);

  private:
    Context* BuildContext(struct rdma_cm_id* id);
    void BuildQueuePairAttr(Context* context,
        struct ibv_qp_init_attr* attributes);
    int BuildConnectionManagerParams(struct rdma_conn_param* params);
    int RegisterMemoryRegion(Context* context);
    int ReceiveMessage(Context* context);
    int SetSemaphore(Context* context, uint64_t current_value,
        uint64_t new_value);
    int ReadData(Context* context);
    int RequestSemaphore(Context* context);
    int RequestData(Context* context);
    int HandleEvent(struct rdma_cm_event* event);
    int HandleAddressResolved(struct rdma_cm_id* id);
    int HandleRouteResolved(struct rdma_cm_id* id);
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);

    static const int TOTAL_TRIAL = 1000000;
    static const int TEST_MODE_SEM = 0;
    static const int TEST_MODE_DATA = 1;

    double total_cas_time_;
    int test_mode_;
    int num_trial_;
    string server_name_;
    string server_port_;
    struct rdma_event_channel* event_channel_;
    struct rdma_cm_id* connection_;
    struct addrinfo* address_;
    struct timespec start_;
    struct timespec end_;
    uint64_t current_semaphore_;
    size_t data_size_;
};

}}

#endif
