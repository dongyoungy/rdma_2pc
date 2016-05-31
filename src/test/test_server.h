#ifndef RDMA_TEST_SERVER_H
#define RDMA_TEST_SERVER_H

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <iostream>

#include "context.h"

using namespace std;

namespace rdma { namespace test {

const size_t BUFFER_SIZE = 1024 * 1024;

class TestServer {

  public:
    TestServer(size_t data_size);
    ~TestServer();
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
    int SendMessage(Context* context);
    int SendSemaphoreMemoryRegion(Context* context);
    int SendDataMemoryRegion(Context* context);
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleEvent(struct rdma_cm_event* event);
    int HandleConnectRequest(struct rdma_cm_id* id);
    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);
    void DestroyListener();

    uint64_t semaphore_;
    char* buffer_;
    struct ibv_mr* registered_memory_region_;
    struct rdma_cm_id* listener_;
    struct rdma_event_channel* event_channel_;
    struct sockaddr_in6 address_;
    uint16_t port_;
    size_t data_size_;
};

}}

#endif
