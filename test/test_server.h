#ifndef RDMA_TEST_SERVER_H
#define RDMA_TEST_SERVER_H

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <iostream>

#include "Context.h"

using namespace std;

namespace rdma { namespace test {

const size_t BUFFER_SIZE = 1024 * 1024;

class TestServer {

  public:
    TestServer();
    int Run();
    void Stop();
    static void* PollCompletionQueue(void* context);

  private:
    Context* BuildContext(struct rdma_cm_id* id);
    void BuildQueuePairAttr(Context* context,
        struct ibv_qp_init_attr* attributes);
    int BuildConnectionManagerParams(struct rdma_conn_param* params);
    int RegisterMemoryRegion();
    int HandleEvent(struct rdma_cm_event* event);
    int HandleConnectRequest(struct rdma_cm_id* id);
    int HandleConnection(struct )
    void DestroyListener();

    char* buffer_;
    struct ibv_mr* registered_memory_region_;
    struct rdma_cm_id* listener_;
    struct rdma_event_channel* event_channel_;
    struct sockaddr_in6 address_;
    uint16_t port_;
};

}}

#endif
