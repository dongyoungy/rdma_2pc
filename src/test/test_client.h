#ifndef RDMA_TEST_CLIENT_H
#define RDMA_TEST_CLIENT_H

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <iostream>

#include "context.h"

using namespace std;

namespace rdma { namespace test {

class TestClient {

  public:
    TestClient(const string& server_name, const string& server_port);
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
    int SetSemaphore(Context* context);
    int RequestSemaphore(Context* context);
    int HandleEvent(struct rdma_cm_event* event);
    int HandleAddressResolved(struct rdma_cm_id* id);
    int HandleRouteResolved(struct rdma_cm_id* id);
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);

    string server_name_;
    string server_port_;
    struct rdma_event_channel* event_channel_;
    struct rdma_cm_id* connection_;
    struct addrinfo* address_;
};

}}

#endif
