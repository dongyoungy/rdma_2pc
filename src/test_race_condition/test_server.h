#ifndef RDMA_TEST_SERVER_H
#define RDMA_TEST_SERVER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <rdma/rdma_cma.h>
#include <iostream>

#include "context.h"

using namespace std;

namespace rdma { namespace test {

const size_t BUFFER_SIZE = 1024 * 1024;

class TestServer {

  public:
    TestServer(const string& work_dir, size_t data_size);
    ~TestServer();
    int Run();
    void Stop();
    uint64_t GetSemaphore() const;
    static void* PollCompletionQueue(void* context);

  private:
    Context* BuildContext(struct rdma_cm_id* id);
    int PrintInfo();
    int GetInfinibandIP(string& ip_address);
    void BuildQueuePairAttr(Context* context,
        struct ibv_exp_qp_init_attr* attributes);
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

    string work_dir_;
    uint64_t semaphore_;
    char* buffer_;
    struct ibv_mr* registered_memory_region_;
    struct rdma_cm_id* listener_;
    struct rdma_event_channel* event_channel_;
    struct sockaddr_in6 address_;
    uint16_t port_;
    size_t data_size_;
    int count_;
    int count2_;

    Context** contexts_;

    // testing single protection domain
    struct ibv_pd* pd_;
    struct ibv_srq* srq_;
};

}}

#endif
