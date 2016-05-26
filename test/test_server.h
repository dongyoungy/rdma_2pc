#ifndef RDMA_TEST_SERVER_H
#define RDMA_TEST_SERVER_H

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <iostream>

using namespace std;

namespace rdma { namespace test {

class TestServer {
  public:
    TestServer();
    int Run();
    void Stop();
  private:
    int HandleEvent(struct rdma_cm_event* event);
    int HandleConnectRequest(struct rdma_cm_id* id);
    int HandleConnection(struct )
    void DestroyListener();

    struct rdma_cm_id* listener_;
    struct rdma_event_channel* event_channel_;
    struct sockaddr_in6 address_;
    uint16_t port_;
};

}}

#endif
