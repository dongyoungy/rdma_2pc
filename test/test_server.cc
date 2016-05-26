#include "test_server.h"

namespace rdma { namespace test{

TestServer::TestServer() {
  // constructor
  listener_      = NULL;
  event_channel_ = NULL;
  port_          = 0;
}

int TestServer::Run() {
  memset(&address_, 0x00, sizeof(address_));
  address_.sin6_family = AF_INET6;
  event_channel_ = rdma_create_event_channel();
  if (event_channel_ == NULL) {
    cerr << "rdma_create_event_channel() failed: " << strerror(errno) << endl;
    return -1;
  }
  if (rdma_create_id(event_channel_, &listener_, NULL, RDMA_PS_TCP)) {
    cerr << "rdma_create_id() failed: " << strerror(errno) << endl;
    return -1;
  }
  if (rdma_bind_addr(listener_, (struct sockaddr *)&address_)) {
    cerr << "rdma_bind_addr() failed: " << strerror(errno) << endl;
    return -1;
  }
  if (rdma_listen(listener_, 128)) {
    cerr << "rdma_listen() failed: " << strerror(errno) << endl;
    return -1;
  }
  port = ntohs(rdma_get_src_port(listener_));
  cout << "listning on port " << port << endl;

  struct rdma_cm_event* event = NULL;
  while (rdma_get_cm_event(event_channel_, &event) == 0) {
    struct rdma_cm_event current_event;
    memcpy(&current_event, event, sizeof(*event));
    rdma_ack_cm_event(event);
    if (HandleEvent(&current_event))
      break;
  }

  DestroyListener();
  return 0;
}

void TestServer::DestroyListener() {
  if (listener_)
    rdma_destroy_id(listener_);
  if (event_channel_)
    rdma_destroy_event_channel(event_channel_);
}

void TestServer::Stop() {
  DestroyListener();
  exit(0);
}

// Handles RDMA connection manager events
int TestServer::HandleEvent(struct rdma_cm_event* event) {
  int ret = 0;
  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
    ret = HandleConnectRequest(event->id);
  } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
    ret = HandleConnection(event->id->context);
  } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
    ret = HandleDisconnect(event->id);
  } else {
    cerr << "Unknown event." << endl;
    Stop();
  }

  return ret;
}

}}
