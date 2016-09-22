#ifndef RDMA_TEST_CONTEXT_H
#define RDMA_TEST_CONTEXT_H

#include <pthread.h>
#include "message.h"

namespace rdma { namespace test {

// forward declaration of TestServer
class TestServer;
// forward declaration of TestClient
class TestClient;

static const size_t DATA_SIZE = 4 * 1024;

struct Context {
  TestServer* server;
  TestClient* client;
  bool connected;
  struct rdma_cm_id* id;
  struct ibv_qp* queue_pair;

  Message* send_message;
  struct ibv_mr* send_mr;
  Message* receive_message;
  struct ibv_mr* receive_mr;

  uint64_t* new_value;
  struct ibv_mr* new_value_mr;

  uint64_t* read_value;
  struct ibv_mr* read_value_mr;

  uint64_t* write_value;
  struct ibv_mr* write_value_mr;
  uint64_t* write_value2;
  struct ibv_mr* write_value2_mr;

  uint64_t* server_semaphore;
  struct ibv_mr* rdma_server_semaphore;
  uint64_t* client_semaphore;
  struct ibv_mr* rdma_client_semaphore;

  int* server_data;
  struct ibv_mr* rdma_server_data;

  int* sorted_data;
  struct ibv_mr* sorted_data_mr;

  struct ibv_mr* range_data_mr;

  char* local_buffer;
  struct ibv_mr* rdma_local_mr;
  char* remote_buffer;
  struct ibv_mr* rdma_remote_mr;

  // device context
  struct ibv_context* device_context;
  // protection domain
  struct ibv_pd* protection_domain;
  // completion queue
  struct ibv_cq* completion_queue;
  // completion event channel
  struct ibv_comp_channel* completion_channel;

  // completion queue poller thread
  pthread_t cq_poller_thread;
};

}}

#endif
