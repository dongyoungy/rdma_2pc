#ifndef RDMA_N_COSED_CONTEXT_H
#define RDMA_N_COSED_CONTEXT_H

#include <pthread.h>
#include "message.h"

namespace rdma { namespace n_cosed {

// forward declaration of LockManager
class LockManager;
// forward declaration of LockClient
class LockClient;

struct Context {
  LockManager* server;
  LockClient* client;

  bool connected;
  struct rdma_cm_id* id;
  struct ibv_qp* queue_pair;

  Message* send_message;
  struct ibv_mr* send_mr;
  Message* receive_message;
  struct ibv_mr* receive_mr;

  int last_home_id;
  int last_user_id;
  int last_lock_type;
  int last_obj_index;
  int last_lock_task;
  int last_shared_count;
  uint64_t last_compare_value;

  uint64_t* lock_table;
  struct ibv_mr* lock_table_mr;

  uint64_t* original_value;
  struct ibv_mr* original_value_mr;

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
