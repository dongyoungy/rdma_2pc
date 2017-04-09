#ifndef RDMA_PROTO_CONTEXT_H
#define RDMA_PROTO_CONTEXT_H

#include <pthread.h>
#include "message_buffer.h"

namespace rdma { namespace proto {

// forward declaration of LockManager
class LockManager;
// forward declaration of LockClient
class Client;
// forward declaration of MessageBuffer
class MessageBuffer;

struct Context {
  LockManager* server;
  Client* client;

  bool connected;
  //bool fail;
  //bool polling;
  struct rdma_cm_id* id;
  struct ibv_qp* queue_pair;

  /*Message* send_message;*/
  /*struct ibv_mr* send_mr;*/
  /*Message* receive_message;*/
  /*struct ibv_mr* receive_mr;*/

  MessageBuffer* send_message_buffer;
  MessageBuffer* receive_message_buffer;

  //int read_purpose;
  //int last_user_id;
  //int last_lock_type;
  //int last_obj_index;
  //int last_lock_task;
  //int last_read_target;
  //int last_seq_no;
  //int retry;

  //uint32_t shared;
  //uint32_t exclusive;
  //uint32_t waiters;
  //uint64_t all_waiters;

  uint64_t* lock_table;
  struct ibv_mr* lock_table_mr;

  uint64_t* original_value;
  struct ibv_mr* original_value_mr;

  uint32_t* read_buffer;
  struct ibv_mr* read_buffer_mr;

  // read buffer for reading entire lock object
  uint64_t* read_buffer2;
  struct ibv_mr* read_buffer2_mr;

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
