#include "lock_client.h"

namespace rdma { namespace proto {

// constructor
LockClient::LockClient(const string& work_dir, LockManager* local_manager,
    LockSimulator* local_user,
    int remote_lm_id) {
  work_dir_                         = work_dir;
  context_                          = NULL;
  event_channel_                    = NULL;
  connection_                       = NULL;
  address_                          = NULL;
  local_manager_                    = local_manager;
  local_user_                       = local_user;
  remote_lm_id_                     = remote_lm_id;
  total_exclusive_lock_remote_time_ = 0;
  total_shared_lock_remote_time_    = 0;
  num_exclusive_lock_               = 0;
  num_shared_lock_                  = 0;

  // initialize local lock mutex
  pthread_mutex_init(&lock_mutex_, NULL);
}

// destructor
LockClient::~LockClient() {
}

Context* LockClient::GetContext() {
  return context_;
}

int LockClient::Run() {
  // read server address from file
  if (ReadServerAddress()) {
    cerr << "Run(): ReadServerAddress() failed" << endl;
    return -1;
  }

  cout << "connecting to server: " << server_name_ << ":" << server_port_
    << endl;
  int ret = 0;
  if ((ret = getaddrinfo(server_name_.c_str(), server_port_.c_str(), NULL,
          &address_))) {
    cerr << "Run(): getaddrinfo() failed: " << gai_strerror(ret) << endl;
    return -1;
  }

  event_channel_ = rdma_create_event_channel();
  if (event_channel_ == NULL) {
    cerr << "Run(): rdma_create_event_channel() failed: " <<
      strerror(errno) << endl;
    return -1;
  }
  if (rdma_create_id(event_channel_, &connection_, NULL, RDMA_PS_TCP)) {
    cerr << "Run(): rdma_create_id() failed: " << strerror(errno) << endl;
    return -1;
  }
  if (rdma_resolve_addr(connection_, NULL, address_->ai_addr, 1000)) {
    cerr << "Run(): rdma_resolve_addr() failed: " << strerror(errno) << endl;
    return -1;
  }

  freeaddrinfo(address_);

  struct rdma_cm_event* event = NULL;
  while (rdma_get_cm_event(event_channel_, &event) == 0) {
    struct rdma_cm_event current_event;
    memcpy(&current_event, event, sizeof(current_event));
    rdma_ack_cm_event(event);
    if (HandleEvent(&current_event))
      break;
  }

  rdma_destroy_event_channel(event_channel_);

  return 0;
}

void LockClient::Stop() {
  exit(0);
}

int LockClient::ReadServerAddress() {
  char ip[64];
  char port[16];

  // open files
  char ip_filename[256];
  char port_filename[256];
  if (sprintf(ip_filename, "%s/lm%04d.ip", work_dir_.c_str(),
        remote_lm_id_) < 0) {
    cerr << "PrintInfo(): sprintf() failed." << endl;
    return -1;
  }
  if (sprintf(port_filename, "%s/lm%04d.port", work_dir_.c_str(),
        remote_lm_id_) < 0) {
    cerr << "PrintInfo(): sprintf() failed." << endl;
    return -1;
  }

  FILE* ip_file = fopen(ip_filename, "r");
  if (ip_file == NULL) {
    cerr << "ReadServerAddress(): fopen() failed: " << strerror(errno) << endl;
    return -1;
  }
  FILE* port_file = fopen(port_filename, "r");
  if (port_file == NULL) {
    cerr << "ReadServerAddress(): fopen() failed: " << strerror(errno) << endl;
    return -1;
  }

  fgets(ip, 64, ip_file);
  fgets(port, 16, port_file);

  char* pos;
  // let's remove trailing newline character
  if ((pos = strchr(ip, '\n')) != NULL) {
    *pos = '\0';
  }
  if ((pos = strchr(port, '\n')) != NULL) {
    *pos = '\0';
  }

  server_name_ = ip;
  server_port_ = port;

  fclose(ip_file);
  fclose(port_file);

  return 0;
}

int LockClient::HandleEvent(struct rdma_cm_event* event) {
  int ret = 0;
  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED) {
    ret = HandleAddressResolved(event->id);
  } else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
    ret = HandleRouteResolved(event->id);
  } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
    ret = HandleConnection(static_cast<Context*>(event->id->context));
  } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
    ret = HandleDisconnect(static_cast<Context*>(event->id->context));
  } else {
    cerr << "Unknown event: " << event->event << endl;
    Stop();
  }

  return ret;
}

int LockClient::HandleAddressResolved(struct rdma_cm_id* id) {

  context_ = BuildContext(id);
  if (context_ == NULL) {
    cerr << "LockClient: BuildContext() failed." << endl;
    return -1;
  }

  struct ibv_exp_qp_init_attr queue_pair_attributes;
  memset(&queue_pair_attributes, 0x00, sizeof(queue_pair_attributes));
  BuildQueuePairAttr(context_, &queue_pair_attributes);

  //if (rdma_create_qp(id, context_->protection_domain,
        //&queue_pair_attributes)) {
    //cerr << "rdma_create_qp() failed: " << strerror(errno) << endl;
    //return -1;
  //}

  struct ibv_qp* queue_pair = ibv_exp_create_qp(id->verbs,
      &queue_pair_attributes);
  if (queue_pair == NULL) {
    cerr << "ibv_exp_create_qp() failed." << endl;
    return -1;
  }
  id->qp = queue_pair;

  // set context for connection
  id->context = context_;
  context_->queue_pair = id->qp;

  // create memory regions for the connection
  if (RegisterMemoryRegion(context_)) {
    cerr << "RegisterMemoryRegion() failed." << endl;
    return -1;
  }

  // post receive to handle MR information from server
  if (ReceiveMessage(context_)) {
    cerr << "ReceiveMessage() failed." << endl;
    return -1;
  }

  // resolve route
  if (rdma_resolve_route(id, 1000)) {
    cerr << "rdma_resolve_route() failed: " << strerror(errno) << endl;
    return -1;
  }

  cout << "address resolved." << endl;

  return 0;
}

int LockClient::HandleRouteResolved(struct rdma_cm_id* id) {
  struct rdma_conn_param connection_parameters;
  memset(&connection_parameters, 0x00, sizeof(connection_parameters));
  connection_parameters.initiator_depth =
    connection_parameters.responder_resources = 5;
  connection_parameters.rnr_retry_count = 5;

  // connect
  if (rdma_connect(id, &connection_parameters)) {
    cerr << "rdma_connect() failed: " << strerror(errno) << endl;
    return -1;
  }

  cout << "route resolved." << endl;

  return 0;
}

int LockClient::HandleConnection(Context* context) {
  //cout << "connected to server." << endl;
  context->connected = true;

  SendLockTableRequest(context);

  return 0;
}

int LockClient::HandleDisconnect(Context* context) {

  //rdma_destroy_qp(context->id);

  if (context->send_mr)
    ibv_dereg_mr(context->send_mr);
  if (context->receive_mr)
    ibv_dereg_mr(context->receive_mr);
  //if (context->lock_table_mr)
    //ibv_dereg_mr(context->lock_table_mr);
  if (context->original_value_mr)
    ibv_dereg_mr(context->original_value_mr);

  delete context->send_message;
  delete context->receive_message;

  //rdma_destroy_id(context->id);

  delete context;

  //cout << "disconnected." << endl;
  return 0;
}

int LockClient::SendMessage(Context* context) {
  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context->send_message;
  sge.length = sizeof(*context->send_message);
  sge.lkey   = context->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  //cout << "SendMessage(): message sent." << endl;

  return 0;
}

// Post receive to get message from clients
int LockClient::ReceiveMessage(Context* context) {
  struct ibv_recv_wr receive_work_request;
  struct ibv_recv_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&receive_work_request, 0x00, sizeof(receive_work_request));

  receive_work_request.wr_id   = (uint64_t)context;
  receive_work_request.next    = NULL;
  receive_work_request.sg_list = &sge;
  receive_work_request.num_sge = 1;

  sge.addr   = (uint64_t)context->receive_message;
  sge.length = sizeof(*context->receive_message);
  sge.lkey   = context->receive_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_recv(context->queue_pair, &receive_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_recv failed: " << strerror(ret) << endl;
    return -1;
  }

  return 0;
}


// Register local memory regions for RDMA
int LockClient::RegisterMemoryRegion(Context* context) {

  context->send_message = new Message;
  context->receive_message = new Message;

  context->send_mr = ibv_reg_mr(context->protection_domain,
      context->send_message,
      sizeof(*(context->send_message)),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
  if (context->send_mr == NULL) {
    cerr << "ibv_reg_mr() failed for send_mr." << endl;
    return -1;
  }
  context->receive_mr = ibv_reg_mr(context->protection_domain,
      context->receive_message,
      sizeof(*(context->receive_message)),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  if (context->receive_mr == NULL) {
    cerr << "ibv_reg_mr() failed for receive_mr." << endl;
    return -1;
  }
  context->original_value = new uint64_t;
  context->original_value_mr = ibv_reg_mr(context->protection_domain,
      context->original_value,
      sizeof(*context->original_value),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
      | IBV_ACCESS_REMOTE_ATOMIC);
  if (context->original_value_mr == NULL) {
    cerr << "ibv_reg_mr() failed for original_value_mr." << endl;
    return -1;
  }

  return 0;
}

// Builds queue pair attributes
void LockClient::BuildQueuePairAttr(Context* context,
    struct ibv_exp_qp_init_attr* attributes) {
  memset(attributes, 0x00, sizeof(*attributes));

  attributes->pd               = context->protection_domain;
  attributes->send_cq          = context->completion_queue;
  attributes->recv_cq          = context->completion_queue;
  attributes->qp_type          = IBV_QPT_RC;
  attributes->cap.max_send_wr  = 16;
  attributes->cap.max_recv_wr  = 16;
  attributes->cap.max_send_sge = 1;
  attributes->cap.max_recv_sge = 1;
  attributes->comp_mask        = IBV_EXP_QP_INIT_ATTR_PD |
    IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS;
  attributes->exp_create_flags = IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY;
  attributes->max_atomic_arg   = sizeof(uint64_t);
}

Context* LockClient::BuildContext(struct rdma_cm_id* id) {

  // create new context for the connection
  Context* new_context = new Context;
  new_context->server = NULL;
  new_context->client = this;
  new_context->connected = false;
  new_context->device_context = id->verbs;
  if ((new_context->protection_domain =
        ibv_alloc_pd(new_context->device_context)) == NULL) {
    cerr << "LockClient: ibv_alloc_pd() failed." << endl;
    return NULL;
  }
  if ((new_context->completion_channel =
        ibv_create_comp_channel(new_context->device_context)) == NULL) {
    cerr << "LockClient: ibv_create_comp_channel() failed." << endl;
    return NULL;
  }
  if ((new_context->completion_queue =
        ibv_create_cq(new_context->device_context, 64,
          NULL, new_context->completion_channel, 0)) == NULL) {
    cerr << "LockClient: ibv_create_cq() failed." << endl;
    return NULL;
  }
  if (ibv_req_notify_cq(new_context->completion_queue, 0)) {
    cerr << "LockClient: ibv_req_notify_cq() failed." << endl;
    return NULL;
  }
  // create completion queue poller thread
  if (pthread_create(&new_context->cq_poller_thread, NULL,
        &LockClient::PollCompletionQueue, new_context)) {
     cerr << "LockClient: pthread_create() failed." << endl;
     return NULL;
  }

  return new_context;
}

int LockClient::HandleWorkCompletion(struct ibv_wc* work_completion) {
  Context* context = (Context *)work_completion->wr_id;

  if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "Work completion status is not IBV_WC_SUCCESS: " <<
      work_completion->status << endl;
    return -1;
  }

  if (work_completion->opcode == IBV_WC_RECV) {
    // post receive first.
    ReceiveMessage(context);

    // if received lock table MR info
    if (context->receive_message->type == Message::LOCK_TABLE_MR) {
      //cout << "received lock table MR." << endl;
      // copy server rdma semaphore region
      context->lock_table_mr = new ibv_mr;
      memcpy(context->lock_table_mr,
          &context->receive_message->lock_table_mr,
          sizeof(*context->lock_table_mr));
    } else if (context->receive_message->type == Message::LOCK_REQUEST_RESULT) {
      //cout << "received lock request result." << endl;
      local_manager_->NotifyLockRequestResult(context->receive_message->user_id,
          context->receive_message->lock_type,
          context->receive_message->obj_index,
          context->receive_message->lock_result);
    } else if (context->receive_message->type == Message::UNLOCK_REQUEST_RESULT) {
      //cout << "received unlock request result" << endl;
      local_manager_->NotifyUnlockRequestResult(
          context->receive_message->user_id,
          context->receive_message->lock_type,
          context->receive_message->obj_index,
          context->receive_message->lock_result);
    }
  } else if (work_completion->opcode == IBV_WC_COMP_SWAP) {
    // completion of compare-and-swap, i.e. remote exclusive locking

    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_remote_exclusive_lock_);
    double time_taken = ((double)end_remote_exclusive_lock_.tv_sec * 1e+9 +
        (double)end_remote_exclusive_lock_.tv_nsec) -
      ((double)start_remote_exclusive_lock_.tv_sec * 1e+9 +
          (double)start_remote_exclusive_lock_.tv_nsec);
    total_exclusive_lock_remote_time_ += time_taken;
    ++num_exclusive_lock_;

    uint64_t prev_value = *context->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value)>>32);
    shared = (uint32_t)value;

    if (context->last_lock_task == LockManager::TASK_LOCK) {
      // it should have been successful since exclusive and shared was 0
      if (exclusive == 0 && shared == 0) {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_FAILURE);
      }
    } else if (context->last_lock_task == LockManager::TASK_UNLOCK) {
      if (exclusive == context->last_user_id && shared == 0) {
        local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else if (exclusive == context->last_user_id && shared != 0) {
        local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_RETRY);
      } else {
        local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_FAILURE);
      }
    }
  } else if (work_completion->opcode == IBV_WC_FETCH_ADD) {
    // completion of fetch-and-add, i.e. remote shared locking

    // get time
    clock_gettime(CLOCK_MONOTONIC, &end_remote_shared_lock_);
    double time_taken = ((double)end_remote_shared_lock_.tv_sec * 1e+9 +
        (double)end_remote_shared_lock_.tv_nsec) -
      ((double)start_remote_shared_lock_.tv_sec * 1e+9 +
          (double)start_remote_shared_lock_.tv_nsec);
    total_shared_lock_remote_time_ += time_taken;
    ++num_shared_lock_;

    uint64_t prev_value = *context->original_value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
    uint64_t value = __bswap_constant_64(prev_value);  // Compiler builtin
#endif
    uint32_t exclusive, shared;
    exclusive = (uint32_t)((value)>>32);
    shared = (uint32_t)value;

    if (context->last_lock_task == LockManager::TASK_LOCK) {
      // it should have been successful since exclusive and shared was 0
      if (exclusive == 0) {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        local_manager_->NotifyLockRequestResult(context->last_user_id,
            context->last_lock_type,
            context->last_obj_index,
            LockManager::RESULT_FAILURE);
      }
    } else if (context->last_lock_task == LockManager::TASK_UNLOCK) {
      // unlock always succeeds. (really?)
      local_manager_->NotifyUnlockRequestResult(context->last_user_id,
          context->last_lock_type,
          context->last_obj_index,
          LockManager::RESULT_SUCCESS);
    }
  }

  return 0;
}

// Requests lock table MR region of from lock manager via IBV_WR_SEND op.
int LockClient::SendLockTableRequest(Context* context) {

  context->send_message->type = Message::LOCK_TABLE_MR_REQUEST;

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context->send_message;
  sge.length = sizeof(*context->send_message);
  sge.lkey   = context->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  //cout << "requested lock table MR" << endl;

  return 0;
}

int LockClient::RequestLock(int user_id, int lock_type, int obj_index,
    int lock_mode) {
  if (lock_mode == LockManager::LOCK_LOCAL) {
    // ask lock manager to place the lock
    return this->SendLockRequest(context_, user_id, lock_type, obj_index);
  } else if (lock_mode == LockManager::LOCK_REMOTE) {
    // try locking remotely
    return this->LockRemotely(context_, user_id, lock_type, obj_index);
  } else {
    cerr << "RequestLock(): Unknown lock mode: " << lock_mode << endl;
  }
}

int LockClient::RequestUnlock(int user_id, int lock_type, int obj_index,
    int lock_mode) {
  if (lock_mode == LockManager::LOCK_LOCAL) {
    return this->SendUnlockRequest(context_, user_id, lock_type, obj_index);
  } else if (lock_mode == LockManager::LOCK_REMOTE) {
    return this->UnlockRemotely(context_, user_id, lock_type, obj_index);
  } else {
    cerr << "RequestUnlock(): Unknown lock mode: " << lock_mode << endl;
  }
}

int LockClient::LockRemotely(Context* context, int user_id, int lock_type,
    int obj_index) {

  if (lock_type == LockManager::SHARED) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_shared_lock_);
  } else if (lock_type == LockManager::EXCLUSIVE) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_exclusive_lock_);
  }

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  context->last_user_id   = user_id;
  context->last_lock_type = lock_type;
  context->last_obj_index = obj_index;
  context->last_lock_task = LockManager::TASK_LOCK;

  sge.addr = (uint64_t)context->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = context->original_value_mr->lkey;

  send_work_request.wr_id      = (uint64_t)context;
  send_work_request.num_sge    = 1;
  send_work_request.sg_list    = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;

  if (lock_type == LockManager::SHARED) {
    send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    send_work_request.wr.atomic.compare_add = 1;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    exclusive = user_id;
    shared = 0;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
    send_work_request.wr.atomic.compare_add = (uint64_t)0ULL;
    send_work_request.wr.atomic.swap        = new_value;
  }

  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  send_work_request.wr.atomic.rkey        =
    context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "LockRemotely(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  return 0;
}


int LockClient::UnlockRemotely(Context* context, int user_id, int lock_type,
    int obj_index) {

  if (lock_type == LockManager::SHARED) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_shared_lock_);
  } else if (lock_type == LockManager::EXCLUSIVE) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_exclusive_lock_);
  }

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  context->last_user_id   = user_id;
  context->last_lock_type = lock_type;
  context->last_obj_index = obj_index;
  context->last_lock_task = LockManager::TASK_UNLOCK;

  sge.addr = (uint64_t)context->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = context->original_value_mr->lkey;

  send_work_request.wr_id      = (uint64_t)context;
  send_work_request.num_sge    = 1;
  send_work_request.sg_list    = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;

  if (lock_type == LockManager::SHARED) {
    send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    send_work_request.wr.atomic.compare_add = -1;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    exclusive = 0;
    shared = 0;
    uint64_t prev_value = ((uint64_t)user_id) << 32 | shared;
    send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
    send_work_request.wr.atomic.compare_add = prev_value;
    send_work_request.wr.atomic.swap        = (uint64_t)0ULL;
  }
  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  send_work_request.wr.atomic.rkey        =
    context->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "UnlockRemotely(): ibv_exp_post_send() failed: " << strerror(ret) <<
      endl;
    return -1;
  }

  return 0;
}

int LockClient::SendLockRequest(Context* context, int user_id,
    int lock_type, int obj_index) {

  pthread_mutex_lock(&lock_mutex_);
  context->send_message->type = Message::LOCK_REQUEST;
  context->send_message->lock_type = lock_type;
  context->send_message->obj_index = obj_index;
  context->send_message->user_id = user_id;

  if (SendMessage(context)) {
    cerr << "SendLockRequest(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  //cout << "SendLockRequest(): lock request sent." << endl;
  return 0;
}

int LockClient::SendUnlockRequest(Context* context, int user_id,
    int lock_type, int obj_index) {
  pthread_mutex_lock(&lock_mutex_);
  context->send_message->type = Message::UNLOCK_REQUEST;
  context->send_message->lock_type = lock_type;
  context->send_message->obj_index = obj_index;
  context->send_message->user_id = user_id;
  if (SendMessage(context)) {
    pthread_mutex_unlock(&lock_mutex_);
    cerr << "SendUnlockRequest(): SendMessage() failed." << endl;
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  //cout << "SendUnlockRequest(): memory region sent." << endl;
  return 0;
}

double LockClient::GetAverageRemoteSharedLockTime() const {
  return num_shared_lock_ > 0 ?
    total_shared_lock_remote_time_ / num_shared_lock_ : 0;
}

double LockClient::GetAverageRemoteExclusiveLockTime() const {
  return num_exclusive_lock_ > 0 ?
    total_exclusive_lock_remote_time_ / num_exclusive_lock_ : 0;
}

// Polls work completion from completion queue
void* LockClient::PollCompletionQueue(void* arg) {
  struct ibv_cq* cq;
  struct ibv_wc wc;
  Context* queue_context;
  Context* context = static_cast<Context*>(arg);

  while (true) {
    if (ibv_get_cq_event(context->completion_channel, &cq,
          (void**)&queue_context)) {
      cerr << "ibv_get_cq_event() failed." << endl;
    }
    ibv_ack_cq_events(cq, 1);
    if (ibv_req_notify_cq(cq, 0)) {
      cerr << "ibv_req_notify_cq() failed." << endl;
    }

    while (ibv_poll_cq(cq, 1, &wc)) {
      context->client->HandleWorkCompletion(&wc);
    }
  }

  return NULL;
}

}}
