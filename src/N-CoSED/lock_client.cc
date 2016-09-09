#include "lock_client.h"

namespace rdma { namespace n_cosed {

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
  total_send_message_time_          = 0;
  total_receive_message_time_       = 0;
  num_exclusive_lock_               = 0;
  num_shared_lock_                  = 0;
  num_send_message_                 = 0;
  num_receive_message_              = 0;
  previous_unlock_shared_running_   = false;

  // initialize local lock mutex
  pthread_mutex_init(&lock_mutex_, NULL);
  pthread_cond_init(&cond_, NULL);
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

  //cout << "connecting to server: " << server_name_ << ":" << server_port_
    //<< endl;
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

  //cout << "address resolved." << endl;

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

  //cout << "route resolved." << endl;

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

  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

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

  if (context->send_message->type == Message::SHARED_UNLOCK_RESULT) {
    if (LockManager::PRINT_DEBUG) {
      pthread_mutex_lock(&LockManager::print_mutex);
      cerr << "Sending SHARED_UNLOCK_RESULT!" << endl;
      pthread_mutex_unlock(&LockManager::print_mutex);
    }
  }

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

  clock_gettime(CLOCK_MONOTONIC, &start_receive_message_);

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

  clock_gettime(CLOCK_MONOTONIC, &end_receive_message_);
  double time_taken = ((double)end_receive_message_.tv_sec * 1e+9 +
      (double)end_receive_message_.tv_nsec) -
    ((double)start_receive_message_.tv_sec * 1e+9 +
     (double)start_receive_message_.tv_nsec);
  total_receive_message_time_ += time_taken;
  ++num_receive_message_;

  return 0;
}


// Register local memory regions for RDMA
int LockClient::RegisterMemoryRegion(Context* context) {

  context->send_message = new Message;
  context->receive_message = new Message;

  context->send_mr = ibv_reg_mr(context->protection_domain,
      context->send_message,
      sizeof(*(context->send_message)),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  if (context->send_mr == NULL) {
    cerr << "ibv_reg_mr() failed for send_mr." << endl;
    return -1;
  }
  context->receive_mr = ibv_reg_mr(context->protection_domain,
      context->receive_message,
      sizeof(*(context->receive_message)),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
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
  attributes->cap.max_send_sge = 16;
  attributes->cap.max_recv_sge = 16;
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
    //if (work_completion->opcode == IBV_WC_COMP_SWAP ||
        //work_completion->opcode == IBV_WC_FETCH_ADD) {
      //SendLockModeRequest(context);
      //local_manager_->NotifyLockRequestResult(context->last_user_id,
          //context->last_lock_type,
            //context->last_home_id,
          //context->last_obj_index,
          //LockManager::RESULT_FAILURE);
      //return 0;
    //}
    //else {
      pthread_mutex_lock(&LockManager::print_mutex);
      cerr << "(LockClient) Work completion status is not IBV_WC_SUCCESS: " <<
        local_manager_->GetID() << ":" << work_completion->status << "," <<
        work_completion->opcode << endl;
      pthread_mutex_unlock(&LockManager::print_mutex);
      return -1;
    //}
  }

  pthread_mutex_lock(&lock_mutex_);
  uint64_t compare_value      = context->last_compare_value;
  uint64_t new_value          = context->last_new_value;
  int last_home_id            = context->last_home_id;
  int last_user_id            = context->last_user_id;
  int last_lock_type          = context->last_lock_type;
  int last_obj_index          = context->last_obj_index;
  int last_lock_task          = context->last_lock_task;
  int last_shared_count       = context->last_shared_count;
  int last_shared_lock_holder = context->last_shared_lock_holder;
  int opcode                  = work_completion->opcode;
  pthread_mutex_unlock(&lock_mutex_);

  if (last_lock_task == LockManager::TASK_UNLOCK && last_lock_type == LockManager::SHARED &&
      previous_unlock_shared_running_) {
    pthread_mutex_lock(&lock_mutex_);
    previous_unlock_shared_running_ = false;
    pthread_cond_signal(&cond_);
    pthread_mutex_unlock(&lock_mutex_);
  }

  if (opcode == IBV_WC_RECV) {

    // post receive first.
    ReceiveMessage(context);

    if (context->receive_message->type == Message::EXCLUSIVE_TO_SHARED_LOCK_GRANT_ACK) {
      if (LockManager::PRINT_DEBUG) {
        cerr << "Exclusive-to-Shared Lock Grant Ack Received." << endl;
      }
      //local_manager_->NotifyUnlockRequestResult(
          //context->receive_message->user_id,
          //LockManager::EXCLUSIVE,
          //context->receive_message->home_id,
          //context->receive_message->obj_index,
          //LockManager::RESULT_SUCCESS);
    } else if (context->receive_message->type == Message::SHARED_UNLOCK_RESULT) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "Shared Unlock Result Received: " <<
          local_manager_->GetID() << ", " <<
          context->receive_message->home_id << ", " <<
          context->receive_message->obj_index << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      //local_manager_->NotifyUnlockRequestResult(local_manager_->GetID(),
          //LockManager::SHARED,
          //context->receive_message->home_id,
          //context->receive_message->obj_index,
          //context->receive_message->lock_result);
    } else if (context->receive_message->type == Message::EXCLUSIVE_TO_EXCLUSIVE_LOCK_GRANT_ACK) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "Exclusive-to-Exclusive Lock Grant Ack Received." << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      //local_manager_->ResetExclusiveToExclusive(
          //context->receive_message->home_id,
          //context->receive_message->obj_index
          //);
      local_manager_->NotifyUnlockRequestResult(
          context->receive_message->user_id,
          LockManager::EXCLUSIVE,
          context->receive_message->home_id,
          context->receive_message->obj_index,
          LockManager::RESULT_SUCCESS);
    } else if (context->receive_message->type == Message::SHARED_TO_EXCLUSIVE_LOCK_GRANT_ACK) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "Shared-to-Exclusive Lock Grant Ack Received." << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }

      // this is probably wrong..
      //local_manager_->ResetSharedToExclusive(
          //context->receive_message->home_id,
          //context->receive_message->obj_index
          //);
      //local_manager_->NotifyUnlockRequestResult(
          //context->receive_message->user_id,
          //LockManager::EXCLUSIVE,
          //context->receive_message->home_id,
          //context->receive_message->obj_index,
          //LockManager::RESULT_SUCCESS);
    // if received lock table MR info + current lock mode
    } else if (context->receive_message->type == Message::LOCK_TABLE_MR) {
      //cout << "received lock table MR." << endl;
      // copy server rdma semaphore region
      local_manager_->UpdateLockModeTable(
          context->receive_message->manager_id,
          context->receive_message->lock_mode
          );
      context->lock_table_mr = new ibv_mr;
      memcpy(context->lock_table_mr,
          &context->receive_message->lock_table_mr,
          sizeof(*context->lock_table_mr));
    } else if (context->receive_message->type == Message::LOCK_MODE) {
      local_manager_->UpdateLockModeTable(
          context->receive_message->manager_id,
          context->receive_message->lock_mode
          );
    } else if (context->receive_message->type == Message::LOCK_REQUEST_RESULT) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cout << "received lock request result." << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      //local_manager_->NotifyLockRequestResult(context->receive_message->user_id,
          //context->receive_message->lock_type,
          //context->receive_message->obj_index,
          //context->receive_message->lock_result);
    } else if (context->receive_message->type == Message::UNLOCK_REQUEST_RESULT) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cout << "received unlock request result" << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      local_manager_->NotifyUnlockRequestResult(
          context->receive_message->user_id,
          context->receive_message->lock_type,
          context->receive_message->home_id,
          context->receive_message->obj_index,
          context->receive_message->lock_result);
    }
  } else if (opcode == IBV_WC_SEND) {
    clock_gettime(CLOCK_MONOTONIC, &end_send_message_);
    double time_taken = ((double)end_send_message_.tv_sec * 1e+9 +
        (double)end_send_message_.tv_nsec) -
      ((double)start_send_message_.tv_sec * 1e+9 +
       (double)start_send_message_.tv_nsec);
    total_send_message_time_ += time_taken;
    ++num_send_message_;
  } else if (opcode == IBV_WC_COMP_SWAP) {
    // completion of compare-and-swap, i.e. remote exclusive locking
    //
    local_manager_->PrintFirstElem();

    if (LockManager::PRINT_DEBUG) {
      pthread_mutex_lock(&LockManager::print_mutex);
      cerr << local_manager_->GetID() << ", op code : COMP_SWAP" << endl;
      pthread_mutex_unlock(&LockManager::print_mutex);
    }

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
    uint32_t new_exclusive, new_shared;
    uint32_t compare_exclusive, compare_shared;
    uint32_t exclusive, shared;
    new_exclusive = (uint32_t)((new_value) >> 32);
    new_shared = (uint32_t)new_value;
    compare_exclusive = (uint32_t)((compare_value) >> 32);
    compare_shared = (uint32_t)compare_value;
    exclusive = (uint32_t)((value)>>32);
    shared = (uint32_t)value;

    if (new_value == 0 && exclusive == 0 && shared > 0 &&
        exclusive == compare_exclusive && shared == compare_shared) {
      // Unlocking shared lock (success)
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "Unlocking Shared Lock: (" << exclusive << "," << shared <<  ")"
          << " -> (" << new_exclusive << "," << new_shared << ")" << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      local_manager_->ResetByUnlock(
          last_home_id,
          last_obj_index,
          compare_shared
          );
      //local_manager_->SendSharedUnlockRequestResult(
          //last_user_id,
          //last_home_id,
          //last_obj_index,
          //LockManager::RESULT_SUCCESS
          //);
    } else if (new_value == 0 && exclusive == 0 && shared > 0 &&
        exclusive == compare_exclusive && shared != compare_shared) {
      // Unlocking shared lock (failure due to additional shared locks)
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "Unlocking Shared Lock Failed: (" << exclusive << "," << shared <<  ")"
          << " -> (" << new_exclusive << "," << new_shared << ")" << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      //local_manager_->SendSharedUnlockRequestResult(
          //last_user_id,
          //last_home_id,
          //last_obj_index,
          //LockManager::RESULT_SUCCESS
          //);
    } else if (last_lock_type == LockManager::TASK_LOCK &&
        new_exclusive > 0 && exclusive > 0 && shared > 0) {
      // exclusive lock failed -- should retry
        local_manager_->NotifyLockRequestResult(last_user_id,
            last_lock_type,
            last_home_id,
            last_obj_index,
            LockManager::RESULT_RETRY);
    } else if (last_lock_task == LockManager::TASK_LOCK) {
      if (exclusive == 0 && exclusive == compare_exclusive &&
          shared == 0 && shared == compare_shared) {
        // it should have been successful since exclusive and shared was 0
        local_manager_->NotifyLockRequestResult(last_user_id,
            last_lock_type,
            last_home_id,
            last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else if (exclusive != 0 &&
          exclusive == compare_exclusive && shared == 0) {
        // exclusive -> exclusive (success)
        local_manager_->SendExclusiveToExclusiveLockRequest(exclusive,
            last_home_id,
            last_user_id,
            last_obj_index);
      } else if (exclusive != compare_exclusive ||
          shared != compare_shared) {
        // exclusive -> exclusive (failure, retry)
        if (LockManager::PRINT_DEBUG) {
          pthread_mutex_lock(&LockManager::print_mutex);
          cerr << "Retrying Exclusive_1: Expected = (" << compare_exclusive <<
            "," << compare_shared << "), Actual = (" << exclusive <<
            "," << shared << "), Trying = (" << new_exclusive << "," << new_shared << ")"
            << endl;
          pthread_mutex_unlock(&LockManager::print_mutex);
        }
        this->LockRemotely(context,
            last_home_id,
            last_user_id,
            last_lock_type,
            last_obj_index,
            value);

        //local_manager_->NotifyLockRequestResult(context->last_user_id,
            //context->last_lock_type,
            //context->last_obj_index,
            //LockManager::RESULT_FAILURE);
      } else if (exclusive == 0 && shared > 0) {
        if (LockManager::PRINT_DEBUG) {
          pthread_mutex_lock(&LockManager::print_mutex);
          cerr << "should be shared -> exclusive: " << endl;
          pthread_mutex_unlock(&LockManager::print_mutex);
        }
        // shared -> exclusive
        if (shared == compare_shared) {
          // success
          this->SendSharedToExclusiveLockRequest(local_manager_->GetID(),
              last_home_id,
              last_user_id,
              last_obj_index, shared);
        } else if (shared != compare_shared) {
          // retry with new value
          if (LockManager::PRINT_DEBUG) {
            cerr << "Retrying Exclusive_2: Expected = (" << compare_exclusive <<
              "," << compare_shared << "), Actual = (" << exclusive <<
              "," << shared << "), Trying = (" << last_user_id << ",0)"
              << endl;
          }
          this->LockRemotely(context,
              last_home_id,
              last_user_id,
              last_lock_type,
              last_obj_index,
              value);
        } else {
          if (LockManager::PRINT_DEBUG) {
            pthread_mutex_lock(&LockManager::print_mutex);
            cerr << "hole 1" << endl;
            pthread_mutex_unlock(&LockManager::print_mutex);
          }
        }
      } else if (exclusive != compare_exclusive && shared != 0) {
        // exclusive -> exclusive (failure)
        local_manager_->NotifyLockRequestResult(last_user_id,
            last_lock_type,
            last_home_id,
            last_obj_index,
            LockManager::RESULT_FAILURE);
      } else {
      }
    } else if (context->last_lock_task == LockManager::TASK_UNLOCK) {
      if (value == compare_value) {
        int shared_count = 0;
        if (last_lock_type == LockManager::SHARED) {
          shared_count = compare_shared;
        }
        if (LockManager::PRINT_DEBUG) {
          pthread_mutex_lock(&LockManager::print_mutex);
          cout << "Unlock Succeeded: expected = " << compare_value <<
            ", actual = " << value << endl;
          pthread_mutex_unlock(&LockManager::print_mutex);
        }
        local_manager_->ResetByUnlock(
            last_home_id,
            last_obj_index,
            compare_shared
            );
        if (last_lock_type == LockManager::EXCLUSIVE) {
          local_manager_->NotifyUnlockRequestResult(last_user_id,
              last_lock_type,
              last_home_id,
              last_obj_index,
              LockManager::RESULT_SUCCESS, true);
          if (shared > 0) { // unlocking both exclusive and shared (exclusive -> shared case)
            if (last_shared_lock_holder == -1) {
              cerr << "ERROR: Last shared lock holder must not be -1." << endl;
              exit(-1);
            }
            local_manager_->SendSharedUnlockRequestResult(
                last_shared_lock_holder,
                last_home_id,
                last_obj_index,
                LockManager::RESULT_SUCCESS
                );
          }
        }
      } else {
        if (LockManager::PRINT_DEBUG) {
          pthread_mutex_lock(&LockManager::print_mutex);
          cout << "Unlock failed: expected = " << compare_value <<
            ", actual = " << value << endl;
          pthread_mutex_unlock(&LockManager::print_mutex);
        }
        //sleep(1);
        //if (context->last_lock_type == LockManager::EXCLUSIVE)
        //local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            //context->last_lock_type,
            //context->last_home_id,
            //context->last_obj_index,
            //LockManager::RESULT_FAILURE);
        if (last_lock_type == LockManager::SHARED && exclusive > 0) {
          //if (shared == 0) {
            //// shared -> exclusive case, unlock message needs to be relayed...
            //pthread_mutex_lock(&LockManager::print_mutex);
            //cout << "Relaying Shared Lock Release message: " << exclusive <<
              //", " <<  << endl;
            //pthread_mutex_unlock(&LockManager::print_mutex);
            //local_manager_->RelaySharedUnlockRequest(
                //exclusive,
                //last_home_id,
                //last_user_id,
                //last_obj_index
                //);
          //}
          //pthread_mutex_lock(&LockManager::print_mutex);
          //cout << "Retrying shared lock release." << endl;
          //pthread_mutex_unlock(&LockManager::print_mutex);
          local_manager_->SendSharedUnlockRequestResult(
              last_user_id,
              last_home_id,
              last_obj_index,
              LockManager::RESULT_SUCCESS
              );
        }

        // exclusive -> exclusive case..
        if (last_lock_type == LockManager::EXCLUSIVE && exclusive > 0 && shared == 0) {
          local_manager_->ResetByUnlock(
              last_home_id,
              last_obj_index
              );
        // trying to unlock  exclusive + shared, yet failed.
        // retry?
        } else if (exclusive > 0 && shared > 0) {
          local_manager_->BroadcastExclusiveToSharedLockGrant(
               last_home_id,
               last_obj_index
              );
        } else if (compare_exclusive == 0 && compare_shared > 0 &&
            exclusive > 0 && shared >= 0) {

        }
        else {
          //local_manager_->Unlock(context->last_user_id,
              //context->last_home_id,
              //context->last_lock_type,
              //context->last_obj_index);
        }
      }
      //if (exclusive == context->last_user_id && shared == 0) {
        //local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            //context->last_lock_type,
            //context->last_home_id,
            //context->last_obj_index,
            //LockManager::RESULT_SUCCESS);
      //} else if (exclusive == context->last_user_id && shared != 0) {
        //local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            //context->last_lock_type,
            //context->last_home_id,
            //context->last_obj_index,
            //LockManager::RESULT_RETRY);
      //} else {
        //local_manager_->NotifyUnlockRequestResult(context->last_user_id,
            //context->last_lock_type,
            //context->last_home_id,
            //context->last_obj_index,
            //LockManager::RESULT_FAILURE);
      //}
    }
  } else if (opcode == IBV_WC_FETCH_ADD) {
    // completion of fetch-and-add, i.e. remote shared locking
    local_manager_->PrintFirstElem();

    if (LockManager::PRINT_DEBUG) {
      pthread_mutex_lock(&LockManager::print_mutex);
      cerr << local_manager_->GetID() << ", op code : FETCH_ADD" << endl;
      pthread_mutex_unlock(&LockManager::print_mutex);
    }
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

    if (last_lock_task == LockManager::TASK_LOCK) {
      // it should have been successful since exclusive and shared was 0
      if (exclusive == 0) {
        local_manager_->NotifyLockRequestResult(last_user_id,
            last_lock_type,
            last_home_id,
            last_obj_index,
            LockManager::RESULT_SUCCESS);
      } else {
        // exclusive -> shared
        // ask local manager to send lock request to the owner of the exclusive lock
        local_manager_->SendExclusiveToSharedLockRequest(
            exclusive, // current_owner
            last_home_id, // home id
            last_user_id,
            last_obj_index);

        //local_manager_->NotifyLockRequestResult(context->last_user_id,
            //context->last_lock_type,
            //context->last_obj_index,
            //LockManager::RESULT_FAILURE);
      }
    } else if (last_lock_task == LockManager::TASK_UNLOCK) {
      // this should not happen with N-CoSED...
      // unlock always succeeds. (really?)
      cerr << "This should not happen!" << endl;
      //local_manager_->NotifyUnlockRequestResult(context->last_user_id,
          //context->last_lock_type,
          //context->last_home_id,
          //context->last_obj_index,
          //LockManager::RESULT_SUCCESS);
    }
  }

  return 0;
}

// Requests lock mode of lock manager via IBV_WR_SEND op.
int LockClient::SendLockModeRequest(Context* context) {

  pthread_mutex_lock(&lock_mutex_);

  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

  context->send_message->type       = Message::LOCK_MODE_REQUEST;
  context->send_message->manager_id = local_manager_->GetID();
  context->send_message->user_id    = local_user_->GetID();

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
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }
  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int LockClient::SendSharedToExclusiveLockRequest(int new_owner,
    int home_id, int user_id,
    int obj_index, int shared_count) {

  pthread_mutex_lock(&lock_mutex_);

  context_->send_message->type                = Message::SHARED_TO_EXCLUSIVE_LOCK_REQUEST;
  context_->send_message->new_owner           = new_owner;
  context_->send_message->home_id             = home_id;
  context_->send_message->user_id             = user_id;
  context_->send_message->obj_index           = obj_index;
  context_->send_message->shared_lock_counter = shared_count;

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context_;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context_->send_message;
  sge.length = sizeof(*context_->send_message);
  sge.lkey   = context_->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context_->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int LockClient::SendExclusiveToExclusiveLockRequest(int new_owner,
    int home_id, int user_id,
    int obj_index) {

  pthread_mutex_lock(&lock_mutex_);
  context_->send_message->type                = Message::EXCLUSIVE_TO_EXCLUSIVE_LOCK_REQUEST;
  context_->send_message->new_owner           = new_owner;
  context_->send_message->home_id             = home_id;
  context_->send_message->user_id             = user_id;
  context_->send_message->obj_index           = obj_index;

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context_;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context_->send_message;
  sge.length = sizeof(*context_->send_message);
  sge.lkey   = context_->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context_->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int LockClient::SendSharedToExclusiveLockGrant(int home_id, int user_id,
    int obj_index) {

  pthread_mutex_lock(&lock_mutex_);
  context_->send_message->type       = Message::SHARED_TO_EXCLUSIVE_LOCK_GRANT;
  context_->send_message->manager_id = home_id;
  context_->send_message->home_id    = home_id;
  context_->send_message->user_id    = user_id;
  context_->send_message->obj_index  = obj_index;
  context_->send_message->lock_type  = LockManager::EXCLUSIVE;

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Sending Shared-To-Exclusive Lock Grant: " << home_id
      << ", " << user_id << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context_;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context_->send_message;
  sge.length = sizeof(*context_->send_message);
  sge.lkey   = context_->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context_->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int LockClient::SendExclusiveToExclusiveLockGrant(int home_id, int prev_user_id,
    int user_id,
    int obj_index) {

  if (prev_user_id == user_id) {
     cerr << "WRONG" << endl;
  }

  pthread_mutex_lock(&lock_mutex_);
  context_->send_message->type         = Message::EXCLUSIVE_TO_EXCLUSIVE_LOCK_GRANT;
  context_->send_message->home_id      = home_id;
  context_->send_message->user_id      = user_id;
  context_->send_message->prev_user_id = prev_user_id;
  context_->send_message->obj_index    = obj_index;
  context_->send_message->lock_type    = LockManager::EXCLUSIVE;

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context_;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context_->send_message;
  sge.length = sizeof(*context_->send_message);
  sge.lkey   = context_->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context_->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int LockClient::SendSharedUnlockRequestResult(int home_id, int obj_index, int result) {
  pthread_mutex_lock(&lock_mutex_);
  context_->send_message->type        = Message::SHARED_UNLOCK_RESULT;
  context_->send_message->home_id     = home_id;
  context_->send_message->obj_index   = obj_index;
  context_->send_message->lock_result = result;

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;


  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context_;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context_->send_message;
  sge.length = sizeof(*context_->send_message);
  sge.lkey   = context_->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context_->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int LockClient::SendExclusiveToSharedLockRequest(int current_owner, int new_owner,
    int home_id, int user_id, int obj_index) {

  pthread_mutex_lock(&lock_mutex_);
  context_->send_message->type          = Message::EXCLUSIVE_TO_SHARED_LOCK_REQUEST;
  context_->send_message->current_owner = current_owner;
  context_->send_message->new_owner     = new_owner;
  context_->send_message->manager_id    = new_owner;
  context_->send_message->home_id       = home_id;
  context_->send_message->user_id       = user_id;
  context_->send_message->obj_index     = obj_index;

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context_;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context_->send_message;
  sge.length = sizeof(*context_->send_message);
  sge.lkey   = context_->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context_->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

int LockClient::SendExclusiveToSharedLockGrant(int current_owner, int new_owner,
    int home_id, int user_id, int obj_index) {

  pthread_mutex_lock(&lock_mutex_);
  context_->send_message->type          = Message::EXCLUSIVE_TO_SHARED_LOCK_GRANT;
  context_->send_message->manager_id    = home_id;
  context_->send_message->current_owner = current_owner;
  context_->send_message->new_owner     = new_owner;
  context_->send_message->home_id       = home_id;
  context_->send_message->user_id       = user_id;
  context_->send_message->obj_index     = obj_index;

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id      = (uint64_t)context_;
  send_work_request.opcode     = IBV_WR_SEND;
  send_work_request.sg_list    = &sge;
  send_work_request.num_sge    = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  sge.addr   = (uint64_t)context_->send_message;
  sge.length = sizeof(*context_->send_message);
  sge.lkey   = context_->send_mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context_->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  return 0;
}

// Requests lock table MR region of from lock manager via IBV_WR_SEND op.
int LockClient::SendLockTableRequest(Context* context) {

  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

  pthread_mutex_lock(&lock_mutex_);
  context->send_message->type       = Message::LOCK_TABLE_MR_REQUEST;
  context->send_message->manager_id = local_manager_->GetID();
  if (local_user_)
    context->send_message->user_id    = local_user_->GetID();

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
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  //cout << "requested lock table MR" << endl;
  pthread_mutex_unlock(&lock_mutex_);

  return 0;
}

int LockClient::RequestLock(int home_id, int user_id, int lock_type, int obj_index,
    int lock_mode, uint64_t old_value) {
  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "RequestLock: " << home_id << ", " << user_id << ", " << obj_index << ", " << lock_type
      << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }
  return this->LockRemotely(context_, home_id, user_id, lock_type, obj_index, old_value);
  //if (lock_mode == LockManager::LOCK_LOCAL) {
    //// ask lock manager to place the lock
    //return this->SendLockRequest(context_, user_id, lock_type, obj_index);
  //} else if (lock_mode == LockManager::LOCK_REMOTE) {
    //// try locking remotely
    //return this->LockRemotely(context_, user_id, lock_type, obj_index);
  //} else {
    //cerr << "RequestLock(): Unknown lock mode: " << lock_mode << endl;
  //}
}

int LockClient::RequestUnlock(int home_id, int user_id, int lock_type, int obj_index,
    int lock_mode, uint64_t old_value, int last_user_id) {
  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "RequestUnlock: " << home_id << ", " << user_id << ", " << obj_index << ", " << lock_type
      << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }
  if (lock_type == LockManager::SHARED) {
    return this->SendUnlockRequest(context_, home_id, user_id, lock_type, obj_index);
  } else if (lock_type == LockManager::EXCLUSIVE) {
    return this->UnlockRemotely(context_, home_id, user_id, lock_type, obj_index,
        old_value, last_user_id);
  }
  //if (lock_mode == LockManager::LOCK_LOCAL) {
    //return this->SendUnlockRequest(context_, user_id, lock_type, obj_index);
  //} else if (lock_mode == LockManager::LOCK_REMOTE) {
    //return this->UnlockRemotely(context_, user_id, lock_type, obj_index);
  //} else {
    //cerr << "RequestUnlock(): Unknown lock mode: " << lock_mode << endl;
  //}
}

int LockClient::UnlockShared(int home_id, int user_id, int obj_index, int count) {

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "UnlockShared: " << count << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  pthread_mutex_lock(&lock_mutex_);

  uint32_t old_exclusive, old_shared;
  uint32_t new_exclusive, new_shared;

  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  //while (previous_unlock_shared_running_) {
     //// busy-wait
     //pthread_cond_wait(&cond_, &lock_mutex_);
  //}

  context_->last_lock_type    = LockManager::SHARED;
  context_->last_home_id      = home_id;
  context_->last_user_id      = user_id;
  context_->last_obj_index    = obj_index;
  context_->last_shared_count = count;
  context_->last_lock_task    = LockManager::TASK_UNLOCK;

  //previous_unlock_shared_running_ = true;

  sge.addr   = (uint64_t)context_->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = context_->original_value_mr->lkey;

  send_work_request.wr_id          = (uint64_t)context_;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED | IBV_SEND_FENCE;

  old_exclusive = 0;
  old_shared = count;
  uint64_t old_value = ((uint64_t)old_exclusive) << 32 | old_shared;
  uint64_t new_value = (uint64_t)0ULL;

  context_->last_compare_value = old_value;
  context_->last_new_value     = 0;

  send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
  send_work_request.wr.atomic.compare_add = old_value;
  send_work_request.wr.atomic.swap        = new_value;

  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context_->lock_table_mr->addr + (obj_index*sizeof(uint64_t));
  send_work_request.wr.atomic.rkey        =
    context_->lock_table_mr->rkey;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context_->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "UnlockShared(): ibv_exp_post_send() failed: " << strerror(ret) <<
      endl;
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&lock_mutex_);
  return 0;

}

int LockClient::LockRemotely(Context* context, int home_id,
    int user_id, int lock_type,
    int obj_index, uint64_t old_value) {

  if (lock_type == LockManager::SHARED) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_shared_lock_);
  } else if (lock_type == LockManager::EXCLUSIVE) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_exclusive_lock_);
  }

  pthread_mutex_lock(&lock_mutex_);

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  context->last_home_id       = home_id;
  context->last_user_id       = user_id;
  context->last_lock_type     = lock_type;
  context->last_obj_index     = obj_index;
  context->last_lock_task     = LockManager::TASK_LOCK;
  context->last_compare_value = old_value;
  context->last_new_value     = 0;

  sge.addr   = (uint64_t)context->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = context->original_value_mr->lkey;

  send_work_request.wr_id          = (uint64_t)context;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED | IBV_SEND_FENCE;

  if (lock_type == LockManager::SHARED) {
    send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    send_work_request.wr.atomic.compare_add = 1;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    exclusive = user_id;
    shared = 0;
    uint64_t new_value = ((uint64_t)exclusive) << 32 | shared;
    context->last_new_value = new_value;
    send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_CMP_AND_SWP;
    send_work_request.wr.atomic.compare_add = old_value;
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
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }
  pthread_mutex_unlock(&lock_mutex_);

  return 0;
}


int LockClient::UnlockRemotely(Context* context, int home_id, int user_id, int lock_type,
    int obj_index, uint64_t old_value, int last_user_id) {

  if (lock_type == LockManager::SHARED) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_shared_lock_);
  } else if (lock_type == LockManager::EXCLUSIVE) {
    clock_gettime(CLOCK_MONOTONIC, &start_remote_exclusive_lock_);
  }

  pthread_mutex_lock(&lock_mutex_);

  uint32_t exclusive, shared;
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  context->last_home_id            = home_id;
  context->last_user_id            = user_id;
  context->last_lock_type          = lock_type;
  context->last_obj_index          = obj_index;
  context->last_lock_task          = LockManager::TASK_UNLOCK;
  context->last_new_value          = 0;
  context->last_shared_lock_holder = last_user_id;

  sge.addr   = (uint64_t)context->original_value;
  sge.length = sizeof(uint64_t);
  sge.lkey   = context->original_value_mr->lkey;

  send_work_request.wr_id          = (uint64_t)context;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_EXP_SEND_SIGNALED;

  if (lock_type == LockManager::SHARED) {
    send_work_request.exp_opcode            = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
    send_work_request.wr.atomic.compare_add = -1;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    exclusive = 0;
    shared = 0;
    uint64_t prev_value = (old_value == 0) ?
      ((uint64_t)user_id) << 32 | shared : old_value;
    context->last_compare_value = prev_value;
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
    pthread_mutex_unlock(&lock_mutex_);
    return -1;
  }
  pthread_mutex_unlock(&lock_mutex_);

  return 0;
}

//int LockClient::SendSwitchToLocal() {
  //pthread_mutex_lock(&lock_mutex_);
  //context_->send_message->type = Message::SWITCH_TO_LOCAL;
  //context_->send_message->manager_id = local_manager_->GetID();

  //if (SendMessage(context_)) {
    //cerr << "SendSwitchToLocal(): SendMessage() failed." << endl;
    //pthread_mutex_unlock(&lock_mutex_);
    //return -1;
  //}

  //pthread_mutex_unlock(&lock_mutex_);
//}

//int LockClient::SendSwitchToRemote() {
  //pthread_mutex_lock(&lock_mutex_);
  //context_->send_message->type = Message::SWITCH_TO_REMOTE;
  //context_->send_message->manager_id = local_manager_->GetID();

  //if (SendMessage(context_)) {
    //cerr << "SendSwitchToRemote(): SendMessage() failed." << endl;
    //pthread_mutex_unlock(&lock_mutex_);
    //return -1;
  //}

  //pthread_mutex_unlock(&lock_mutex_);
//}

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

int LockClient::SendUnlockRequest(Context* context, int home_id, int user_id,
    int lock_type, int obj_index) {
  pthread_mutex_lock(&lock_mutex_);
  //context->send_message->home_id   = local_manager_->GetID();
  context->send_message->home_id   = home_id;
  context->send_message->type      = Message::UNLOCK_REQUEST;
  context->send_message->lock_type = lock_type;
  context->send_message->obj_index = obj_index;
  context->send_message->user_id   = user_id;

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Sending Shared Unlock Request: " << user_id <<
      ", " << home_id << ", " << obj_index << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }
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

double LockClient::GetAverageSendMessageTime() const {
  return num_send_message_ > 0 ?
    total_send_message_time_ / num_send_message_ : 0;
}

double LockClient::GetAverageReceiveMessageTime() const {
  return num_receive_message_ > 0 ?
    total_receive_message_time_ / num_receive_message_ : 0;
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
