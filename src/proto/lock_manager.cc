#include "lock_manager.h"

namespace rdma { namespace proto {

// constructor
LockManager::LockManager(const string& work_dir, uint32_t rank,
    int num_manager, int num_lock_object, int lock_mode) {
  work_dir_                        = work_dir;
  rank_                            = rank;
  num_manager_                     = num_manager;
  num_lock_object_                 = num_lock_object;
  lock_table_                      = new uint64_t[num_lock_object_];
  listener_                        = NULL;
  event_channel_                   = NULL;
  registered_memory_region_        = NULL;
  port_                            = 0;
  lock_mode_                       = lock_mode;
  total_local_exclusive_lock_time_ = 0;
  total_local_shared_lock_time_    = 0;
  num_local_exclusive_lock_        = 0;
  num_local_shared_lock_           = 0;

  // initialize lock table with 0
  memset(lock_table_, 0x00, num_lock_object_*sizeof(uint64_t));

  // initialize local lock mutex
  lock_mutex_ = new pthread_mutex_t*[num_lock_object_];
}

// destructor
LockManager::~LockManager() {
  if (lock_table_)
    delete[] lock_table_;

  // destroy mutex and free the resource
  for (int i=0;i<num_lock_object_;++i) {
    pthread_mutex_destroy(lock_mutex_[i]);
  }
}

int LockManager::GetID() const {
  return rank_;
}

int LockManager::Initialize() {

  for (int i=0;i<num_lock_object_;++i) {
    lock_mutex_[i] = new pthread_mutex_t;
    pthread_mutex_init(lock_mutex_[i], NULL);
  }
  memset(&address_, 0x00, sizeof(address_));
  address_.sin6_family = AF_INET6;

  event_channel_ = rdma_create_event_channel();
  if (event_channel_ == NULL) {
    cerr << "Run(): rdma_create_event_channel() failed: " <<
      strerror(errno) << endl;
    return -1;
  }
  if (rdma_create_id(event_channel_, &listener_, NULL, RDMA_PS_TCP)) {
    cerr << "Run(): rdma_create_id() failed: " << strerror(errno) << endl;
    return -1;
  }
  if (rdma_bind_addr(listener_, (struct sockaddr *)&address_)) {
    cerr << "Run(): rdma_bind_addr() failed: " << strerror(errno) << endl;
    return -1;
  }
  if (rdma_listen(listener_, 128)) {
    cerr << "Run(): rdma_listen() failed: " << strerror(errno) << endl;
    return -1;
  }
  port_ = ntohs(rdma_get_src_port(listener_));
  cout << "LockManager " << rank_ <<  " listening on port " << port_ << endl;

  // print ip,port of lock manager in the working directory
  if (PrintInfo()) {
    cerr << "PrintInfo() error." << endl;
    return -1;
  }
  return 0;
}

int LockManager::Run() {
  struct rdma_cm_event* event = NULL;
  while (rdma_get_cm_event(event_channel_, &event) == 0) {
    struct rdma_cm_event current_event;
    memcpy(&current_event, event, sizeof(current_event));
    rdma_ack_cm_event(event);
    if (HandleEvent(&current_event))
      break;
  }

  DestroyListener();
  return 0;
}

int LockManager::RegisterUser(int user_id, LockSimulator* user) {
  user_map[user_id] = user;
  pthread_mutex_t* mutex = new pthread_mutex_t;
  pthread_mutex_init(mutex, NULL);
  user_mutex_map[user_id] = mutex;
  users.push_back(user);
  return 0;
}

int LockManager::InitializeLockClients() {
  for (int i = 0; i < num_manager_; ++i) {
    for (int j = 0; j < users.size(); ++j) {
      LockSimulator* user = users[j];
      LockClient* client = new LockClient(work_dir_, this, user, i);
      pthread_t* client_thread = new pthread_t;

      if (pthread_create(client_thread, NULL,
            &LockManager::RunLockClient, (void*)client)) {
        cerr << "pthread_create() for LockClient failed." << endl;
        return -1;
      }

      lock_clients_[MAX_USER*i+user->GetID()] = client;
      //lock_clients_.push_back(client);
      lock_client_threads_.push_back(client_thread);

    }
  }
  return 0;
}

int LockManager::PrintInfo() {
  // write ip, port (infiniband) of lock manager in the work dir

  // open files
  char ip_filename[256];
  char port_filename[256];
  if (sprintf(ip_filename, "%s/lm%04d.ip", work_dir_.c_str(), rank_) < 0) {
    cerr << "PrintInfo(): sprintf() failed." << endl;
    return -1;
  }
  if (sprintf(port_filename, "%s/lm%04d.port", work_dir_.c_str(), rank_) < 0) {
    cerr << "PrintInfo(): sprintf() failed." << endl;
    return -1;
  }

  FILE* ip_file = fopen(ip_filename, "w");
  if (ip_file == NULL) {
    cerr << "PrintInfo(): fopen() failed: " << strerror(errno) << endl;
    return -1;
  }
  FILE* port_file = fopen(port_filename, "w");
  if (port_file == NULL) {
    cerr << "PrintInfo(): fopen() failed: " << strerror(errno) << endl;
    return -1;
  }

  string ip_address;
  if (GetInfinibandIP(ip_address)) {
    cerr << "Run(): failed to obtain infiniband ip address from interface ib0"
      << endl;
    return -1;
  }
  cout << "ip address: " << ip_address << endl;
  if (fprintf(ip_file, "%s\n", ip_address.c_str()) < 0) {
    cerr << "Run(): fprintf() error while writing ip." << endl;
    return -1;
  }
  if (fprintf(port_file, "%d\n", port_) < 0) {
    cerr << "Run(): fprintf() error while writing port." << endl;
    return -1;
  }

  fclose(ip_file);
  fclose(port_file);
}

int LockManager::GetInfinibandIP(string& ip_address) {
  struct ifaddrs *ifaddr, *ifa;
  int family, s, n;
  char host[NI_MAXHOST];

  if (getifaddrs(&ifaddr) == -1) {
    cerr << "getifaddrs() error: " << strerror(errno) << endl;
    return -1;
  }

  /* Walk through linked list, maintaining head pointer so we
   *               can free list later */

  bool ip_found = false;
  for (ifa = ifaddr, n = 0; ifa != NULL; ifa = ifa->ifa_next, n++) {
    if (ifa->ifa_addr == NULL)
      continue;

    if (strncmp(ifa->ifa_name, "ib0", 3) == 0 &&
        ifa->ifa_addr->sa_family == AF_INET) {

      s = getnameinfo(ifa->ifa_addr,
          sizeof(*ifa->ifa_addr),
          host, NI_MAXHOST,
          NULL, 0, NI_NUMERICHOST);
      if (s != 0) {
        cerr << "getnameinfo() failed: " << gai_strerror(s) << endl;
        return -1;
      }

      ip_found = true;
      ip_address = host;
    }
  }

  if (ip_found) {
    return 0;
  } else {
    cerr << "Infiniband ip address not found." << endl;
    return -1;
  }
}

void LockManager::DestroyListener() {
  if (listener_)
    rdma_destroy_id(listener_);
  if (event_channel_)
    rdma_destroy_event_channel(event_channel_);
}

void LockManager::Stop() {
  DestroyListener();
  exit(0);
}

// Currently the server registers same memory region for each client.
int LockManager::RegisterMemoryRegion(Context* context) {

  context->send_message = new Message;
  context->receive_message = new Message;

  context->lock_table = lock_table_;

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
  context->lock_table_mr = ibv_reg_mr(context->protection_domain,
      context->lock_table,
      num_lock_object_*sizeof(uint64_t),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  if (context->lock_table_mr == NULL) {
    cerr << "ibv_reg_mr() failed for lock_table_mr." << endl;
    return -1;
  }

  return 0;
}

int LockManager::HandleConnectRequest(struct rdma_cm_id* id) {
  Context* context = BuildContext(id);
  if (context == NULL) {
    cerr << "LockManager: BuildContext() failed." << endl;
    return -1;
  }
  struct ibv_exp_qp_init_attr queue_pair_attributes;
  BuildQueuePairAttr(context, &queue_pair_attributes);

  //if (rdma_create_qp(id, context->protection_domain, &queue_pair_attributes)){
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
  id->context = context;
  context->queue_pair = id->qp;

  // create memory regions for the connection
  if (RegisterMemoryRegion(context)) {
    cerr << "RegisterMemoryRegion() failed." << endl;
    return -1;
  }

  // post receive to handle incoming requests from client
  if (ReceiveMessage(context)) {
    cerr << "ReceiveMessage() failed." << endl;
    return -1;
  }

  // set rdma connection parameters
  struct rdma_conn_param connection_parameters;
  memset(&connection_parameters, 0x00, sizeof(connection_parameters));
  connection_parameters.initiator_depth =
    connection_parameters.responder_resources = 5;
  connection_parameters.rnr_retry_count = 5;

  // accept connection
  if (rdma_accept(id, &connection_parameters)) {
    cerr << "rdma_accept() failed: " << strerror(errno) << endl;
    return -1;
  }

  return 0;
}

int LockManager::HandleConnection(Context* context) {
  //cout << "Client connected." << endl;
  context->connected = true;

  return 0;
}

int LockManager::HandleDisconnect(Context* context) {
  // rdma_destroy_qp() causes seg fault when client disconnects. why?
  //rdma_destroy_qp(context->id);

  if (context->send_mr)
    ibv_dereg_mr(context->send_mr);
  if (context->receive_mr)
    ibv_dereg_mr(context->receive_mr);
  if (context->lock_table_mr)
    ibv_dereg_mr(context->lock_table_mr);

  delete context->send_message;
  delete context->receive_message;

  // rdma_destroy_id() also causes seg fault when client disconnects. why?
  //rdma_destroy_id(context->id);

  delete context;

  //cout << "client disconnected." << endl;
  return 0;
}

// Send local lock table memory region to client.
int LockManager::SendLockTableMemoryRegion(Context* context) {
  context->send_message->type = Message::LOCK_TABLE_MR;
  memcpy(&context->send_message->lock_table_mr, context->lock_table_mr,
      sizeof(context->send_message->lock_table_mr));
  if (SendMessage(context)) {
    cerr << "SendLockTableMemoryRegion(): SendMessage() failed." << endl;
    return -1;
  }

  //cout << "SendLockTableMemoryRegion(): memory region sent." << endl;
  return 0;
}

// Send lock request result to client.
int LockManager::SendLockRequestResult(Context* context, int user_id,
    int lock_type, int obj_index, int result) {
  context->send_message->type        = Message::LOCK_REQUEST_RESULT;
  context->send_message->user_id     = user_id;
  context->send_message->lock_type   = lock_type;
  context->send_message->obj_index   = obj_index;
  context->send_message->lock_result = result;
  if (SendMessage(context)) {
    cerr << "SendLockRequestResult(): SendMessage() failed." << endl;
    return -1;
  }

  //cout << "SendLockRequestResult(): memory region sent." << endl;
  return 0;
}

// Send unlock request result to client.
int LockManager::SendUnlockRequestResult(Context* context, int user_id,
    int lock_type, int obj_index, int result) {
  context->send_message->type        = Message::UNLOCK_REQUEST_RESULT;
  context->send_message->user_id     = user_id;
  context->send_message->lock_type   = lock_type;
  context->send_message->obj_index   = obj_index;
  context->send_message->lock_result = result;
  if (SendMessage(context)) {
    cerr << "SendUnlockRequestResult(): SendMessage() failed." << endl;
    return -1;
  }

  //cout << "SendUnlockRequestResult(): memory region sent." << endl;
  return 0;
}

int LockManager::Lock(int user_id, int manager_id, int lock_type,
    int obj_index) {
  LockClient* lock_client = lock_clients_[manager_id*MAX_USER+user_id];
  return lock_client->RequestLock(user_id, lock_type, obj_index, lock_mode_);

  //if (lock_mode_ == LockManager::LOCK_LOCAL && rank_ == manager_id) {
    //return LockLocally(lock_client->GetContext(), user_id, lock_type, obj_index);
  //} else {
    //return lock_client->RequestLock(user_id, lock_type, obj_index, lock_mode_);
  //}
}

int LockManager::Unlock(int user_id, int manager_id, int lock_type,
    int obj_index) {
  LockClient* lock_client = lock_clients_[manager_id*MAX_USER+user_id];
  return lock_client->RequestUnlock(user_id, lock_type, obj_index, lock_mode_);

  //if (lock_mode_ == LockManager::LOCK_LOCAL && rank_ == manager_id) {
    //return UnlockLocally(lock_client->GetContext(), user_id, lock_type, obj_index);
  //}
  //else {
    //return lock_client->RequestUnlock(user_id, lock_type, obj_index, lock_mode_);
  //}
}

int LockManager::LockLocally(Context* context) {

  // get time
  clock_gettime(CLOCK_MONOTONIC, &start_local_lock_);

  int lock_result = LockManager::RESULT_FAILURE;
  int user_id   = context->receive_message->user_id;
  int obj_index = context->receive_message->obj_index;
  int lock_type = context->receive_message->lock_type;
  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared = (uint32_t)(*lock_object);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  // if shared lock is requested
  if (lock_type == LockManager::SHARED) {
    if (exclusive == 0) {
      ++shared;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
      lock_result = LockManager::RESULT_FAILURE;
    }
  } else if (lock_type == LockManager::EXCLUSIVE) {
    // if exclusive lock is requested
    if (exclusive == 0 && shared == 0) {
      exclusive = context->receive_message->user_id;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
      lock_result = LockManager::RESULT_FAILURE;
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  // get time
  clock_gettime(CLOCK_MONOTONIC, &end_local_lock_);
  double time_taken = ((double)end_local_lock_.tv_sec * 1e+9 +
      (double)end_local_lock_.tv_nsec) -
    ((double)start_local_lock_.tv_sec * 1e+9 +
     (double)start_local_lock_.tv_nsec);

  if (lock_type == LockManager::SHARED) {
    total_local_shared_lock_time_ += time_taken;
    ++num_local_shared_lock_;
  } else if (lock_type == LockManager::EXCLUSIVE) {
    total_local_exclusive_lock_time_ += time_taken;
    ++num_local_exclusive_lock_;
  }

  // send result back to the client
  if (SendLockRequestResult(context, user_id, lock_type, obj_index,
        lock_result)) {
    cerr << "LockLocally() failed." << endl;
    return -1;
  }

  return 0;
}

int LockManager::LockLocalDirect(int user_id, int lock_type, int obj_index) {
  int lock_result = LockManager::RESULT_FAILURE;
  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared = (uint32_t)(*lock_object);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  // if shared lock is requested
  if (lock_type == LockManager::SHARED) {
    if (exclusive == 0) {
      ++shared;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
      lock_result = LockManager::RESULT_FAILURE;
    }
  } else if (lock_type == LockManager::EXCLUSIVE) {
    // if exclusive lock is requested
    if (exclusive == 0 && shared == 0) {
      exclusive = user_id;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
      lock_result = LockManager::RESULT_FAILURE;
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  return lock_result;
}

int LockManager::UnlockLocally(Context* context) {

  int lock_result;
  int user_id   = context->receive_message->user_id;
  int obj_index = context->receive_message->obj_index;
  int lock_type = context->receive_message->lock_type;
  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared = (uint32_t)(*lock_object);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  // unlocking shared lock
  if (lock_type == LockManager::SHARED) {
    if (shared > 0) {
      --shared;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
     //cerr << "client is trying to unlock shared lock with 0 counts." << endl;
      lock_result = LockManager::RESULT_FAILURE;
    }
  } else if (lock_type == LockManager::EXCLUSIVE) {// if exclusive lock is requested
    if (exclusive == user_id) {
      exclusive = 0;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
      //cerr << "client is trying to unlock exclusive lock," <<
        //" which it does not own." << endl;
      lock_result = LockManager::RESULT_FAILURE;
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  // send result back to the client
  if (SendUnlockRequestResult(context, user_id, lock_type, obj_index,
        lock_result)) {
    cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
    return -1;
  }

  return 0;
}

int LockManager::UnlockLocalDirect(int user_id, int lock_type, int obj_index) {
  int lock_result;
  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared = (uint32_t)(*lock_object);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  // unlocking shared lock
  if (lock_type == LockManager::SHARED) {
    if (shared > 0) {
      --shared;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
     //cerr << "client is trying to unlock shared lock with 0 counts." << endl;
      lock_result = LockManager::RESULT_FAILURE;
    }
  } else if (lock_type == LockManager::EXCLUSIVE) {// if exclusive lock is requested
    if (exclusive == user_id) {
      exclusive = 0;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
      //cerr << "client is trying to unlock exclusive lock," <<
        //" which it does not own." << endl;
      lock_result = LockManager::RESULT_FAILURE;
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  return lock_result;
}

int LockManager::NotifyLockRequestResult(int user_id, int lock_type,
    int obj_index, int result) {
  LockSimulator* user = user_map[user_id];
  //pthread_mutex_lock(user_mutex_map[user_id]);
  user->NotifyResult(LockManager::TASK_LOCK, lock_type, obj_index, result);
  //pthread_mutex_unlock(user_mutex_map[user_id]);
}

int LockManager::NotifyUnlockRequestResult(int user_id, int lock_type,
    int obj_index, int result) {
  LockSimulator* user = user_map[user_id];
  //pthread_mutex_lock(user_mutex_map[user_id]);
  user->NotifyResult(LockManager::TASK_UNLOCK, lock_type, obj_index, result);
  //pthread_mutex_unlock(user_mutex_map[user_id]);
}

int LockManager::SendMessage(Context* context) {
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
int LockManager::ReceiveMessage(Context* context) {
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

// Builds queue pair attributes
void LockManager::BuildQueuePairAttr(Context* context,
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
  attributes->max_atomic_arg   = sizeof(uint64_t);
  attributes->exp_create_flags = IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY;
}

Context* LockManager::BuildContext(struct rdma_cm_id* id) {
  // create new context for the connection
  Context* new_context = new Context;
  new_context->server = this;
  new_context->client = NULL;
  new_context->connected = false;
  new_context->device_context = id->verbs;
  if ((new_context->protection_domain =
        ibv_alloc_pd(new_context->device_context)) == NULL) {
    cerr << "LockManager: ibv_alloc_pd() failed." << endl;
    return NULL;
  }
  if ((new_context->completion_channel =
        ibv_create_comp_channel(new_context->device_context)) == NULL) {
    cerr << "LockManager: ibv_create_comp_channel() failed." << endl;
    return NULL;
  }
  if ((new_context->completion_queue =
        ibv_create_cq(new_context->device_context, 64,
          NULL, new_context->completion_channel, 0)) == NULL) {
    cerr << "LockManager: ibv_create_cq() failed." << endl;
    return NULL;
  }
  if (ibv_req_notify_cq(new_context->completion_queue, 0)) {
    cerr << "LockManager: ibv_req_notify_cq() failed." << endl;
    return NULL;
  }
  // create completion queue poller thread
  if (pthread_create(&new_context->cq_poller_thread, NULL,
        &LockManager::PollCompletionQueue, new_context)) {
    cerr << "LockManager: pthread_create() failed." << endl;
    return NULL;
  }

  return new_context;
}

// Handles RDMA connection manager events
int LockManager::HandleEvent(struct rdma_cm_event* event) {
  int ret = 0;
  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
    ret = HandleConnectRequest(event->id);
  } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
    ret = HandleConnection(static_cast<Context*>(event->id->context));
  } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
    ret = HandleDisconnect(static_cast<Context*>(event->id->context));
  } else {
    cerr << "Unknown event." << endl;
    Stop();
  }

  return ret;
}

// Handles work completions.
int LockManager::HandleWorkCompletion(struct ibv_wc* work_completion) {
  Context* context = (Context *)work_completion->wr_id;

  if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "Work completion status is not IBV_WC_SUCCESS." << endl;
    return -1;
  }

  if (work_completion->opcode & IBV_WC_RECV) {
    // Post receive first.
    ReceiveMessage(context);

    // if client is requesting semaphore MR
    if (context->receive_message->type == Message::LOCK_TABLE_MR_REQUEST) {
      SendLockTableMemoryRegion(context);
    } else if (context->receive_message->type == Message::LOCK_REQUEST) {
      LockLocally(context);
    } else if (context->receive_message->type == Message::UNLOCK_REQUEST) {
      UnlockLocally(context);
    } else {
      cerr << "Unknown message type: " << context->receive_message->type
        << endl;
      return -1;
    }
  }
}

double LockManager::GetAverageLocalSharedLockTime() const {
  return (num_local_shared_lock_ > 0) ?
    total_local_shared_lock_time_ / num_local_shared_lock_ : 0;
}

double LockManager::GetAverageLocalExclusiveLockTime() const {
  return (num_local_exclusive_lock_ > 0) ?
    total_local_exclusive_lock_time_ / num_local_exclusive_lock_ : 0;
}

double LockManager::GetAverageRemoteExclusiveLockTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<int, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetAverageRemoteExclusiveLockTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageRemoteSharedLockTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<int, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetAverageRemoteSharedLockTime();
  }
  return total_time / num_clients;
}

// Polls work completion from completion queue
void* LockManager::PollCompletionQueue(void* arg) {
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
      context->server->HandleWorkCompletion(&wc);
    }
  }

  return NULL;
}

int LockManager::GetLockMode() const {
  return lock_mode_;
}

void* LockManager::RunLockClient(void* args) {
  LockClient* client = static_cast<LockClient*>(args);
  client->Run();
}


}} // end namespace
