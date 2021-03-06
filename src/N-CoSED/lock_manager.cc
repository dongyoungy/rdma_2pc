#include "lock_manager.h"

namespace rdma { namespace n_cosed {

pthread_mutex_t LockManager::print_mutex = PTHREAD_MUTEX_INITIALIZER;

// constructor
LockManager::LockManager(const string& work_dir, uint32_t rank,
    int num_manager, int num_lock_object, int lock_mode) {
  work_dir_                              = work_dir;
  rank_                                  = rank;
  num_manager_                           = num_manager;
  num_lock_object_                       = num_lock_object;
  lock_table_                            = new uint64_t[num_lock_object_];
  lock_mode_table_                       = new int[num_manager_+1];
  shared_lock_counter_                   = new int[num_lock_object_*(num_manager_+1)];
  exclusive_to_shared_unlock_to_receive_ = new int[num_lock_object_*(num_manager_+1)];
  shared_to_exclusive_unlock_to_receive_ = new int[num_lock_object_*(num_manager_+1)];
  shared_to_exclusive_home_id_           = new int[num_lock_object_*(num_manager_+1)];
  shared_to_exclusive_user_id_           = new int[num_lock_object_*(num_manager_+1)];
  exclusive_to_exclusive_home_id_        = new int[num_lock_object_*(num_manager_+1)];
  exclusive_to_exclusive_user_id_        = new int[num_lock_object_*(num_manager_+1)];
  //node_to_release_shared_lock_         = new int[num_lock_object_*(num_manager_+1)];
  node_to_send_shared_release_           = new int[num_lock_object_*(num_manager_+1)];
  node_to_unlock_exclusive_shared_       = new int[num_lock_object_*(num_manager_+1)];
  exclusive_lock_holders_                = new int[num_lock_object_*(num_manager_+1)];
  has_unlocked_exclusive_                = new int[num_lock_object_*(num_manager_+1)];
  has_unlocked_shared_                   = new bool[num_lock_object_*(num_manager_+1)];
  is_unlocking_shared_                   = new bool[num_lock_object_*(num_manager_+1)];
  listener_                              = NULL;
  event_channel_                         = NULL;
  registered_memory_region_              = NULL;
  port_                                  = 0;
  lock_mode_                             = LOCK_REMOTE;
  current_lock_mode_                     = LOCK_REMOTE;
  total_local_exclusive_lock_time_       = 0;
  total_local_shared_lock_time_          = 0;
  num_local_exclusive_lock_              = 0;
  num_local_shared_lock_                 = 0;
  num_local_lock_                        = 0;
  num_remote_lock_                       = 0;

  // always LOCK_REMOTE
  //if (lock_mode_ == LOCK_ADAPTIVE) {
    //current_lock_mode_ = LOCK_REMOTE;
  //} else {
    //current_lock_mode_ = lock_mode_;
  //}

  // initialize lock table with 0
  memset(lock_table_, 0x00, num_lock_object_*sizeof(uint64_t));
  memset(shared_lock_counter_, 0x00, num_lock_object_*(num_manager_+1)*sizeof(int));
  memset(exclusive_to_shared_unlock_to_receive_, 0x00, num_lock_object_*(num_manager_+1)*sizeof(int));
  memset(shared_to_exclusive_unlock_to_receive_, 0x00, num_lock_object_*(num_manager_+1)*sizeof(int));
  memset(shared_to_exclusive_user_id_, 0x00, num_lock_object_*(num_manager_+1)*sizeof(int));
  memset(exclusive_to_exclusive_user_id_, 0x00, num_lock_object_*(num_manager_+1)*sizeof(int));
  memset(exclusive_lock_holders_, 0x00, num_lock_object_*(num_manager_+1)*sizeof(int));
  memset(node_to_send_shared_release_, 0x00, num_lock_object_*(num_manager_+1)*sizeof(int));
  memset(node_to_unlock_exclusive_shared_, 0x00, num_lock_object_*(num_manager_+1)*sizeof(int));
  for (int i = 0; i < num_lock_object_*(num_manager_+1); ++i) {
    //node_to_release_shared_lock_[i] = -1;
    shared_to_exclusive_home_id_[i] = -1;
    exclusive_to_exclusive_home_id_[i] = -1;
    has_unlocked_exclusive_[i] = 0;
    has_unlocked_shared_[i] = false;
    is_unlocking_shared_[i] = false;
  }

  lock_mode_table_[rank_] = current_lock_mode_;

  //for (int i=0;i<num_manager_;++i) {
    //// every lock manager starts in remote mode.
    //lock_mode_table_[i] = LockManager::LOCK_REMOTE;
  //}

  // initialize local lock mutex
  lock_mutex_ = new pthread_mutex_t*[num_lock_object_];
  pthread_mutex_init(&mutex_, NULL);
  pthread_mutex_init(&wc_mutex_, NULL);
  pthread_mutex_init(&user_mutex_, NULL);
  pthread_cond_init(&cond_, NULL);
}

// destructor
LockManager::~LockManager() {
  if (lock_table_)
    delete[] lock_table_;

  if (lock_mode_table_)
    delete[] lock_mode_table_;

  if (shared_lock_counter_)
    delete[] shared_lock_counter_;

  if (exclusive_to_shared_unlock_to_receive_)
    delete[] exclusive_to_shared_unlock_to_receive_;

  if (shared_to_exclusive_unlock_to_receive_)
    delete[] shared_to_exclusive_unlock_to_receive_;

  if (node_to_send_shared_release_)
    delete[] node_to_send_shared_release_;

  if (node_to_unlock_exclusive_shared_)
    delete[] node_to_unlock_exclusive_shared_;

  if (shared_to_exclusive_home_id_)
    delete[] shared_to_exclusive_home_id_;

  if (shared_to_exclusive_user_id_)
    delete[] shared_to_exclusive_user_id_;

  if (exclusive_to_exclusive_home_id_)
    delete[] exclusive_to_exclusive_home_id_;

  if (exclusive_to_exclusive_user_id_)
    delete[] exclusive_to_exclusive_user_id_;

  if (exclusive_lock_holders_)
    delete[] exclusive_lock_holders_;

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
  //cout << "LockManager " << rank_ <<  " listening on port " << port_ << endl;

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
    LockSimulator* user = users[0];
    LockClient* client = new LockClient(work_dir_, this, user, i+1);
    pthread_t* client_thread = new pthread_t;

    if (pthread_create(client_thread, NULL,
          &LockManager::RunLockClient, (void*)client)) {
      cerr << "pthread_create() for LockClient failed." << endl;
      return -1;
    }

    lock_clients_[i+1] = client;
    if (i == rank_) {
      local_client_id_ = user->GetID();
    }
    //lock_clients_.push_back(client);
    lock_client_threads_.push_back(client_thread);
  }

  local_client_ = new LockClient(work_dir_, this, NULL, rank_);
  pthread_t local_client_thread;
  if (pthread_create(&local_client_thread, NULL, &LockManager::RunLockClient,
        (void*)local_client_)) {
    cerr << "pthread_create() for dedicated LockClient failed." << endl;
    return -1;
  }

  // let's create a dedicated lock client for communication.
  for (int i = 1; i <= num_manager_; ++i) {
    //LockClient* client = new LockClient(work_dir_, this, NULL, rank_);
    //pthread_t local_client_thread;

    //if (i == rank_) {
      //local_client_ = client;
    //}

    //if (pthread_create(&local_client_thread, NULL, &LockManager::RunLockClient,
          //(void*)client)) {
      //cerr << "pthread_create() for dedicated LockClient failed." << endl;
      //return -1;
    //}

    //communication_clients_[i] = client;
  }

  for (int i=0; i < num_lock_object_; ++i) {
    shared_lock_owner_waitlist_[i] = new queue<int>;
    shared_lock_user_id_waitlist_[i] = new queue<int>;
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
  //cout << "ip address: " << ip_address << endl;
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
  context_set_.insert(context);
  // if lock mode == local (i.e., proxy, we disable atomic operations)
  if (current_lock_mode_ == LOCK_LOCAL) {
    struct ibv_qp_attr attr;
    memset(&attr, 0x00, sizeof(attr));
    attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    if (ibv_modify_qp(context->queue_pair, &attr, IBV_QP_ACCESS_FLAGS)) {
      cerr << "ibv_modify_qp() failed." << endl;
      return -1;
    }
  }

  struct ibv_qp_attr attr;
  struct ibv_qp_init_attr init_attr;

  if (ibv_query_qp(context->queue_pair, &attr,
        IBV_QP_ACCESS_FLAGS, &init_attr)) {
    fprintf(stderr, "Failed to query QP state\n");
    return -1;
  }
  return 0;
}

int LockManager::UpdateLockModeTable(int manager_id, int mode) {
  lock_mode_table_[manager_id] = mode;
  return 0;
}

int LockManager::DisableRemoteAtomicAccess() {
  struct ibv_qp_attr attr;
  memset(&attr, 0x00, sizeof(attr));

  attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

  for (set<Context*>::iterator it = context_set_.begin();
      it != context_set_.end(); ++it) {
    Context* context = *it;
    if (ibv_modify_qp(context->queue_pair, &attr, IBV_QP_ACCESS_FLAGS)) {
      cerr << "ibv_modify_qp() failed." << endl;
      return -1;
    }
  }
  return 0;
}

int LockManager::EnableRemoteAtomicAccess() {
  struct ibv_qp_attr attr;
  memset(&attr, 0x00, sizeof(attr));

  attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
    IBV_ACCESS_REMOTE_ATOMIC;

  for (set<Context*>::iterator it = context_set_.begin();
      it != context_set_.end(); ++it) {
    Context* context = *it;
    if (ibv_modify_qp(context->queue_pair, &attr, IBV_QP_ACCESS_FLAGS)) {
      cerr << "ibv_modify_qp() failed." << endl;
      return -1;
    }
  }
  return 0;
}

int LockManager::HandleDisconnect(Context* context) {
  // rdma_destroy_qp() causes seg fault when client disconnects. why?
  //rdma_destroy_qp(context->id);

  context_set_.erase(context);

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

// Send local lock table memory region + current lock mode to client.
int LockManager::SendLockTableMemoryRegion(Context* context) {
  context->send_message->type       = Message::LOCK_TABLE_MR;
  context->send_message->lock_mode  = current_lock_mode_;
  context->send_message->manager_id = rank_;

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
  context->send_message->home_id     = rank_;
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

int LockManager::SendExclusiveToSharedLockGrantAck(Context* context, int user_id,
    int home_id, int obj_index) {
  context->send_message->type        = Message::EXCLUSIVE_TO_SHARED_LOCK_GRANT_ACK;
  context->send_message->home_id     = home_id;
  context->send_message->user_id     = user_id;
  context->send_message->lock_type   = LockManager::SHARED;
  context->send_message->obj_index   = obj_index;
  if (SendMessage(context)) {
    cerr << "SendSharedLockGrantAck(): SendMessage() failed." << endl;
    return -1;
  }

  //cout << "SendUnlockRequestResult(): memory region sent." << endl;
  return 0;
}

int LockManager::SendSharedToExclusiveLockGrantAck(Context* context,
    int home_id, int user_id,
    int obj_index) {
  context->send_message->type        = Message::SHARED_TO_EXCLUSIVE_LOCK_GRANT_ACK;
  context->send_message->home_id     = home_id;
  context->send_message->user_id     = user_id;
  context->send_message->obj_index   = obj_index;
  if (SendMessage(context)) {
    cerr << "SendSharedToExclusiveLockGrantAck(): SendMessage() failed." << endl;
    return -1;
  }
  return 0;
}

int LockManager::SendExclusiveToExclusiveLockGrantAck(Context* context, int home_id,
    int user_id, int obj_index) {
  context->send_message->type        = Message::EXCLUSIVE_TO_EXCLUSIVE_LOCK_GRANT_ACK;
  context->send_message->home_id     = home_id;
  context->send_message->user_id     = user_id;
  context->send_message->obj_index   = obj_index;
  if (SendMessage(context)) {
    cerr << "SendExclusiveToExclusiveLockGrantAck(): SendMessage() failed." << endl;
    return -1;
  }
  return 0;
}


int LockManager::SendExclusiveToExclusiveLockRequest(int current_owner, int home_id,
    int user_id, int obj_index) {
  LockClient* client = lock_clients_[current_owner];
  return client->SendExclusiveToExclusiveLockRequest(rank_, home_id, user_id,
      obj_index);
}

int LockManager::SendExclusiveToSharedLockRequest(int current_owner, int home_id,
    int user_id, int obj_index) {
  LockClient* client = lock_clients_[current_owner];
  return client->SendExclusiveToSharedLockRequest(current_owner, rank_, home_id, user_id,
      obj_index);
}

int LockManager::SendSharedUnlockRequestResult(int node_id, int home_id, int obj_index,
    int result) {
  int index = num_lock_object_ * home_id + obj_index;

  pthread_mutex_lock(&mutex_);
  is_unlocking_shared_[index] = false;
  pthread_cond_signal(&cond_);
  pthread_mutex_unlock(&mutex_);
  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Sending Shared Unlock Result: from = " << rank_ <<
      ", to = " << node_id << ", " <<
      home_id << ", " <<
      obj_index << ", " << result << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }
  LockClient* client = lock_clients_[node_id];
  return client->SendSharedUnlockRequestResult(home_id, obj_index, result);
}

int LockManager::NotifyLockModeAll() {
  for (set<Context*>::iterator it = context_set_.begin();
      it != context_set_.end(); ++it) {
    Context* context = *it;
    NotifyLockMode(context);
  }
}

int LockManager::NotifyLockMode(Context* context) {
  context->send_message->type       = Message::LOCK_MODE;
  context->send_message->manager_id = rank_;
  context->send_message->lock_mode  = current_lock_mode_;
  if (SendMessage(context)) {
    cerr << "SendUnlockRequestResult(): SendMessage() failed." << endl;
    return -1;
  }
  return 0;
}

int LockManager::Lock(int user_id, int manager_id, int lock_type,
    int obj_index) {
  LockClient* lock_client = lock_clients_[manager_id];
  //if (lock_mode_ == LockManager::LOCK_ADAPTIVE) {
    //if (manager_id == this->GetID()) {
      //++num_local_lock_;
      //if (num_local_lock_ + num_remote_lock_ > NUM_LOCK_HISTORY) {
        //if (num_local_lock_ > NUM_LOCK_HISTORY) {
          //num_local_lock_ = NUM_LOCK_HISTORY;
        //} else {
          //--num_remote_lock_;
        //}
      //}
    //} else {
      //++num_remote_lock_;
      //if (num_local_lock_ + num_remote_lock_ > NUM_LOCK_HISTORY) {
        //if (num_remote_lock_ > NUM_LOCK_HISTORY) {
          //num_remote_lock_ = NUM_LOCK_HISTORY;
        //} else {
          //--num_local_lock_;
        //}
      //}
    //}

    //if ((double)num_local_lock_ / (double)(num_local_lock_ + num_remote_lock_) >=
        //ADAPT_THRESHOLD && current_lock_mode_ == LockManager::LOCK_REMOTE) {
      //SwitchToLocal();
    //} else if ((double)num_local_lock_ / (double)(num_local_lock_ + num_remote_lock_) <
        //ADAPT_THRESHOLD && current_lock_mode_ == LockManager::LOCK_LOCAL) {
      //SwitchToRemote();
    //}
  //}
  if (lock_type == LockManager::EXCLUSIVE) {
    int index = num_lock_object_ * manager_id + obj_index;
    pthread_mutex_lock(&mutex_);
    has_unlocked_exclusive_[index] = 0;
    pthread_mutex_unlock(&mutex_);
  }

  return lock_client->RequestLock(manager_id, user_id, lock_type, obj_index,
      lock_mode_table_[manager_id]);
}

int LockManager::SwitchToLocal() {
  current_lock_mode_ = LOCK_LOCAL;
  NotifyLockModeAll();
  usleep(100000);
  DisableRemoteAtomicAccess();
  return 0;
}

int LockManager::SwitchToRemote() {
  current_lock_mode_ = LOCK_REMOTE;
  NotifyLockModeAll();
  usleep(100000);
  EnableRemoteAtomicAccess();
  return 0;
}

// This function gets called when a user is done with the object
int LockManager::Unlock(int user_id, int manager_id, int lock_type,
    int obj_index) {
  int ret = 0;
  int index = num_lock_object_ * manager_id + obj_index;

  pthread_mutex_lock(&mutex_);
  if (lock_type == LockManager::EXCLUSIVE) {
    //has_unlocked_exclusive_[index] = user_id;
    //
    // if there is a next owner for the exclusive lock, send 'lock grant' message
    if (exclusive_to_exclusive_home_id_[index] != -1) {
      int next_owner = exclusive_to_exclusive_home_id_[index];
      int next_user_id = exclusive_to_exclusive_user_id_[index];
      LockClient* client = lock_clients_[next_owner];
      this->ResetExclusiveToExclusive(manager_id, obj_index);
      if (user_id == next_user_id) {
        cerr << "wrong" << endl;
      }
      ret = client->SendExclusiveToExclusiveLockGrant(manager_id, user_id, next_user_id,
          obj_index);
    // if there are nodes waiting for the shared lock, send 'lock grant' message
    } else if (!shared_lock_owner_waitlist_[obj_index]->empty()) {
      // broadcasts lock grant message.
      while (!shared_lock_owner_waitlist_[obj_index]->empty()) {
        int owner = shared_lock_owner_waitlist_[obj_index]->front();
        int user_id = shared_lock_user_id_waitlist_[obj_index]->front();
        //LockClient* client = communication_clients_[owner];
        LockClient* client = lock_clients_[owner];
        client->SendExclusiveToSharedLockGrant(rank_, owner, manager_id, user_id, obj_index);
        shared_lock_owner_waitlist_[obj_index]->pop();
        shared_lock_user_id_waitlist_[obj_index]->pop();
      }
    } else {
      LockClient* lock_client = lock_clients_[manager_id];
      ret = lock_client->RequestUnlock(manager_id, user_id, lock_type, obj_index,
          lock_mode_table_[manager_id]);
    }
  } else if (lock_type == LockManager::SHARED) {
    if (node_to_send_shared_release_[index] > 0) {
      LockClient* lock_client =
        lock_clients_[node_to_send_shared_release_[index]];
      node_to_send_shared_release_[index] = 0;
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "Sending RequestUnlock to " << node_to_send_shared_release_[index] << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      ret = lock_client->RequestUnlock(manager_id, user_id, lock_type, obj_index,
          lock_mode_table_[manager_id]);
    } else {
      LockClient* lock_client = lock_clients_[manager_id];
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "Sending RequestUnlock to " << manager_id << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      ret = lock_client->RequestUnlock(manager_id, user_id, lock_type, obj_index,
          lock_mode_table_[manager_id]);
    }
  }

  pthread_mutex_unlock(&mutex_);
  return ret;

  //LockClient* lock_client = lock_clients_[manager_id*MAX_USER+user_id];
  //return lock_client->RequestUnlock(user_id, lock_type, obj_index,
      //lock_mode_table_[manager_id]);
}

int LockManager::RelaySharedUnlockRequest(int home_id, int user_id, int obj_index) {
  LockClient* client = lock_clients_[home_id];
  return client->RequestUnlock(home_id, user_id, LockManager::SHARED, obj_index,
      lock_mode_table_[home_id]);
}

int LockManager::LockLocally(Context* context) {

  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == LOCK_REMOTE) {
    NotifyLockMode(context);
    return -1;
  }
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

int LockManager::LockLocally(Context* context, int user_id, int lock_type,
    int obj_index) {

  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == LOCK_REMOTE) {
    NotifyLockMode(context);
    return -1;
  }

  // get time
  clock_gettime(CLOCK_MONOTONIC, &start_local_lock_);

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

  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == LOCK_REMOTE) {
    NotifyLockMode(context);
    return -1;
  }

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

int LockManager::UnlockLocally(Context* context, int user_id, int lock_type,
    int obj_index) {

  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == LOCK_REMOTE) {
    NotifyLockMode(context);
    return -1;
  }

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

int LockManager::UpdateLockTableLocal(Context* context) {
  return 0;
}

int LockManager::UpdateLockTableRemote(Context* context) {
  return 0;
}

int LockManager::NotifyLockRequestResult(int user_id, int lock_type,
    int home_id, int obj_index, int result) {
  int index = num_lock_object_ * home_id + obj_index;
  if (lock_type == LockManager::EXCLUSIVE &&
      result == LockManager::RESULT_SUCCESS) {
    pthread_mutex_lock(&mutex_);
    has_unlocked_exclusive_[index] = 0;
    pthread_mutex_unlock(&mutex_);
    //exclusive_lock_holders_[index] = user_id;
  }
  pthread_mutex_lock(&user_mutex_);
  LockSimulator* user = user_map[user_id];
  //pthread_mutex_lock(user_mutex_map[user_id]);
  user->NotifyResult(LockManager::TASK_LOCK, lock_type, home_id, obj_index,
      result);
  pthread_mutex_unlock(&user_mutex_);
  //pthread_mutex_unlock(user_mutex_map[user_id]);
}

int LockManager::NotifyUnlockRequestResult(int user_id, int lock_type,
    int home_id, int obj_index, int result, bool reset_counter) {
    pthread_mutex_lock(&user_mutex_);
  if (LockManager::PRINT_DEBUG) {
    if (result != LockManager::RESULT_SUCCESS) {
      cerr << "Unlock failure" << endl;
    }
  }
  //int index = num_lock_object_ * home_id + obj_index;
  //if (result == LockManager::RESULT_SUCCESS) {
    //pthread_mutex_lock(&mutex_);
    //if (lock_type == LockManager::SHARED) {
      //shared_lock_counter_[index] = 0;
    //}
    //pthread_mutex_unlock(&mutex_);
  //}

  //int index = num_lock_object_ * home_id + obj_index;
  //if (result == LockManager::RESULT_SUCCESS) {
    //pthread_mutex_lock(&mutex_);
    //if (lock_type == LockManager::EXCLUSIVE) {
      //if (reset_counter) {
        //node_to_release_shared_lock_[index] = -1;
        //shared_lock_to_receive_[index] = 0;
        //shared_lock_counter_[index] = 0;
      //}
      //exclusive_lock_holders_[index] = 0;
    //} else if (lock_type == LockManager::SHARED) {
      //shared_lock_counter_[index] = 0;
    //}
    //pthread_mutex_unlock(&mutex_);
  //}
  LockSimulator* user = user_map[user_id];
  //pthread_mutex_lock(user_mutex_map[user_id]);
  user->NotifyResult(LockManager::TASK_UNLOCK, lock_type, home_id, obj_index,
      result);
  //pthread_mutex_unlock(user_mutex_map[user_id]);
    pthread_mutex_unlock(&user_mutex_);
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
  attributes->cap.max_send_sge = 16;
  attributes->cap.max_recv_sge = 16;
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

  //pthread_mutex_lock(&wc_mutex_);
  Context* context = (Context *)work_completion->wr_id;

  if (work_completion->status != IBV_WC_SUCCESS) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "(LockManager) Work completion status is not IBV_WC_SUCCESS: " <<
      rank_ << ":" << work_completion->status << "," << work_completion->opcode << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
    pthread_mutex_unlock(&wc_mutex_);
    return -1;
  }

  if (work_completion->opcode & IBV_WC_RECV) {
    int type = context->receive_message->type;

    // Post receive
    ReceiveMessage(context);

    // if client is requesting semaphore MR
    if (type == Message::LOCK_TABLE_MR_REQUEST) {
      SendLockTableMemoryRegion(context);
    } else if (type == Message::LOCK_REQUEST) {
      LockLocally(context);
    } else if (type == Message::UNLOCK_REQUEST) {
      //UnlockLocally(context);
      if (context->receive_message->lock_type == LockManager::SHARED) {
        HandleSharedLockRelease(context);
      } else {
        cerr << "SOMETHING IS WRONG!" << endl;
      }
    } else if (type == Message::SHARED_UNLOCK_RESULT) {
      HandleSharedUnlockResult(context);
    } else if (type == Message::EXCLUSIVE_TO_SHARED_LOCK_REQUEST) {
      HandleExclusiveToSharedLockRequest(context);
    } else if (type == Message::EXCLUSIVE_TO_SHARED_LOCK_GRANT) {
      HandleExclusiveToSharedLockGrant(context);
    } else if (type == Message::SHARED_TO_EXCLUSIVE_LOCK_REQUEST) {
      HandleSharedToExclusiveLockRequest(context);
    } else if (type == Message::SHARED_TO_EXCLUSIVE_LOCK_GRANT) {
      HandleSharedToExclusiveLockGrant(context);
    } else if (type == Message::EXCLUSIVE_TO_EXCLUSIVE_LOCK_REQUEST) {
      HandleExclusiveToExclusiveLockRequest(context);
    } else if (type == Message::EXCLUSIVE_TO_EXCLUSIVE_LOCK_GRANT) {
      HandleExclusiveToExclusiveLockGrant(context);
    } else {
      cerr << "Unknown message type: " << type
        << endl;
      //pthread_mutex_unlock(&wc_mutex_);
      return -1;
    }

  }
  //pthread_mutex_unlock(&wc_mutex_);
  return 0;
}

int LockManager::BroadcastExclusiveToSharedLockGrant(int home_id, int obj_index) {

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Broadcasting Exclusive-to-Shared Lock Grant: " << rank_ << ", " <<
      home_id << ", " << obj_index << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  int index = num_lock_object_ * home_id + obj_index;
  pthread_mutex_lock(&mutex_);

  has_unlocked_exclusive_[index] = rank_;
  if (!shared_lock_owner_waitlist_[obj_index]->empty()) {
    // broadcasts lock grant message.
    while (!shared_lock_owner_waitlist_[obj_index]->empty()) {
      int owner = shared_lock_owner_waitlist_[obj_index]->front();
      int user_id = shared_lock_user_id_waitlist_[obj_index]->front();
      //LockClient* client = communication_clients_[owner];
      LockClient* client = lock_clients_[owner];
      client->SendExclusiveToSharedLockGrant(rank_, owner, home_id, user_id, obj_index);
      shared_lock_owner_waitlist_[obj_index]->pop();
      shared_lock_user_id_waitlist_[obj_index]->pop();
    }
  }
  pthread_mutex_unlock(&mutex_);
}

int LockManager::HandleExclusiveToSharedLockRequest(Context* context) {
  int current_owner = context->receive_message->current_owner;
  int new_owner = context->receive_message->new_owner;
  int user_id = context->receive_message->user_id;
  int home_id = context->receive_message->home_id;
  int obj_index = context->receive_message->obj_index;
  int index = num_lock_object_ * home_id + obj_index;

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Exclusive-to-Shared Lock Request received: " << rank_ << ", "
      << current_owner << ", " << new_owner << ", " << home_id << ", " << obj_index << ", "
      << has_unlocked_exclusive_[index] << ", " << exclusive_to_shared_unlock_to_receive_[index]
      << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  pthread_mutex_lock(&mutex_);
  node_to_unlock_exclusive_shared_[index] = home_id;
  if (has_unlocked_exclusive_[index] > 0) {
    LockClient* client = lock_clients_[new_owner];
    client->SendExclusiveToSharedLockGrant(current_owner, new_owner, home_id, user_id, obj_index);
  } else {
    shared_lock_owner_waitlist_[obj_index]->push(new_owner);
    shared_lock_user_id_waitlist_[obj_index]->push(user_id);
  }
  //shared_lock_to_receive_[index]++;
  exclusive_to_shared_unlock_to_receive_[index]++;
  pthread_mutex_unlock(&mutex_);
}

int LockManager::HandleSharedLockRelease(Context* context) {


  int ret = 0;
  int lock_result = LockManager::RESULT_SUCCESS;
  int user_id = context->receive_message->user_id;
  int home_id = context->receive_message->home_id;
  int obj_index = context->receive_message->obj_index;
  int index = num_lock_object_ * home_id + obj_index;
  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared = (uint32_t)(*lock_object);

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Shared Lock Release received: " << rank_ << ", " <<
      home_id << ", " << obj_index << ", " << shared_lock_counter_[index] << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  // lock locally on lock table
  pthread_mutex_lock(&mutex_);

  has_unlocked_shared_[obj_index] = false;
  // when # request == # counter, pass it to other or unlock it.
  ++shared_lock_counter_[index];
  if (shared_to_exclusive_home_id_[index] == -1 &&
      node_to_unlock_exclusive_shared_[index] == 0) {
    // if no one else is waiting for exclusive lock, unlock it
    if (shared_lock_counter_[index] == shared) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "path 1: " << index << ", " << shared_lock_counter_[index] << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      //if (home_id != rank_) {
        //cerr << "This should not happen?" << endl;
        //LockClient* lock_client = lock_clients_[home_id];
        //lock_client->UnlockShared(home_id, obj_index, shared_lock_counter_[index]);
      //} else {
        //local_client_->UnlockShared(rank_, obj_index, shared_lock_counter_[index]);
      //}
      LockClient* lock_client = lock_clients_[home_id];
      //lock_client->UnlockShared(home_id, user_id, obj_index, shared_lock_counter_[index]);
      //while (is_unlocking_shared_[index]) {
        //pthread_cond_wait(&cond_, &mutex_);
      //}
      local_client_->UnlockShared(home_id, user_id, obj_index, shared_lock_counter_[index]);
      is_unlocking_shared_[index] = true;
      //has_unlocked_shared_[obj_index] = true;
      if (SendUnlockRequestResult(context, user_id, LockManager::SHARED, obj_index,
            lock_result)) {
        cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
        return -1;
      }
      //shared_lock_counter_[index] = 0;
    } else if (shared == 0) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "path 2: " << index << ", " << shared_lock_counter_[index] << ", " << shared << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      if (SendUnlockRequestResult(context, user_id, LockManager::SHARED, obj_index,
            lock_result)) {
        cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
        return -1;
      }
    } else {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "path 3: " << index << ", " << shared_lock_counter_[index] << ", " << shared << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      if (SendUnlockRequestResult(context, user_id, LockManager::SHARED, obj_index,
            lock_result)) {
        cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
        return -1;
      }
    }
  } else if (shared_to_exclusive_home_id_[index] > -1) {
    // if someone is waiting for its exclusive lock, send the grant message
    if (shared_lock_counter_[index] == shared_to_exclusive_unlock_to_receive_[index]) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "path 4" << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      int home_id = shared_to_exclusive_home_id_[index];
      int next_user_id = shared_to_exclusive_user_id_[index];
      LockClient* client = lock_clients_[home_id];
      ret = client->SendSharedToExclusiveLockGrant(rank_, next_user_id, obj_index);

      if (SendUnlockRequestResult(context, user_id, LockManager::SHARED, obj_index,
            lock_result)) {
        cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
        return -1;
      }
      shared_lock_counter_[index] = 0;
      shared_to_exclusive_unlock_to_receive_[index] = 0;
      shared_to_exclusive_home_id_[index] = -1;
      shared_to_exclusive_user_id_[index] = 0;
    } else {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "path 5: " << index << ", " << shared_lock_counter_[index] << ", " <<
          shared_to_exclusive_unlock_to_receive_[index] << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }
      if (SendUnlockRequestResult(context, user_id, LockManager::SHARED, obj_index,
            lock_result)) {
        cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
        return -1;
      }
      // this means it has done already?
      if (shared_to_exclusive_unlock_to_receive_[index] == 0) {
        shared_lock_counter_[index] = 0;
      }
    }
  } else if (node_to_unlock_exclusive_shared_[index] > 0) {
    // unlock exclusive + shared when it receives all shared release messages.
    if (shared_lock_counter_[index] >= exclusive_to_shared_unlock_to_receive_[index]) {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "path 6: " << index << ", " << shared_lock_counter_[index] << ", " <<
          exclusive_to_shared_unlock_to_receive_[index] << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }

      int home_id = node_to_unlock_exclusive_shared_[index];
      LockClient* lock_client = lock_clients_[home_id];
      uint64_t old_value;
      uint32_t shared, exclusive;
      exclusive = rank_;
      shared = exclusive_to_shared_unlock_to_receive_[index];
      old_value = ((uint64_t)exclusive) << 32 | shared;
      lock_client->RequestUnlock(home_id, rank_, LockManager::EXCLUSIVE, obj_index,
          lock_mode_table_[home_id], old_value, user_id);
      //shared_lock_counter_[index] = 0;
      //exclusive_to_shared_unlock_to_receive_[index] = 0;
      //shared_lock_to_receive_[index] = 0;
    } else {
      if (LockManager::PRINT_DEBUG) {
        pthread_mutex_lock(&LockManager::print_mutex);
        cerr << "path 7: " << shared_lock_counter_[index] << ", " <<
          exclusive_to_shared_unlock_to_receive_[index] << endl;
        pthread_mutex_unlock(&LockManager::print_mutex);
      }

      // send unlock result.
      if (SendUnlockRequestResult(context, user_id, LockManager::SHARED, obj_index,
            lock_result)) {
        cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
        return -1;
      }
    }
  } else {
    if (LockManager::PRINT_DEBUG) {
      cerr << "ERROR: " << shared_lock_counter_[index] << ", " <<
        shared_to_exclusive_unlock_to_receive_[index] << ", " <<
        exclusive_to_shared_unlock_to_receive_[index] << endl;
      cerr << "ERROR: ";
      cerr << shared_to_exclusive_home_id_[index] << ", ";
      cerr << shared_to_exclusive_user_id_[index] << ", ";
      cerr << exclusive_to_exclusive_home_id_[index] << ", ";
      cerr << exclusive_to_exclusive_user_id_[index] << ", ";
      cerr << node_to_unlock_exclusive_shared_[index] << endl;
      cerr << "ERROR: This should not happen." << endl;
    }
    return -1;
  }

  pthread_mutex_unlock(&mutex_);

  return ret;
}

int LockManager::HandleSharedUnlockResult(Context* context) {
  int home_id   = context->receive_message->home_id;
  int obj_index = context->receive_message->obj_index;
  int result    = context->receive_message->lock_result;
  int index     = num_lock_object_ * home_id + obj_index;

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Shared Unlock Result received: " << pthread_self() << " @ " << rank_ << ", " <<
      home_id << ", " << obj_index << ", " << result << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  if (result == LockManager::RESULT_RETRY) {
    pthread_mutex_lock(&mutex_);
    if (shared_lock_counter_[index] > 0)
      shared_lock_counter_[index]--;
    if (shared_lock_counter_[index] < 0) {
      pthread_mutex_lock(&LockManager::print_mutex);
      cerr << "ERROR: shared lock counter became negative" << endl;
      pthread_mutex_unlock(&LockManager::print_mutex);
    }
    pthread_mutex_unlock(&mutex_);
  }

  this->NotifyUnlockRequestResult(rank_,
      LockManager::SHARED,
      home_id,
      obj_index,
      result);
}

int LockManager::HandleExclusiveToSharedLockGrant(Context* context) {
  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Exclusive-to-Shared Lock Grant received: " << rank_ << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }
  int current_owner = context->receive_message->current_owner;
  int home_id = context->receive_message->home_id;
  int user_id = context->receive_message->user_id;
  int obj_index = context->receive_message->obj_index;
  int index = num_lock_object_ * home_id + obj_index;

  pthread_mutex_lock(&mutex_);
  node_to_send_shared_release_[index] = current_owner;
  pthread_mutex_unlock(&mutex_);

  this->SendExclusiveToSharedLockGrantAck(context, user_id, home_id, obj_index);
  return this->NotifyLockRequestResult(user_id, LockManager::SHARED,
      home_id, obj_index, LockManager::RESULT_SUCCESS);
}

int LockManager::HandleSharedToExclusiveLockRequest(Context* context) {
  int home_id           = context->receive_message->home_id;
  int new_owner         = context->receive_message->new_owner;
  int user_id           = context->receive_message->user_id;
  int obj_index         = context->receive_message->obj_index;
  int shared_to_receive = context->receive_message->shared_lock_counter;

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Shared-To-Exclusive Lock Request received: " << rank_ << ", " << shared_to_receive
      << ", " << new_owner << ", " << home_id << ", " << obj_index << ", "<< user_id
      << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  int index = num_lock_object_ * home_id + obj_index;

  pthread_mutex_lock(&mutex_);
  shared_to_exclusive_home_id_[index] = new_owner;
  shared_to_exclusive_user_id_[index] = user_id;
  if (shared_to_receive > 0)
    shared_to_exclusive_unlock_to_receive_[index]    = shared_to_receive;

  // should check if already eligible for exclusive lock
  if (shared_lock_counter_[index] == shared_to_receive) {
      //has_unlocked_shared_[obj_index]) {
    LockClient* client = lock_clients_[new_owner];
    client->SendSharedToExclusiveLockGrant(home_id, user_id, obj_index);
    //if (has_unlocked_shared_[obj_index]) {
      //has_unlocked_shared_[obj_index] = false;
    //}
    shared_lock_counter_[index] = 0;
    shared_to_exclusive_unlock_to_receive_[index] = 0;
    shared_to_exclusive_home_id_[index] = -1;
    shared_to_exclusive_user_id_[index] = 0;
  }
  pthread_mutex_unlock(&mutex_);

  return 0;
}

int LockManager::HandleSharedToExclusiveLockGrant(Context* context) {
  int home_id = context->receive_message->home_id;
  int user_id = context->receive_message->user_id;
  int obj_index = context->receive_message->obj_index;
  int lock_type = context->receive_message->lock_type;

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Shared-To-Exclusive Lock Grant received: " << rank_ << ", " << home_id
      << ", " << user_id << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  this->SendSharedToExclusiveLockGrantAck(context, home_id, user_id, obj_index);
  return this->NotifyLockRequestResult(user_id, LockManager::EXCLUSIVE,
      home_id, obj_index, LockManager::RESULT_SUCCESS);
}

int LockManager::HandleExclusiveToExclusiveLockRequest(Context* context) {
  int home_id           = context->receive_message->home_id;
  int new_owner         = context->receive_message->new_owner;
  int user_id           = context->receive_message->user_id;
  int obj_index         = context->receive_message->obj_index;


  int index = num_lock_object_ * home_id + obj_index;

  pthread_mutex_lock(&mutex_);
  exclusive_to_exclusive_home_id_[index] = new_owner;
  exclusive_to_exclusive_user_id_[index] = user_id;
  if (user_id == rank_) {
    cerr << "something wrong"<< endl;
  }

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Exclusive-To-Exclusive Lock Request received: " <<
      rank_ << ", " << home_id << ", " << obj_index << ", " <<
      has_unlocked_exclusive_[index] << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  // should check if already eligible for exclusive lock
  if (has_unlocked_exclusive_[index] > 0) {
    LockClient* client = lock_clients_[new_owner];
    int prev_user_id = has_unlocked_exclusive_[index];
    this->ResetExclusiveToExclusive(home_id, obj_index);
    client->SendExclusiveToExclusiveLockGrant(home_id,
        prev_user_id, user_id, obj_index);
  }
  pthread_mutex_unlock(&mutex_);

  return 0;
}

int LockManager::HandleExclusiveToExclusiveLockGrant(Context* context) {
  int home_id = context->receive_message->home_id;
  int user_id = context->receive_message->user_id;
  int prev_user_id = context->receive_message->prev_user_id;
  int obj_index = context->receive_message->obj_index;
  int lock_type = context->receive_message->lock_type;

  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Exclusive-To-Exclusive Lock Grant received: "
      << rank_ << ", " << home_id << ", " << prev_user_id << ", " << user_id
      << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }

  this->SendExclusiveToExclusiveLockGrantAck(context, home_id, prev_user_id, obj_index);
  return this->NotifyLockRequestResult(user_id, LockManager::EXCLUSIVE,
      home_id, obj_index, LockManager::RESULT_SUCCESS);
}

void LockManager::ResetByUnlock(int home_id, int obj_index, int shared_count) {
  int index = num_lock_object_ * home_id + obj_index;
  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Reset By Unlock: " << rank_ << ", " << home_id << ", " << index <<
      ", " << shared_count << endl;
    if (home_id == 0) {
      cerr << "HERE" << endl;
    }
    pthread_mutex_unlock(&LockManager::print_mutex);
  }
  pthread_mutex_lock(&mutex_);
  is_unlocking_shared_[index] = false;
  pthread_cond_signal(&cond_);
  node_to_unlock_exclusive_shared_[index] = 0;
  exclusive_to_shared_unlock_to_receive_[index] = 0;
  if (shared_count > 0 && shared_count <= shared_lock_counter_[index]) {
    shared_lock_counter_[index] -= shared_count;
  } else {
    shared_lock_counter_[index] = 0;
  }
  has_unlocked_exclusive_[index] = rank_;

  // if there is a next owner for the exclusive lock, send 'lock grant' message
  if (exclusive_to_exclusive_home_id_[index] != -1) {
    int next_owner = exclusive_to_exclusive_home_id_[index];
    int next_user_id = exclusive_to_exclusive_user_id_[index];
    LockClient* client = lock_clients_[next_owner];
    this->ResetExclusiveToExclusive(home_id, obj_index);
    if (rank_ == next_user_id) {
      cerr << "something is wrong..." << endl;
    }
    client->SendExclusiveToExclusiveLockGrant(home_id, rank_, next_user_id,
        obj_index);
  } else if (shared_to_exclusive_home_id_[index] > -1) {
    if (shared_lock_counter_[index] == shared_to_exclusive_unlock_to_receive_[index]) {
      int home_id = shared_to_exclusive_home_id_[index];
      int next_user_id = shared_to_exclusive_user_id_[index];
      LockClient* client = lock_clients_[home_id];
      int ret = client->SendSharedToExclusiveLockGrant(rank_, next_user_id, obj_index);
      shared_lock_counter_[index] = 0;
      shared_to_exclusive_unlock_to_receive_[index] = 0;
      shared_to_exclusive_home_id_[index] = -1;
      shared_to_exclusive_user_id_[index] = 0;
    }
  }
  shared_to_exclusive_unlock_to_receive_[index] = 0;
  pthread_mutex_unlock(&mutex_);
}

void LockManager::ResetExclusiveToExclusive(int home_id, int obj_index) {
  int index = num_lock_object_ * home_id + obj_index;
  exclusive_to_exclusive_home_id_[index] = -1;
  exclusive_to_exclusive_user_id_[index] = 0;
  has_unlocked_exclusive_[index] = 0;
}

void LockManager::ResetSharedToExclusive(int home_id, int obj_index) {
  if (LockManager::PRINT_DEBUG) {
    pthread_mutex_lock(&LockManager::print_mutex);
    cerr << "Reset Shared-to-Exclusive" << endl;
    pthread_mutex_unlock(&LockManager::print_mutex);
  }
  int index = num_lock_object_ * home_id + obj_index;
  pthread_mutex_lock(&mutex_);
  shared_to_exclusive_home_id_[index] = -1;
  shared_to_exclusive_user_id_[index] = 0;
  shared_to_exclusive_unlock_to_receive_[index] = 0;
  pthread_mutex_unlock(&mutex_);
}

int LockManager::RegisterContext(Context* context) {
  context_set_.insert(context);
  return 0;
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

double LockManager::GetAverageSendMessageTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<int, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetAverageSendMessageTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageReceiveMessageTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<int, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetAverageReceiveMessageTime();
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
  return current_lock_mode_;
}

void* LockManager::RunLockClient(void* args) {
  LockClient* client = static_cast<LockClient*>(args);
  client->Run();
}


}} // end namespace
