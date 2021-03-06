#include "lock_manager.h"
#include "notify_lock_client.h"
#include "direct_queue_lock_client.h"
#include "direct_queue_lock_client_two.h"
#include "lock_client.h"
#include "communication_client.h"

namespace rdma { namespace proto {

int LockManager::shared_exclusive_rule_    = LockManager::RULE_FAIL;
int LockManager::exclusive_shared_rule_    = LockManager::RULE_FAIL;
int LockManager::exclusive_exclusive_rule_ = LockManager::RULE_FAIL;
int LockManager::poll_retry_               = 10;
int LockManager::fail_retry_               = 10;

// constructor
LockManager::LockManager(const string& work_dir, uint32_t rank,
    int num_manager, int num_lock_object, int lock_mode, int num_total_user, int num_client) {
  work_dir_                        = work_dir;
  rank_                            = rank;
  num_manager_                     = num_manager;
  num_client_                      = num_client;
  num_total_user_                  = num_total_user;
  num_lock_object_                 = num_lock_object;
  lock_table_                      = new uint64_t[num_lock_object_];
  last_lock_table_                 = new uint64_t[num_lock_object_];
  fail_count_                      = new uint64_t[num_lock_object_];
  lock_mode_table_                 = new int[num_manager_];
  listener_                        = NULL;
  event_channel_                   = NULL;
  registered_memory_region_        = NULL;
  port_                            = 0;
  lock_mode_                       = lock_mode;
  total_local_exclusive_lock_time_ = 0;
  total_local_shared_lock_time_    = 0;
  num_local_exclusive_lock_        = 0;
  num_local_shared_lock_           = 0;
  num_local_lock_                  = 0;
  num_remote_lock_                 = 0;
  num_rdma_send_                   = 0;
  num_rdma_recv_                   = 0;

  if (lock_mode_ == LOCK_ADAPTIVE) {
    current_lock_mode_ = LOCK_REMOTE_POLL;
  } else {
    current_lock_mode_ = lock_mode_;
  }

  // initialize lock table with 0
  memset(lock_table_, 0x00, num_lock_object_*sizeof(uint64_t));
  memset(last_lock_table_, 0x00, num_lock_object_*sizeof(uint64_t));
  memset(fail_count_, 0x00, num_lock_object_*sizeof(uint64_t));

  lock_mode_table_[rank_] = current_lock_mode_;

  //for (int i=0;i<num_manager_;++i) {
    //// every lock manager starts in remote mode.
    //lock_mode_table_[i] = LockManager::LOCK_REMOTE;
  //}

  // initialize lock wait queues, one for each lock object
  wait_queues_.reserve(num_lock_object_);
  for (int i = 0; i < num_lock_object_; ++i) {
    LockWaitQueue* queue = new LockWaitQueue(num_manager+36); // +36 is for the buffer
    wait_queues_.push_back(queue);
  }

  // initialize local lock mutex
  lock_mutex_ = new pthread_mutex_t*[num_lock_object_];
  pthread_mutex_init(&msg_mutex_, NULL);
  pthread_mutex_init(&poll_mutex_, NULL);
  pthread_mutex_init(&seq_mutex_, NULL);
}

// destructor
LockManager::~LockManager() {
  if (lock_table_)
    delete[] lock_table_;

  if (lock_mode_table_)
    delete[] lock_mode_table_;

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

int LockManager::RegisterUser(uint32_t user_id, LockSimulator* user) {
  user_map[user_id] = user;
  pthread_mutex_t* mutex = new pthread_mutex_t;
  pthread_mutex_init(mutex, NULL);
  user_mutex_map[user_id] = mutex;
  users.push_back(user);
  num_user_ = users.size();
  return 0;
}

int LockManager::InitializeLockClients() {
  int ret = 0;
  for (int i = 0; i < num_manager_; ++i) {
    for (int j = 0; j < users.size(); ++j) {
      LockSimulator* user      = users[j];
      pthread_t* client_thread = new pthread_t;
      LockClient* client;
      if (lock_mode_ == LOCK_REMOTE_NOTIFY)
        client = new NotifyLockClient(work_dir_, this, user, i);
      else if (lock_mode_ == LOCK_REMOTE_QUEUE)
        client = new DirectQueueLockClient(work_dir_, this, user, i);
      else
        client = new LockClient(work_dir_, this, user, i);

      ret = pthread_create(client_thread, NULL,
            &LockManager::RunLockClient, (void*)client);

      if (ret) {
        cout << "LockManager::pthread_create(): " << strerror(ret) << endl;
        return -1;
      }

      lock_clients_[MAX_USER*i+user->GetID()] = client;
      lock_client_threads_.push_back(client_thread);

      if (ret) {
        cout << "LockManager::pthread_create(): " << strerror(ret) << endl;
        return -1;
      }

      user_to_home_map_[user->GetID()] = i;
    }

    CommunicationClient* comm_client = new CommunicationClient(work_dir_, this, NULL, i);
    pthread_t* comm_client_thread    = new pthread_t;
    ret = pthread_create(comm_client_thread, NULL,
        &LockManager::RunLockClient, (void*)comm_client);

    if (ret) {
      cout << "LockManager::pthread_create(): " << strerror(ret) << endl;
      return -1;
    }
    communication_clients_[i] = comm_client;
    communication_client_threads_.push_back(comm_client_thread);

  }
  // initialize local work queue/poller.
  local_work_queue_ = new LocalWorkQueue<Message>;
  ret = pthread_create(&local_work_poller_, NULL, &LockManager::PollLocalWorkQueue,
      (void*)this);
  if (ret) {
    perror("LockManager::pthread_create()");
    return -1;
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

  context->send_message_buffer = new MessageBuffer;
  context->receive_message_buffer = new MessageBuffer;

  context->lock_table = lock_table_;

  if (context->send_message_buffer->Register(context)) {
    cerr << "MessageBuffer::Register failed()" << endl;
    return -1;
  }
  if (context->receive_message_buffer->Register(context)) {
    cerr << "MessageBuffer::Register failed()" << endl;
    return -1;
  }

  //context->send_mr = ibv_reg_mr(context->protection_domain,
      //context->send_message,
      //sizeof(*(context->send_message)),
      //IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
  //if (context->send_mr == NULL) {
    //cerr << "ibv_reg_mr() failed for send_mr." << endl;
    //return -1;
  //}
  //context->receive_mr = ibv_reg_mr(context->protection_domain,
      //context->receive_message,
      //sizeof(*(context->receive_message)),
      //IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  //if (context->receive_mr == NULL) {
    //cerr << "ibv_reg_mr() failed for receive_mr." << endl;
    //return -1;
  //}
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
  if (current_lock_mode_ == LOCK_PROXY_RETRY ||
      current_lock_mode_ == LOCK_PROXY_QUEUE) {
    struct ibv_qp_attr attr;
    memset(&attr, 0x00, sizeof(attr));
    attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    if (ibv_modify_qp(context->queue_pair, &attr, IBV_QP_ACCESS_FLAGS)) {
      cerr << "ibv_modify_qp() failed." << endl;
      return -1;
    }
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

  if (context->lock_table_mr)
    ibv_dereg_mr(context->lock_table_mr);

  delete context->send_message_buffer;
  delete context->receive_message_buffer;

  // rdma_destroy_id() also causes seg fault when client disconnects. why?
  //rdma_destroy_id(context->id);

  delete context;

  //cout << "client disconnected." << endl;
  return 0;
}

// Send local lock table memory region + current lock mode to client.
int LockManager::SendLockTableMemoryRegion(Context* context) {

  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();
  msg->type       = Message::LOCK_TABLE_MR;
  msg->lock_mode  = current_lock_mode_;
  msg->manager_id = rank_;

  memcpy(&msg->lock_table_mr, context->lock_table_mr,
      sizeof(msg->lock_table_mr));
  if (SendMessage(context)) {
    cerr << "SendLockTableMemoryRegion(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&msg_mutex_);
  //cout << "SendLockTableMemoryRegion(): memory region sent." << endl;
  return 0;
}

// Send lock request result to client.
int LockManager::SendLockRequestResult(Context* context, int seq_no, uint32_t user_id,
    int lock_type, int obj_index, int result) {

  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();

  msg->type        = Message::LOCK_REQUEST_RESULT;
  msg->seq_no      = seq_no;
  msg->user_id     = user_id;
  msg->lock_type   = lock_type;
  msg->obj_index   = obj_index;
  msg->lock_result = result;

  if (SendMessage(context)) {
    cerr << "SendLockRequestResult(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  //cout << "SendLockRequestResult(): memory region sent." << endl;
  pthread_mutex_unlock(&msg_mutex_);
  return 0;
}

// Send unlock request result to client.
int LockManager::SendUnlockRequestResult(Context* context, int seq_no, uint32_t user_id,
    int lock_type, int obj_index, int result) {

  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();

  msg->type        = Message::UNLOCK_REQUEST_RESULT;
  msg->seq_no      = seq_no;
  msg->user_id     = user_id;
  msg->lock_type   = lock_type;
  msg->obj_index   = obj_index;
  msg->lock_result = result;
  if (SendMessage(context)) {
    cerr << "SendUnlockRequestResult(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&msg_mutex_);
  //cout << "SendUnlockRequestResult(): memory region sent." << endl;
  return 0;
}

// Send unlock request result to client.
int LockManager::SendGrantLockAck(Context* context, int seq_no, uint32_t user_id, int lock_type,
    int obj_index) {

  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();

  msg->type      = Message::GRANT_LOCK_ACK;
  msg->seq_no    = seq_no;
  msg->user_id   = user_id;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  if (SendMessage(context)) {
    cerr << "SendGrantLockAck(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&msg_mutex_);
  return 0;
}


int LockManager::NotifyLockModeAll() {
  for (set<Context*>::iterator it = context_set_.begin();
      it != context_set_.end(); ++it) {
    Context* context = *it;
    NotifyLockMode(context);
  }
}

int LockManager::NotifyLockMode(Context* context) {

  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();

  msg->type       = Message::LOCK_MODE;
  msg->manager_id = rank_;
  msg->lock_mode  = current_lock_mode_;

  if (SendMessage(context)) {
    cerr << "SendUnlockRequestResult(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }
  pthread_mutex_unlock(&msg_mutex_);
  return 0;
}

int LockManager::Lock(int seq_no, uint32_t user_id, uint32_t manager_id, int lock_type,
    int obj_index) {
  LockClient* lock_client = lock_clients_[manager_id*MAX_USER+user_id];
  if (lock_mode_ == LockManager::LOCK_ADAPTIVE) {
    if (manager_id == this->GetID()) {
      ++num_local_lock_;
      if (num_local_lock_ + num_remote_lock_ > NUM_LOCK_HISTORY) {
        if (num_local_lock_ > NUM_LOCK_HISTORY) {
          num_local_lock_ = NUM_LOCK_HISTORY;
        } else {
          --num_remote_lock_;
        }
      }
    } else {
      ++num_remote_lock_;
      if (num_local_lock_ + num_remote_lock_ > NUM_LOCK_HISTORY) {
        if (num_remote_lock_ > NUM_LOCK_HISTORY) {
          num_remote_lock_ = NUM_LOCK_HISTORY;
        } else {
          --num_local_lock_;
        }
      }
    }

    if ((double)num_local_lock_ / (double)(num_local_lock_ + num_remote_lock_) >=
        ADAPT_THRESHOLD && current_lock_mode_ == LockManager::LOCK_REMOTE) {
      SwitchToLocal();
    } else if ((double)num_local_lock_ / (double)(num_local_lock_ + num_remote_lock_) <
        ADAPT_THRESHOLD && current_lock_mode_ == LockManager::LOCK_LOCAL) {
      SwitchToRemote();
    }
  }

  //if ((current_lock_mode_ == LOCK_PROXY_QUEUE || current_lock_mode_ == LOCK_PROXY_RETRY) &&
      //manager_id == rank_) {
    //Message new_work;
    //new_work.seq_no    = seq_no;
    //new_work.home_id   = manager_id;
    //new_work.user_id   = user_id;
    //new_work.lock_type = lock_type;
    //new_work.obj_index = obj_index;
    //new_work.task      = TASK_LOCK;

    //local_work_queue_->Insert(new_work);
    //return 0;
  //} else {
    //return lock_client->RequestLock(seq_no, user_id, lock_type, obj_index,
        //lock_mode_table_[manager_id]);
  //}
  return lock_client->RequestLock(seq_no, user_id, lock_type, obj_index,
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

int LockManager::Unlock(int seq_no, uint32_t user_id, uint32_t manager_id, int lock_type,
    int obj_index) {
  //if ((current_lock_mode_ == LOCK_PROXY_QUEUE || current_lock_mode_ == LOCK_PROXY_RETRY) &&
      //manager_id == rank_) {
    //Message new_work;
    //new_work.seq_no    = seq_no;
    //new_work.home_id   = manager_id;
    //new_work.user_id   = user_id;
    //new_work.lock_type = lock_type;
    //new_work.obj_index = obj_index;
    //new_work.task      = TASK_UNLOCK;

    //local_work_queue_->Insert(new_work);
    //return 0;
  //} else {
    //LockClient* lock_client = lock_clients_[manager_id*MAX_USER+user_id];
    //return lock_client->RequestUnlock(seq_no, user_id, lock_type, obj_index,
        //lock_mode_table_[manager_id]);
  //}
  LockClient* lock_client = lock_clients_[manager_id*MAX_USER+user_id];
  return lock_client->RequestUnlock(seq_no, user_id, lock_type, obj_index,
      lock_mode_table_[manager_id]);
}

int LockManager::GrantLock(int seq_no, uint32_t user_id, uint32_t home_id, uint32_t waiting_id,
    int lock_type, int obj_index) {
  //uint64_t waiting_id = user_to_home_map_[user_id];
  //CommunicationClient* client = communication_clients_[MAX_USER*waiting_id+user_id];
  CommunicationClient* client = communication_clients_[waiting_id];
  //cerr << "GrantLock():" << rank_ << "," << waiting_id << "," << home_id << "," <<
    //user_id <<endl;
  return client->GrantLock(seq_no, user_id, home_id, obj_index, lock_type);
}

int LockManager::RejectLock(int seq_no, uint32_t user_id, uint32_t home_id, int lock_type,
    int obj_index) {
  CommunicationClient* client = communication_clients_[home_id];
  return client->RejectLock(seq_no, user_id, home_id, obj_index, lock_type);
}

int LockManager::LockLocallyWithRetry(Context* context, Message* message) {

  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == LOCK_REMOTE_POLL ||
      current_lock_mode_ == LOCK_REMOTE_NOTIFY) {
    NotifyLockMode(context);
    return -1;
  }
  // get time
  clock_gettime(CLOCK_MONOTONIC, &start_local_lock_);

  int lock_result  = RESULT_FAILURE;
  int seq_no       = message->seq_no;
  uint32_t user_id = message->user_id;
  int obj_index    = message->obj_index;
  int lock_type    = message->lock_type;

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared = (uint32_t)(*lock_object);

  // if shared lock is requested
  if (lock_type == LockManager::SHARED) {
    if (exclusive == 0) {
      shared += user_id;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
      lock_result = LockManager::RESULT_RETRY;
    }
  } else if (lock_type == LockManager::EXCLUSIVE) {
    // if exclusive lock is requested
    if (exclusive == 0 && shared == 0) {
      exclusive = message->user_id;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = LockManager::RESULT_SUCCESS;
    } else {
      lock_result = LockManager::RESULT_RETRY;
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

  if (context) {
    // send result back to the client
    if (SendLockRequestResult(context, seq_no, user_id, lock_type, obj_index,
          lock_result)) {
      cerr << "LockLocally() failed." << endl;
      return -1;
    }
  } else {
    // if context is NULL, it means it is a local workload
    this->NotifyLockRequestResult(
        seq_no,
        user_id,
        lock_type,
        obj_index,
        lock_result
        );
  }


  return 0;
}

int LockManager::LockLocallyWithQueue(Context* context, Message* message) {

  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == LOCK_REMOTE_POLL ||
      current_lock_mode_ == LOCK_REMOTE_NOTIFY) {
    NotifyLockMode(context);
    return -1;
  }
  // get time
  clock_gettime(CLOCK_MONOTONIC, &start_local_lock_);

  int lock_result      = RESULT_FAILURE;
  int seq_no           = message->seq_no;
  uint32_t home_id     = message->home_id;
  uint32_t user_id     = message->user_id;
  int obj_index        = message->obj_index;
  int lock_type        = message->lock_type;
  LockWaitQueue* queue = wait_queues_[obj_index];


  pthread_mutex_lock(&seq_mutex_);
  // get seq no from user
  int last_seq_no = last_seq_no_map_[user_id];
  // if seq no is less than last no.. ignore
  if (last_seq_no > seq_no) {
    pthread_mutex_unlock(&seq_mutex_);
    return 0;
  } else {
    last_seq_no_map_[user_id] = seq_no;
  }
  pthread_mutex_unlock(&seq_mutex_);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared = (uint32_t)(*lock_object);

  if (exclusive == 0 && shared == 0 && queue->GetSize() > 0) {
    queue->RemoveAll();
  }

  if (queue->GetSize() > 0) {
    // there are some users already waiting...
    queue->Insert(seq_no, home_id, user_id, lock_type);
  } else {
    // if shared lock is requested
    if (lock_type == SHARED) {
      if (exclusive == 0) {
        shared += user_id;
        *lock_object = ((uint64_t)exclusive) << 32 | shared;
        lock_result = LockManager::RESULT_SUCCESS;
      } else {
        queue->Insert(seq_no, home_id, user_id, lock_type);
      }
    } else if (lock_type == EXCLUSIVE) {
      // if exclusive lock is requested
      if (exclusive == 0 && shared == 0) {
        exclusive = message->user_id;
        *lock_object = ((uint64_t)exclusive) << 32 | shared;
        lock_result = LockManager::RESULT_SUCCESS;
      } else {
        queue->Insert(seq_no, home_id, user_id, lock_type);
      }
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

  // send result back to the client only if successful
  if (context) {
    if (lock_result == RESULT_SUCCESS) {
      if (SendLockRequestResult(context, seq_no, user_id, lock_type, obj_index,
            lock_result)) {
        cerr << "LockLocally() failed." << endl;
        return -1;
      }
    }
  } else {
    // if context is NULL, it means it is a local workload
    if (lock_result == RESULT_SUCCESS) {
      this->NotifyLockRequestResult(
          seq_no,
          user_id,
          lock_type,
          obj_index,
          lock_result
          );
    }
  }

  return 0;
}


//int LockManager::LockLocally(Context* context, int user_id, int lock_type,
    //int obj_index) {

  //// if current lock mode is remote (i.e. direct), then notify back.
  //if (current_lock_mode_ == LOCK_REMOTE) {
    //NotifyLockMode(context);
    //return -1;
  //}

  //// get time
  //clock_gettime(CLOCK_MONOTONIC, &start_local_lock_);

  //int lock_result = LockManager::RESULT_FAILURE;
  //uint64_t* lock_object = (lock_table_+obj_index);
  //uint32_t exclusive, shared;
  //exclusive = (uint32_t)((*lock_object)>>32);
  //shared = (uint32_t)(*lock_object);

  //// lock locally on lock table
  //pthread_mutex_lock(lock_mutex_[obj_index]);

  //// if shared lock is requested
  //if (lock_type == LockManager::SHARED) {
    //if (exclusive == 0) {
      //++shared;
      //*lock_object = ((uint64_t)exclusive) << 32 | shared;
      //lock_result = LockManager::RESULT_SUCCESS;
    //} else {
      //lock_result = LockManager::RESULT_FAILURE;
    //}
  //} else if (lock_type == LockManager::EXCLUSIVE) {
    //// if exclusive lock is requested
    //if (exclusive == 0 && shared == 0) {
      //exclusive = user_id;
      //*lock_object = ((uint64_t)exclusive) << 32 | shared;
      //lock_result = LockManager::RESULT_SUCCESS;
    //} else {
      //lock_result = LockManager::RESULT_FAILURE;
    //}
  //}

  //// unlock locally on lock table
  //pthread_mutex_unlock(lock_mutex_[obj_index]);

  //// get time
  //clock_gettime(CLOCK_MONOTONIC, &end_local_lock_);
  //double time_taken = ((double)end_local_lock_.tv_sec * 1e+9 +
      //(double)end_local_lock_.tv_nsec) -
    //((double)start_local_lock_.tv_sec * 1e+9 +
     //(double)start_local_lock_.tv_nsec);

  //if (lock_type == LockManager::SHARED) {
    //total_local_shared_lock_time_ += time_taken;
    //++num_local_shared_lock_;
  //} else if (lock_type == LockManager::EXCLUSIVE) {
    //total_local_exclusive_lock_time_ += time_taken;
    //++num_local_exclusive_lock_;
  //}

  //// send result back to the client
  //if (SendLockRequestResult(context, user_id, lock_type, obj_index,
        //lock_result)) {
    //cerr << "LockLocally() failed." << endl;
    //return -1;
  //}

  //return 0;
//}

int LockManager::LockLocalDirect(uint32_t user_id, int lock_type, int obj_index) {
  int lock_result = LockManager::RESULT_FAILURE;

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared = (uint32_t)(*lock_object);
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

int LockManager::UnlockLocallyWithRetry(Context* context, Message* message) {

  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == LOCK_REMOTE) {
    NotifyLockMode(context);
    return -1;
  }

  int lock_result;
  int seq_no       = message->seq_no;
  uint32_t user_id = message->user_id;
  int obj_index    = message->obj_index;
  int lock_type    = message->lock_type;

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  // get seq no from user
  int last_seq_no = last_seq_no_map_[user_id];
  // if seq no is less than last no.. ignore
  if (last_seq_no > seq_no) {
    pthread_mutex_unlock(lock_mutex_[obj_index]);
    return 0;
  } else {
    last_seq_no_map_[user_id] = seq_no;
  }
  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared    = (uint32_t)(*lock_object);

  // unlocking shared lock
  if (lock_type == SHARED) {
    if ((shared & user_id) != 0) {
      shared -= user_id;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = RESULT_SUCCESS;
    } else {
      cerr << "client is trying to unlock shared lock it did not acquire." << endl;
      lock_result = RESULT_FAILURE;
    }
  } else if (lock_type == EXCLUSIVE) {// if exclusive lock is requested
    if (exclusive == user_id) {
      exclusive = 0;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = RESULT_SUCCESS;
    } else {
      cerr << "client is trying to unlock exclusive lock," <<
        " which it does not own." << endl;
      lock_result = RESULT_FAILURE;
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  // send result back to the client
  if (context) {
    if (SendUnlockRequestResult(context, seq_no, user_id, lock_type, obj_index,
          lock_result)) {
      cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
      return -1;
    }
  } else {
    // if context is NULL, it means it is a local workload
    this->NotifyUnlockRequestResult(
        seq_no,
        user_id,
        lock_type,
        obj_index,
        lock_result
        );
  }

  return 0;
}

int LockManager::UnlockLocallyWithQueue(Context* context, Message* message) {

  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == LOCK_REMOTE_POLL || current_lock_mode_ == LOCK_REMOTE_NOTIFY) {
    NotifyLockMode(context);
    return -1;
  }

  int lock_result      = RESULT_FAILURE;
  int seq_no           = message->seq_no;
  uint32_t home_id     = message->home_id;
  uint32_t user_id     = message->user_id;
  int obj_index        = message->obj_index;
  int lock_type        = message->lock_type;
  LockWaitQueue* queue = wait_queues_[obj_index];

  //pthread_mutex_lock(&seq_mutex_);
  //// get seq no from user
  //int last_seq_no = last_seq_no_map_[user_id];
  //// if seq no is less than last no.. ignore
  //if (last_seq_no > seq_no) {
    //pthread_mutex_unlock(&seq_mutex_);
    //return 0;
  //} else {
    //last_seq_no_map_[user_id] = seq_no;
  //}
  //pthread_mutex_unlock(&seq_mutex_);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  uint64_t* lock_object = (lock_table_+obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object)>>32);
  shared    = (uint32_t)(*lock_object);

  // remove it from the queue as well if exists
  int num_elem = queue->RemoveAllElements(seq_no, home_id, user_id, lock_type);

  // unlocking shared lock
  if (lock_type == SHARED) {
    if ((shared & user_id) != 0) {
      shared -= user_id;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = RESULT_SUCCESS;
    }
  } else if (lock_type == EXCLUSIVE) {// if exclusive lock is requested
    if (exclusive == user_id) {
      exclusive = 0;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = RESULT_SUCCESS;
    }
  }

  if (*lock_object != 0) {
    ++fail_count_[obj_index];
  } else {
    fail_count_[obj_index] = 0;
  }

  // reset lock object if we see same value too many
  if (fail_count_[obj_index] > 10) {
    *lock_object = 0;
    queue->RemoveAll();
    fail_count_[obj_index] = 0;
  }

  // Notify waiting users
  if (lock_result == RESULT_SUCCESS) {
    LockWaitElement* elem = queue->Pop();
    if (elem) {
      //CommunicationClient* client = communication_clients_[MAX_USER*elem->home_id+elem->user_id];
      CommunicationClient* client = communication_clients_[elem->home_id];
      exclusive = (uint32_t)((*lock_object)>>32);
      shared    = (uint32_t)(*lock_object);
      if (exclusive == 0 && elem->type == SHARED) {
        if (exclusive != 0) {
          cerr << "exclusive should be zero." << endl;
          lock_result = RESULT_FAILURE;
        }
        shared += elem->user_id;
        *lock_object = ((uint64_t)exclusive) << 32 | shared;
        client->GrantLock(elem->seq_no, elem->user_id, elem->home_id, obj_index, elem->type);
        elem = queue->Front();
        while (elem && elem->type == SHARED) {
          queue->Pop();
          shared += elem->user_id;
          *lock_object = ((uint64_t)exclusive) << 32 | shared;
          //client = communication_clients_[MAX_USER*elem->home_id+elem->user_id];
          client = communication_clients_[elem->home_id];
          client->GrantLock(elem->seq_no, elem->user_id, elem->home_id, obj_index, elem->type);
          elem = queue->Front();
        }
      } else if (exclusive == 0 && shared == 0 && elem->type == EXCLUSIVE) {
        exclusive = (uint32_t)((*lock_object)>>32);
        shared    = (uint32_t)(*lock_object);
        if (exclusive != 0 || shared != 0) {
          cerr << "exclusive and shared both should be zero." << endl;
          lock_result = RESULT_FAILURE;
        }
        exclusive = elem->user_id;
        *lock_object = ((uint64_t)exclusive) << 32 | shared;

        client->GrantLock(elem->seq_no, elem->user_id, elem->home_id, obj_index, elem->type);
      }
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  // send result back to the client
  if (context) {
    if (SendUnlockRequestResult(context, seq_no, user_id, lock_type, obj_index,
          lock_result)) {
      cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
      return -1;
    }
  } else {
    // if context is NULL, it means it is a local workload
    this->NotifyUnlockRequestResult(
        seq_no,
        user_id,
        lock_type,
        obj_index,
        lock_result
        );
  }

  return 0;
}

//int LockManager::UnlockLocally(Context* context, int user_id, int lock_type,
    //int obj_index) {

  //// if current lock mode is remote (i.e. direct), then notify back.
  //if (current_lock_mode_ == LOCK_REMOTE) {
    //NotifyLockMode(context);
    //return -1;
  //}

  //int lock_result;
  //uint64_t* lock_object = (lock_table_+obj_index);
  //uint32_t exclusive, shared;
  //exclusive = (uint32_t)((*lock_object)>>32);
  //shared = (uint32_t)(*lock_object);

  //// lock locally on lock table
  //pthread_mutex_lock(lock_mutex_[obj_index]);

  //// unlocking shared lock
  //if (lock_type == LockManager::SHARED) {
    //if (shared > 0) {
      //--shared;
      //*lock_object = ((uint64_t)exclusive) << 32 | shared;
      //lock_result = LockManager::RESULT_SUCCESS;
    //} else {
     ////cerr << "client is trying to unlock shared lock with 0 counts." << endl;
      //lock_result = LockManager::RESULT_FAILURE;
    //}
  //} else if (lock_type == LockManager::EXCLUSIVE) {// if exclusive lock is requested
    //if (exclusive == user_id) {
      //exclusive = 0;
      //*lock_object = ((uint64_t)exclusive) << 32 | shared;
      //lock_result = LockManager::RESULT_SUCCESS;
    //} else {
      ////cerr << "client is trying to unlock exclusive lock," <<
        ////" which it does not own." << endl;
      //lock_result = LockManager::RESULT_FAILURE;
    //}
  //}

  //// unlock locally on lock table
  //pthread_mutex_unlock(lock_mutex_[obj_index]);

  //// send result back to the client
  //if (SendUnlockRequestResult(context, user_id, lock_type, obj_index,
        //lock_result)) {
    //cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
    //return -1;
  //}

  //return 0;
//}

int LockManager::TryLock(Context* context, Message* message) {
  int seq_no       = message->seq_no;
  uint32_t home_id = message->home_id;
  int lock_type    = message->lock_type;
  int obj_index    = message->obj_index;
  uint32_t user_id = message->user_id;
  LockClient* client = lock_clients_[MAX_USER*home_id+user_id];
  NotifyLockClient* notify_client = dynamic_cast<NotifyLockClient*>(client);
  if (notify_client == NULL) {
    cerr << "NotifyLockClient cast fail:" << rank_ << "," << home_id << "," << user_id <<endl;
    exit(-1);
    return -1;
  }

  // sends the ACK message back to CommunicationClient
  this->SendGrantLockAck(context, seq_no, user_id, lock_type, obj_index);

  return notify_client->TryLock(seq_no, user_id, lock_type, obj_index);
}


int LockManager::UnlockLocalDirect(uint32_t user_id, int lock_type, int obj_index) {
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

int LockManager::NotifyLockRequestResult(int seq_no, uint32_t user_id, int lock_type,
    int obj_index, int result) {
  LockSimulator* user = user_map[user_id];
  //pthread_mutex_lock(user_mutex_map[user_id]);
  user->NotifyResult(seq_no, LockManager::TASK_LOCK, lock_type, obj_index, result);
  //pthread_mutex_unlock(user_mutex_map[user_id]);
}

int LockManager::NotifyUnlockRequestResult(int seq_no, uint32_t user_id, int lock_type,
    int obj_index, int result) {
  LockSimulator* user = user_map[user_id];
  //pthread_mutex_lock(user_mutex_map[user_id]);
  user->NotifyResult(seq_no, LockManager::TASK_UNLOCK, lock_type, obj_index, result);
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

  Message* msg = context->send_message_buffer->GetMessage();
  struct ibv_mr* mr = context->send_message_buffer->GetMR();

  sge.addr   = (uint64_t)msg;
  sge.length = sizeof(*msg);
  sge.lkey   = mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  ++num_rdma_send_;
  context->send_message_buffer->Rotate();
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

  context->receive_message_buffer->Rotate();
  Message* msg = context->receive_message_buffer->GetMessage();
  struct ibv_mr* mr = context->receive_message_buffer->GetMR();
  //pthread_mutex_lock(&PRINT_MUTEX);
  //cout << pthread_self() << ", ReceiveMessage: " <<
    //context->receive_message_buffer->GetIndex() << ", " << msg->mr->lkey << endl;
  //pthread_mutex_unlock(&PRINT_MUTEX);

  sge.addr   = (uint64_t)msg;
  sge.length = sizeof(*msg);
  sge.lkey   = mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_recv(context->queue_pair, &receive_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_recv failed: " << strerror(ret) << endl;
    return -1;
  }
  ++num_rdma_recv_;

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
  attributes->cap.max_send_wr  = 64;
  attributes->cap.max_recv_wr  = 64;
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

    Message* message = context->receive_message_buffer->GetMessage();
    //context->receive_message_buffer->Rotate();
    // Post receive first.
    ReceiveMessage(context);

    // if client is requesting semaphore MR
    if (message->type == Message::LOCK_TABLE_MR_REQUEST) {
      SendLockTableMemoryRegion(context);
    } else if (message->type == Message::LOCK_REQUEST) {
      if (current_lock_mode_ == LOCK_PROXY_RETRY) {
        LockLocallyWithRetry(context, message);
      } else if (current_lock_mode_ == LOCK_PROXY_QUEUE) {
        LockLocallyWithQueue(context, message);
      } else {
        cerr << "Incompatible proxy lock mode: " << current_lock_mode_ << endl;
        return -1;
      }
    } else if (message->type == Message::UNLOCK_REQUEST) {
      if (current_lock_mode_ == LOCK_PROXY_RETRY) {
        UnlockLocallyWithRetry(context, message);
      } else if (current_lock_mode_ == LOCK_PROXY_QUEUE) {
        UnlockLocallyWithQueue(context, message);
      } else {
        cerr << "Incompatible proxy lock mode: " << current_lock_mode_ << endl;
        return -1;
      }
    } else if (message->type == Message::GRANT_LOCK) {
      if (current_lock_mode_ == LOCK_REMOTE_NOTIFY) {
        TryLock(context, message);
      } else if (current_lock_mode_ == LOCK_PROXY_QUEUE) {
        // sends the ACK message back to CommunicationClient
        this->SendGrantLockAck(context,
            message->seq_no,
            message->user_id,
            message->lock_type,
            message->obj_index);

        this->NotifyLockRequestResult(
            message->seq_no,
            message->user_id,
            message->lock_type,
            message->obj_index,
            RESULT_SUCCESS
            );
      } else {
        cerr << "Unknown lock mode for GrantLock Message: " << current_lock_mode_ << endl;
        return -1;
      }
    } else {
      cerr << "Unknown message type: " << message->type
        << endl;
      return -1;
    }
  }
}

int LockManager::RegisterContext(Context* context) {
  context_set_.insert(context);
  return 0;
}

uint64_t LockManager::GetTotalLockContention() const {
  uint64_t count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    count += it->second->GetNumLockContention();
  }
  return count;
}

uint64_t LockManager::GetTotalLockSuccessWithPoll() const {
  uint64_t count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    count += it->second->GetNumLockSuccessWithPoll();
  }
  return count;
}

uint64_t LockManager::GetTotalSumPollWhenSuccess() const {
  uint64_t count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    count += it->second->GetSumPollWhenSuccess();
  }
  return count;
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
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetAverageRemoteExclusiveLockTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageRemoteSharedLockTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetAverageRemoteSharedLockTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageSendMessageTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetAverageSendMessageTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageReceiveMessageTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetAverageReceiveMessageTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageRDMAReadCount() const {
  double num_clients = lock_clients_.size();
  double total_count = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_count += (double)it->second->GetRDMAReadCount();
  }
  return total_count / num_clients;
}

double LockManager::GetAverageRDMAAtomicCount() const {
  double num_clients = lock_clients_.size();
  double total_count = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_count += (double)it->second->GetRDMAAtomicCount();
  }
  return total_count / num_clients;
}

uint64_t LockManager::GetTotalRDMAReadCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_count += it->second->GetRDMAReadCount();
  }
  for (it2=communication_clients_.begin(); it2 != communication_clients_.end();++it2) {
    total_count += it2->second->GetRDMAReadCount();
  }
  return total_count;
}

uint64_t LockManager::GetTotalRDMARecvCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_count += it->second->GetRDMARecvCount();
  }
  for (it2=communication_clients_.begin(); it2 != communication_clients_.end();++it2) {
    total_count += it2->second->GetRDMARecvCount();
  }
  total_count += num_rdma_recv_;
  return total_count;
}

uint64_t LockManager::GetTotalRDMASendCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_count += it->second->GetRDMASendCount();
  }
  for (it2=communication_clients_.begin(); it2 != communication_clients_.end();++it2) {
    total_count += it2->second->GetRDMASendCount();
  }
  total_count += num_rdma_send_;
  return total_count;
}

uint64_t LockManager::GetTotalRDMAWriteCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_count += it->second->GetRDMAWriteCount();
  }
  for (it2=communication_clients_.begin(); it2 != communication_clients_.end();++it2) {
    total_count += it2->second->GetRDMAWriteCount();
  }
  return total_count;
}

uint64_t LockManager::GetTotalRDMAAtomicCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_count += it->second->GetRDMAAtomicCount();
  }
  for (it2=communication_clients_.begin(); it2 != communication_clients_.end();++it2) {
    total_count += it2->second->GetRDMAAtomicCount();
  }
  return total_count;
}

double LockManager::GetTotalRDMAReadTime() const {
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetTotalRDMAReadTime();
  }
  return total_time;
}

double LockManager::GetTotalRDMAAtomicTime() const {
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it=lock_clients_.begin(); it != lock_clients_.end();++it) {
    total_time += it->second->GetTotalRDMAAtomicTime();
  }
  return total_time;
}

// Polls work completion from completion queue
void* LockManager::PollCompletionQueue(void* arg) {
  struct ibv_cq* cq;
  struct ibv_cq* ev_cq;
  struct ibv_wc wc;
  Context* queue_context;
  Context* context = static_cast<Context*>(arg);
  cq = context->completion_queue;

  while (true) {
    if (ibv_get_cq_event(context->completion_channel, &ev_cq,
          (void**)&queue_context)) {
      cerr << "ibv_get_cq_event() failed." << endl;
    }
    ibv_ack_cq_events(ev_cq, 1);
    if (ibv_req_notify_cq(ev_cq, 0)) {
      cerr << "ibv_req_notify_cq() failed." << endl;
    }

    while (ibv_poll_cq(cq, 1, &wc)) {
      context->server->HandleWorkCompletion(&wc);
    }
  }

  return NULL;
}

void* LockManager::PollLocalWorkQueue(void* arg) {
  LockManager* manager = (LockManager*)arg;
  LocalWorkQueue<Message>* queue = manager->GetLocalWorkQueue();
  Message* work = new Message;
  while (queue->IsRun()) {
    Message m = queue->Remove();
    work->seq_no    = m.seq_no;
    work->user_id   = m.user_id;
    work->lock_type = m.lock_type;
    work->obj_index = m.obj_index;
    work->home_id   = m.home_id;
    int lock_mode = manager->GetCurrentLockMode();
    if (m.task == TASK_LOCK) {
      if (lock_mode == LOCK_PROXY_RETRY) {
        manager->LockLocallyWithRetry(NULL, work);
      } else if (lock_mode == LOCK_PROXY_QUEUE) {
        manager->LockLocallyWithQueue(NULL, work);
      } else {
        cerr << "Incompatible proxy lock mode: " << lock_mode << endl;
      }
    } else {
      if (lock_mode == LOCK_PROXY_RETRY) {
        manager->UnlockLocallyWithRetry(NULL, work);
      } else if (lock_mode == LOCK_PROXY_QUEUE) {
        manager->UnlockLocallyWithQueue(NULL, work);
      } else {
        cerr << "Incompatible proxy lock mode: " << lock_mode << endl;
      }
    }
  }
}

int LockManager::GetLockMode() const {
  return current_lock_mode_;
}

void* LockManager::RunLockClient(void* args) {
  Client* client = static_cast<Client*>(args);
  client->Run();
}


}} // end namespace
