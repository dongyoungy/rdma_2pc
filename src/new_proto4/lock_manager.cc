#include "lock_manager.h"
#include "communication_client.h"
#include "d2lm_lock_client.h"
#include "direct_queue_lock_client.h"
#include "direct_queue_lock_client_two.h"
#include "drtm_lock_client.h"
#include "lock_client.h"
#include "ncosed_lock_client.h"
#include "notify_lock_client.h"

namespace rdma {
namespace proto {

string LockManager::shared_exclusive_rule_ = "fail";
string LockManager::exclusive_shared_rule_ = "fail";
string LockManager::exclusive_exclusive_rule_ = "fail";
int LockManager::poll_retry_ = 3;
int LockManager::fail_retry_ = 3;
bool LockManager::is_atomic_hca_reply_be_ = false;
map<uint32_t, uint32_t> LockManager::user_to_node_map_;

// constructor
LockManager::LockManager(const string& work_dir, uint32_t rank, int num_manager,
                         int num_lock_object, LockMode lock_mode,
                         int num_total_user, int num_client) {
  work_dir_ = work_dir;
  rank_ = rank;
  // id_ = (uint32_t)pow(2.0, rank_);
  id_ = rank + 1;
  num_manager_ = num_manager;
  num_client_ = num_client;
  num_total_user_ = num_total_user;
  num_lock_object_ = num_lock_object;
  last_lock_table_ = new uint64_t[num_lock_object_];
  fail_count_ = new uint64_t[num_lock_object_];
  lock_mode_table_ = new LockMode[num_manager_];
  listener_ = NULL;
  event_channel_ = NULL;
  registered_memory_region_ = NULL;
  wait_queues_ = NULL;
  port_ = 0;
  lock_mode_ = lock_mode;
  total_local_exclusive_lock_time_ = 0;
  total_local_shared_lock_time_ = 0;
  num_local_exclusive_lock_ = 0;
  num_local_shared_lock_ = 0;
  num_local_lock_ = 0;
  num_remote_lock_ = 0;
  num_rdma_send_ = 0;
  num_rdma_recv_ = 0;
  terminate_ = false;
  // llm_ =
  // new LocalLockManager(rank_, num_manager_, num_lock_object_,
  // max_local_exclusive_locks, max_local_shared_locks);

  local_e_e_lock_pass_count_ = 0;

  num_local_lock_direct_pass_ = 0;
  num_local_lock_direct_fail_ = 0;
  num_local_lock_wait_pass_ = 0;
  num_local_lock_wait_fail_ = 0;

  request_lock_call_time_ = 0;
  request_lock_call_count_ = 0;

  current_lock_mode_ = lock_mode_;

  // initialize lock table with 0
  memset(last_lock_table_, 0x00, num_lock_object_ * sizeof(uint64_t));
  memset(fail_count_, 0x00, num_lock_object_ * sizeof(uint64_t));
  lock_table_ = new uint64_t[num_lock_object_];
  memset(lock_table_, 0x00, num_lock_object_ * sizeof(uint64_t));

  lock_mode_table_[rank_] = current_lock_mode_;
  // for (int i=0;i<num_manager_;++i) {
  //// every lock manager starts in remote mode.
  // lock_mode_table_[i] = LockManager::LOCK_REMOTE;
  //}

  // lock_table_[0] =
  //(uint64_t)5 << 48 | (uint64_t)4 << 32 | (uint64_t)3 << 16 | 2;
  // initialize local lock mutex
  lock_mutex_ = new pthread_mutex_t*[num_lock_object_];
  pthread_mutex_init(&msg_mutex_, NULL);
  pthread_mutex_init(&poll_mutex_, NULL);
  pthread_mutex_init(&seq_mutex_, NULL);
}

// destructor
LockManager::~LockManager() {
  if (lock_table_) delete[] lock_table_;

  if (lock_mode_table_) delete[] lock_mode_table_;

  if (wait_queues_) delete[] wait_queues_;
}

void LockManager::run() { this->Run(); }

int LockManager::GetID() const { return id_; }

int LockManager::GetRank() const { return rank_; }

int LockManager::Initialize() {
  for (int i = 0; i < num_lock_object_; ++i) {
    lock_mutex_[i] = new pthread_mutex_t;
    pthread_mutex_init(lock_mutex_[i], NULL);
  }
  memset(&address_, 0x00, sizeof(address_));
  address_.sin6_family = AF_INET6;

  event_channel_ = rdma_create_event_channel();
  if (event_channel_ == NULL) {
    cerr << "Run(): rdma_create_event_channel() failed: " << strerror(errno)
         << endl;
    return -1;
  }
  if (rdma_create_id(event_channel_, &listener_, NULL, RDMA_PS_TCP)) {
    cerr << "Run(): rdma_create_id() failed: " << strerror(errno) << endl;
    return -1;
  }
  if (rdma_bind_addr(listener_, (struct sockaddr*)&address_)) {
    cerr << "Run(): rdma_bind_addr() failed: " << strerror(errno) << endl;
    return -1;
  }
  if (rdma_listen(listener_, 2048)) {
    cerr << "Run(): rdma_listen() failed: " << strerror(errno) << endl;
    return -1;
  }
  port_ = ntohs(rdma_get_src_port(listener_));
  // cout << "LockManager " << rank_ <<  " listening on port " << port_ << endl;

  // print ip,port of lock manager in the working directory
  if (PrintInfo()) {
    cerr << "PrintInfo() error." << endl;
    return -1;
  }
  return 0;
}

void LockManager::SetTerminate(bool terminate) { terminate_ = terminate; }

int LockManager::Run() {
  struct rdma_cm_event* event = NULL;
  while (rdma_get_cm_event(event_channel_, &event) == 0 && !terminate_) {
    struct rdma_cm_event current_event;
    memcpy(&current_event, event, sizeof(current_event));
    rdma_ack_cm_event(event);
    if (HandleEvent(&current_event)) break;
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
  temp_lock_clients_ = new LockClient*[num_manager_ + 1];
  for (int i = 1; i <= num_manager_; ++i) {
    for (int j = 0; j < 1; ++j) {
      // for (unsigned int j = 0; j < users.size(); ++j) {
      LockSimulator* user = users[j];
      pthread_t* client_thread = new pthread_t;
      LockClient* client;
      if (lock_mode_ == REMOTE_NOTIFY)
        // client = new NotifyLockClient(work_dir_, this, users.size(), i);
        client = new NCOSEDLockClient(work_dir_, this, users.size(), i);
      else if (lock_mode_ == REMOTE_D2LM_V1)
        client = new DirectQueueLockClient(work_dir_, this, users.size(), i);
      else if (lock_mode_ == REMOTE_D2LM_V2)
        client = new D2LMLockClient(work_dir_, this, users.size(), i);
      else if (lock_mode_ == REMOTE_DRTM)
        client = new DRTMLockClient(work_dir_, this, users.size(), i);
      else
        client = new LockClient(work_dir_, this, users.size(), i);

      ret = pthread_create(client_thread, NULL, &LockManager::RunLockClient,
                           (void*)client);

      if (ret) {
        cout << "LockManager::pthread_create(): " << strerror(ret) << endl;
        return -1;
      }

      // lock_clients_[MAX_USER*i+user->GetID()] = client;
      lock_clients_[i] = client;
      temp_lock_clients_[i] = client;
      lock_client_threads_.push_back(client_thread);

      if (ret) {
        cout << "LockManager::pthread_create(): " << strerror(ret) << endl;
        return -1;
      }

      user_to_home_map_[(uintptr_t)user] = i;
    }

    CommunicationClient* comm_client =
        new CommunicationClient(work_dir_, this, users.size(), i);
    pthread_t* comm_client_thread = new pthread_t;
    ret = pthread_create(comm_client_thread, NULL, &LockManager::RunLockClient,
                         (void*)comm_client);

    if (ret) {
      cout << "LockManager::pthread_create(): " << strerror(ret) << endl;
      return -1;
    }
    communication_clients_[i] = comm_client;
    communication_client_threads_.push_back(comm_client_thread);
  }
  // initialize lock wait queues, one for each lock object
  if (lock_mode_ == PROXY_QUEUE || lock_mode_ == PROXY_RETRY) {
    wait_queues_ = new LockWaitQueue*[num_lock_object_];
    for (int i = 0; i < num_lock_object_; ++i) {
      wait_queues_[i] = new LockWaitQueue(MAX_LOCAL_THREADS);
    }
    // wait_queues_.reserve(num_lock_object_);
    // for (int i = 0; i < num_lock_object_; ++i) {
    // LockWaitQueue* queue = new LockWaitQueue(users.size()+1); // +36 is for
    // the buffer  wait_queues_.push_back(queue);
    //}
  }

  // initialize local work queue/poller.
  local_work_queue_ = new LocalWorkQueue<Message>;
  ret = pthread_create(&local_work_poller_, NULL,
                       &LockManager::PollLocalWorkQueue, (void*)this);
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
  if (sprintf(ip_filename, "%s/lm%04d.ip", work_dir_.c_str(), id_) < 0) {
    cerr << "PrintInfo(): sprintf() failed." << endl;
    return -1;
  }
  if (sprintf(port_filename, "%s/lm%04d.port", work_dir_.c_str(), id_) < 0) {
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
  // cout << "ip address: " << ip_address << endl;
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

  return FUNC_SUCCESS;
}

int LockManager::GetInfinibandIP(string& ip_address) {
  struct ifaddrs *ifaddr, *ifa;
  int s, n;
  char host[NI_MAXHOST];

  if (getifaddrs(&ifaddr) == -1) {
    cerr << "getifaddrs() error: " << strerror(errno) << endl;
    return -1;
  }

  /* Walk through linked list, maintaining head pointer so we
   *               can free list later */

  bool ip_found = false;
  for (ifa = ifaddr, n = 0; ifa != NULL; ifa = ifa->ifa_next, n++) {
    if (ifa->ifa_addr == NULL) continue;

    if (strncmp(ifa->ifa_name, "ib0", 3) == 0 &&
        ifa->ifa_addr->sa_family == AF_INET) {
      s = getnameinfo(ifa->ifa_addr, sizeof(*ifa->ifa_addr), host, NI_MAXHOST,
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
  if (listener_) rdma_destroy_id(listener_);
  if (event_channel_) rdma_destroy_event_channel(event_channel_);
}

void LockManager::Stop() {
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    it->second->Stop();
  }
  terminate_ = true;
  // DestroyListener();
  // exit(0);
}

// Currently the server registers same memory region for each client.
int LockManager::RegisterMemoryRegion(Context* context) {
  context->send_message_buffer.reset(new MessageBuffer);
  context->receive_message_buffer.reset(new MessageBuffer);

  context->lock_table = lock_table_;

  if (context->send_message_buffer->Register(context)) {
    cerr << "LockManager: MessageBuffer::Register failed()" << endl;
    return -1;
  }
  if (context->receive_message_buffer->Register(context)) {
    cerr << "LockManager: MessageBuffer::Register failed()" << endl;
    return -1;
  }

  size_t lock_table_size = num_lock_object_;

  context->lock_table_mr =
      ibv_reg_mr(context->protection_domain, context->lock_table,
                 lock_table_size * sizeof(uint64_t),
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

  // if (rdma_create_qp(id, context->protection_domain,
  // &queue_pair_attributes)){  cerr << "rdma_create_qp() failed: " <<
  // strerror(errno) << endl;  return -1;
  //}

  struct ibv_qp* queue_pair =
      ibv_exp_create_qp(id->verbs, &queue_pair_attributes);
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
      connection_parameters.responder_resources = 7;
  connection_parameters.rnr_retry_count = 7;

  // accept connection
  if (rdma_accept(id, &connection_parameters)) {
    cerr << "rdma_accept() failed: " << strerror(errno) << endl;
    return -1;
  }

  return 0;
}

int LockManager::HandleConnection(Context* context) {
  // cout << "Client connected." << endl;
  context->connected = true;
  context_set_.insert(context);
  // if lock mode == local (i.e., proxy, we disable atomic operations)
  if (current_lock_mode_ == PROXY_RETRY || current_lock_mode_ == PROXY_QUEUE) {
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

int LockManager::UpdateLockModeTable(int manager_id, LockMode mode) {
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
  // rdma_destroy_qp(context->id);

  context_set_.erase(context);

  if (context->lock_table_mr) ibv_dereg_mr(context->lock_table_mr);

  // rdma_destroy_id() also causes seg fault when client disconnects. why?
  // rdma_destroy_id(context->id);

  delete context;

  // cout << "client disconnected." << endl;
  return 0;
}

// Send local lock table memory region + current lock mode to client.
int LockManager::SendLockTableMemoryRegion(Context* context) {
  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();
  msg->type = Message::LOCK_TABLE_MR;
  msg->lock_mode = current_lock_mode_;
  msg->manager_id = id_;

  memcpy(&msg->lock_table_mr, context->lock_table_mr,
         sizeof(msg->lock_table_mr));
  if (SendMessage(context)) {
    cerr << "SendLockTableMemoryRegion(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&msg_mutex_);
  // cout << "SendLockTableMemoryRegion(): memory region sent." << endl;
  return 0;
}

// Send lock request result to client.
int LockManager::SendLockRequestResult(Context* context, int seq_no,
                                       uintptr_t owner_user_id,
                                       LockType lock_type, int obj_index,
                                       LockResult result) {
  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();

  msg->type = Message::LOCK_REQUEST_RESULT;
  msg->seq_no = seq_no;
  msg->target_node_id = id_;
  msg->owner_user_id = owner_user_id;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->lock_result = result;

  if (SendMessage(context)) {
    cerr << "SendLockRequestResult(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  // cout << "SendLockRequestResult(): memory region sent." << endl;
  pthread_mutex_unlock(&msg_mutex_);
  return 0;
}

// Send unlock request result to client.
int LockManager::SendUnlockRequestResult(Context* context, int seq_no,
                                         uintptr_t user_id, LockType lock_type,
                                         int obj_index, LockResult result) {
  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();

  msg->type = Message::UNLOCK_REQUEST_RESULT;
  msg->seq_no = seq_no;
  msg->target_node_id = id_;
  msg->owner_user_id = user_id;
  msg->lock_type = lock_type;
  msg->obj_index = obj_index;
  msg->lock_result = result;
  if (SendMessage(context)) {
    cerr << "SendUnlockRequestResult(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&msg_mutex_);
  // cout << "SendUnlockRequestResult(): memory region sent." << endl;
  return 0;
}

// Send unlock request result to client.
int LockManager::SendGrantLockAck(Context* context, int seq_no,
                                  uintptr_t user_id, LockType lock_type,
                                  int obj_index) {
  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();

  msg->type = Message::GRANT_LOCK_ACK;
  msg->seq_no = seq_no;
  msg->target_node_id = id_;
  msg->owner_user_id = user_id;
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
  return FUNC_SUCCESS;
}

int LockManager::NotifyLockMode(Context* context) {
  pthread_mutex_lock(&msg_mutex_);
  Message* msg = context->send_message_buffer->GetMessage();

  msg->type = Message::LOCK_MODE;
  msg->manager_id = id_;
  msg->lock_mode = current_lock_mode_;

  if (SendMessage(context)) {
    cerr << "SendUnlockRequestResult(): SendMessage() failed." << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }
  pthread_mutex_unlock(&msg_mutex_);
  return 0;
}

const Poco::Optional<std::promise<LockResultInfo>*> LockManager::Lock(
    const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(mutex_);
  lock_result_map_[request.user_id] = std::promise<LockResultInfo>();
  LockStatusInfo status(request.seq_no, request.user_id, LOCKING);
  lock_status_map_[request.lm_id][request.obj_index] = status;
  LockClient* lock_client = lock_clients_[request.lm_id];
  if (lock_client->RequestLock(request, lock_mode_table_[request.lm_id])) {
    return Poco::Optional<std::promise<LockResultInfo>*>(
        &lock_result_map_[request.user_id]);
  } else {
    return Poco::Optional<std::promise<LockResultInfo>*>();
  }
}

const Poco::Optional<std::promise<LockResultInfo>*> LockManager::Unlock(
    const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(mutex_);
  lock_result_map_[request.user_id] = std::promise<LockResultInfo>();
  LockStatusInfo status(request.seq_no, request.user_id, UNLOCKING);
  lock_status_map_[request.lm_id][request.obj_index] = status;
  LockClient* lock_client = lock_clients_[request.lm_id];
  if (lock_client->RequestUnlock(request, lock_mode_table_[request.lm_id])) {
    return Poco::Optional<std::promise<LockResultInfo>*>(
        &lock_result_map_[request.user_id]);
  } else {
    return Poco::Optional<std::promise<LockResultInfo>*>();
  }
}

int LockManager::SwitchToLocal() {
  current_lock_mode_ = LOCAL;
  NotifyLockModeAll();
  usleep(100000);
  DisableRemoteAtomicAccess();
  return 0;
}

int LockManager::SwitchToRemote() {
  current_lock_mode_ = REMOTE_POLL;
  NotifyLockModeAll();
  usleep(100000);
  EnableRemoteAtomicAccess();
  return 0;
}

int LockManager::GrantLock(int seq_no, int releasing_node_id,
                           int target_node_id, int owner_node_id,
                           LockType lock_type, int obj_index) {
  CommunicationClient* client = communication_clients_[owner_node_id];
  return client->GrantLock(seq_no, releasing_node_id, target_node_id, obj_index,
                           lock_type);
}

int LockManager::RejectLock(int seq_no, uintptr_t user_id, uint32_t home_id,
                            LockType lock_type, int obj_index) {
  CommunicationClient* client = communication_clients_[home_id];
  return client->RejectLock(seq_no, user_id, home_id, obj_index, lock_type);
}

int LockManager::LockLocallyWithRetry(Context* context, Message* message) {
  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ == REMOTE_POLL ||
      current_lock_mode_ == REMOTE_NOTIFY) {
    NotifyLockMode(context);
    return -1;
  }
  // get time
  clock_gettime(CLOCK_MONOTONIC, &start_local_lock_);

  LockResult lock_result = FAILURE;
  int seq_no = message->seq_no;
  uint32_t owner_node_id = message->owner_node_id;
  uint32_t owner_user_id = message->owner_user_id;
  int obj_index = message->obj_index;
  LockType lock_type = message->lock_type;
  uint32_t lock_value = (uint32_t)pow(2.0, owner_node_id);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  uint64_t* lock_object = (lock_table_ + obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object) >> 32);
  shared = (uint32_t)(*lock_object);

  // if shared lock is requested
  if (lock_type == SHARED) {
    if (exclusive == 0) {
      shared += lock_value;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    } else {
      lock_result = RETRY;
    }
  } else if (lock_type == EXCLUSIVE) {
    // if exclusive lock is requested
    if (exclusive == 0 && shared == 0) {
      exclusive = lock_value;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    } else {
      lock_result = RETRY;
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

  if (lock_type == SHARED) {
    total_local_shared_lock_time_ += time_taken;
    ++num_local_shared_lock_;
  } else if (lock_type == EXCLUSIVE) {
    total_local_exclusive_lock_time_ += time_taken;
    ++num_local_exclusive_lock_;
  }

  if (context) {
    // send result back to the client
    if (SendLockRequestResult(context, seq_no, owner_user_id, lock_type,
                              obj_index, lock_result)) {
      cerr << "LockLocally() failed." << endl;
      return -1;
    }
  } else {
    // if context is NULL, it means it is a local workload
    this->NotifyLockRequestResult(seq_no, owner_user_id, lock_type, id_,
                                  obj_index, 0, lock_result);
  }

  return 0;
}

int LockManager::LockLocallyWithQueue(Context* context, Message* message) {
  LockResult lock_result = FAILURE;
  int seq_no = message->seq_no;
  uint32_t target_node_id = id_;
  uint32_t owner_node_id = message->owner_node_id;
  uintptr_t owner_user_id = message->owner_user_id;
  int obj_index = message->obj_index;
  LockType lock_type = message->lock_type;
  uint32_t lock_value = message->owner_node_id;
  LockWaitQueue* queue = wait_queues_[obj_index];

  uint64_t* lock_object = NULL;

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  lock_object = (lock_table_ + obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object) >> 32);
  shared = (uint32_t)(*lock_object);

  if (exclusive == 0 && shared == 0 && queue->GetSize() > 0) {
    queue->RemoveAll();
  }

  if (queue->GetSize() > 0) {
    // there are some users already waiting...
    queue->Insert(seq_no, target_node_id, owner_node_id, owner_user_id,
                  lock_type);
    lock_result = QUEUED;
  } else {
    // if shared lock is requested
    if (lock_type == SHARED) {
      if (exclusive == 0) {
        shared += 1;
        *lock_object = ((uint64_t)exclusive) << 32 | shared;
        lock_result = SUCCESS;
      } else {
        queue->Insert(seq_no, target_node_id, owner_node_id, owner_user_id,
                      lock_type);
        lock_result = QUEUED;
      }
    } else if (lock_type == EXCLUSIVE) {
      // if exclusive lock is requested
      if (exclusive == 0 && shared == 0) {
        exclusive = lock_value;
        *lock_object = ((uint64_t)exclusive) << 32 | shared;
        lock_result = SUCCESS;
      } else if (exclusive == lock_value && shared == 0) {
        lock_result = SUCCESS;
      } else {
        queue->Insert(seq_no, target_node_id, owner_node_id, owner_user_id,
                      lock_type);
        lock_result = QUEUED;
      }
    }
  }

  pthread_mutex_unlock(lock_mutex_[obj_index]);
//// unlock locally on lock table

send_lock_result:

  // send result back to the client only if successful
  if (context) {
    // if (lock_result == SUCCESS) {
    if (SendLockRequestResult(context, seq_no, owner_user_id, lock_type,
                              obj_index, lock_result)) {
      cerr << "LockLocally() failed." << endl;
      return -1;
    }
    //}
  } else {
    // if context is NULL, it means it is a local workload
    if (lock_result == SUCCESS) {
      this->NotifyLockRequestResult(seq_no, owner_user_id, lock_type,
                                    id_,  // id_?
                                    obj_index, 0, lock_result);
    }
  }

  return 0;
}

// int LockManager::LockLocally(Context* context, int user_id, int lock_type,
// int obj_index) {

//// if current lock mode is remote (i.e. direct), then notify back.
// if (current_lock_mode_ == LOCK_REMOTE) {
// NotifyLockMode(context);
// return -1;
//}

//// get time
// clock_gettime(CLOCK_MONOTONIC, &start_local_lock_);

// int lock_result = LockManager::FAILURE;
// uint64_t* lock_object = (lock_table_+obj_index);
// uint32_t exclusive, shared;
// exclusive = (uint32_t)((*lock_object)>>32);
// shared = (uint32_t)(*lock_object);

//// lock locally on lock table
// pthread_mutex_lock(lock_mutex_[obj_index]);

//// if shared lock is requested
// if (lock_type == LockManager::SHARED) {
// if (exclusive == 0) {
//++shared;
//*lock_object = ((uint64_t)exclusive) << 32 | shared;
// lock_result = LockManager::SUCCESS;
//} else {
// lock_result = LockManager::FAILURE;
//}
//} else if (lock_type == LockManager::EXCLUSIVE) {
//// if exclusive lock is requested
// if (exclusive == 0 && shared == 0) {
// exclusive = user_id;
//*lock_object = ((uint64_t)exclusive) << 32 | shared;
// lock_result = LockManager::SUCCESS;
//} else {
// lock_result = LockManager::FAILURE;
//}
//}

//// unlock locally on lock table
// pthread_mutex_unlock(lock_mutex_[obj_index]);

//// get time
// clock_gettime(CLOCK_MONOTONIC, &end_local_lock_);
// double time_taken = ((double)end_local_lock_.tv_sec * 1e+9 +
//(double)end_local_lock_.tv_nsec) -
//((double)start_local_lock_.tv_sec * 1e+9 +
//(double)start_local_lock_.tv_nsec);

// if (lock_type == LockManager::SHARED) {
// total_local_shared_lock_time_ += time_taken;
//++num_local_shared_lock_;
//} else if (lock_type == LockManager::EXCLUSIVE) {
// total_local_exclusive_lock_time_ += time_taken;
//++num_local_exclusive_lock_;
//}

//// send result back to the client
// if (SendLockRequestResult(context, user_id, lock_type, obj_index,
// lock_result)) {
// cerr << "LockLocally() failed." << endl;
// return -1;
//}

// return 0;
//}

int LockManager::LockLocalDirect(uint32_t user_id, LockType lock_type,
                                 int obj_index) {
  LockResult lock_result = FAILURE;

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  uint64_t* lock_object = (lock_table_ + obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object) >> 32);
  shared = (uint32_t)(*lock_object);
  // if shared lock is requested
  if (lock_type == SHARED) {
    if (exclusive == 0) {
      ++shared;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    } else {
      lock_result = FAILURE;
    }
  } else if (lock_type == EXCLUSIVE) {
    // if exclusive lock is requested
    if (exclusive == 0 && shared == 0) {
      exclusive = user_id;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    } else {
      lock_result = FAILURE;
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  return lock_result;
}

int LockManager::UnlockLocallyWithRetry(Context* context, Message* message) {
  // if current lock mode is remote (i.e. direct), then notify back.
  if (current_lock_mode_ != PROXY_RETRY) {
    NotifyLockMode(context);
    return -1;
  }

  LockResult lock_result = FAILURE;
  int seq_no = message->seq_no;
  uintptr_t owner_node_id = message->owner_node_id;
  uint32_t owner_user_id = message->owner_user_id;
  int obj_index = message->obj_index;
  LockType lock_type = message->lock_type;
  uint32_t lock_value = (uint32_t)pow(2.0, owner_node_id);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  // get seq no from user
  int last_seq_no = last_seq_no_map_[owner_node_id][owner_user_id];
  // if seq no is less than last no.. ignore
  if (last_seq_no > seq_no) {
    pthread_mutex_unlock(lock_mutex_[obj_index]);
    return 0;
  } else {
    last_seq_no_map_[owner_node_id][owner_user_id] = seq_no;
  }
  uint64_t* lock_object = (lock_table_ + obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object) >> 32);
  shared = (uint32_t)(*lock_object);

  // unlocking shared lock
  if (lock_type == SHARED) {
    if ((shared & lock_value) != 0) {
      shared -= lock_value;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    } else {
      cerr << "client is trying to unlock shared lock it did not acquire."
           << endl;
      lock_result = FAILURE;
    }
  } else if (lock_type == EXCLUSIVE) {  // if exclusive lock is requested
    if (exclusive == lock_value) {
      exclusive = 0;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    } else {
      cerr << "client is trying to unlock exclusive lock,"
           << " which it does not own." << endl;
      lock_result = FAILURE;
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  // send result back to the client
  if (context) {
    if (SendUnlockRequestResult(context, seq_no, owner_user_id, lock_type,
                                obj_index, lock_result)) {
      cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
      return -1;
    }
  } else {
    // if context is NULL, it means it is a local workload
    this->NotifyUnlockRequestResult(seq_no, owner_user_id, lock_type, id_,
                                    obj_index, lock_result);
  }

  return 0;
}

int LockManager::UnlockLocallyWithQueue(Context* context, Message* message) {
  LockResult lock_result = FAILURE;
  int seq_no = message->seq_no;
  uint32_t owner_node_id = message->owner_node_id;
  uintptr_t owner_user_id = message->owner_user_id;
  int obj_index = message->obj_index;
  LockType lock_type = message->lock_type;
  LockWaitQueue* queue = wait_queues_[obj_index];
  uint32_t lock_value = message->owner_node_id;

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  uint64_t* lock_object = (lock_table_ + obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object) >> 32);
  shared = (uint32_t)(*lock_object);

  // remove it from the queue as well if exists
  int num_elem = queue->RemoveAllElements(owner_node_id, lock_type);
  if (num_elem > 0) {
    // unlock locally on lock table
    pthread_mutex_unlock(lock_mutex_[obj_index]);
    lock_result = SUCCESS;

    // send result back to the client
    if (context) {
      if (SendUnlockRequestResult(context, seq_no, owner_user_id, lock_type,
                                  obj_index, lock_result)) {
        cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
        return -1;
      }
    } else {
      // if context is NULL, it means it is a local workload
      this->NotifyUnlockRequestResult(seq_no, owner_user_id, lock_type, id_,
                                      obj_index, lock_result);
    }
    return 0;
  }

  // unlocking shared lock
  if (lock_type == SHARED) {
    if (shared > 0) {
      --shared;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    }
  } else if (lock_type == EXCLUSIVE) {  // if exclusive lock is requested
    if (exclusive == lock_value) {
      exclusive = 0;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    }
  }

  // Notify waiting users
  if (lock_result == SUCCESS) {
    LockWaitElement* elem = queue->Front();
    if (elem) {
      // CommunicationClient* client =
      // communication_clients_[MAX_USER*elem->home_id+elem->user_id];
      CommunicationClient* client = communication_clients_[elem->owner_node_id];
      lock_value = (uint32_t)elem->owner_node_id;
      exclusive = (uint32_t)((*lock_object) >> 32);
      shared = (uint32_t)(*lock_object);
      if (exclusive == 0 && elem->type == SHARED) {
        shared += 1;
        *lock_object = ((uint64_t)exclusive) << 32 | shared;
        client->GrantLock(elem->seq_no, elem->target_node_id,
                          elem->owner_user_id, obj_index, elem->type);
        queue->Pop();
        elem = queue->Front();
        while (elem && elem->type == SHARED) {
          shared += 1;
          *lock_object = ((uint64_t)exclusive) << 32 | shared;
          // client =
          // communication_clients_[MAX_USER*elem->home_id+elem->user_id];
          client = communication_clients_[elem->owner_node_id];
          client->GrantLock(elem->seq_no, elem->target_node_id,
                            elem->owner_user_id, obj_index, elem->type);
          queue->Pop();
          elem = queue->Front();
        }
      } else if (exclusive == 0 && shared == 0 && elem->type == EXCLUSIVE) {
        exclusive = (uint32_t)((*lock_object) >> 32);
        shared = (uint32_t)(*lock_object);
        exclusive = lock_value;
        *lock_object = ((uint64_t)exclusive) << 32 | shared;

        client->GrantLock(elem->seq_no, elem->target_node_id,
                          elem->owner_user_id, obj_index, elem->type);
        queue->Pop();
      }
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  // send result back to the client
  if (context) {
    if (SendUnlockRequestResult(context, seq_no, owner_user_id, lock_type,
                                obj_index, lock_result)) {
      cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
      return -1;
    }
  } else {
    // if context is NULL, it means it is a local workload
    this->NotifyUnlockRequestResult(seq_no, owner_user_id, lock_type, id_,
                                    obj_index, lock_result);
  }

  return 0;
}

// int LockManager::UnlockLocally(Context* context, int user_id, int lock_type,
// int obj_index) {

//// if current lock mode is remote (i.e. direct), then notify back.
// if (current_lock_mode_ == LOCK_REMOTE) {
// NotifyLockMode(context);
// return -1;
//}

// int lock_result;
// uint64_t* lock_object = (lock_table_+obj_index);
// uint32_t exclusive, shared;
// exclusive = (uint32_t)((*lock_object)>>32);
// shared = (uint32_t)(*lock_object);

//// lock locally on lock table
// pthread_mutex_lock(lock_mutex_[obj_index]);

//// unlocking shared lock
// if (lock_type == LockManager::SHARED) {
// if (shared > 0) {
//--shared;
//*lock_object = ((uint64_t)exclusive) << 32 | shared;
// lock_result = LockManager::SUCCESS;
//} else {
////cerr << "client is trying to unlock shared lock with 0 counts." << endl;
// lock_result = LockManager::FAILURE;
//}
//} else if (lock_type == LockManager::EXCLUSIVE) {// if exclusive lock is
// requested  if (exclusive == user_id) {  exclusive = 0; *lock_object =
//((uint64_t)exclusive) << 32 | shared;  lock_result =
// LockManager::SUCCESS;
//} else {
////cerr << "client is trying to unlock exclusive lock," <<
////" which it does not own." << endl;
// lock_result = LockManager::FAILURE;
//}
//}

//// unlock locally on lock table
// pthread_mutex_unlock(lock_mutex_[obj_index]);

//// send result back to the client
// if (SendUnlockRequestResult(context, user_id, lock_type, obj_index,
// lock_result)) {
// cerr << "UnlockLocally(): SendUnlockRequestResult() failed." << endl;
// return -1;
//}

// return 0;
//}

int LockManager::TryLock(Context* context, Message* message) {
  int seq_no = message->seq_no;
  uint32_t target_node_id = message->target_node_id;
  LockType lock_type = message->lock_type;
  int obj_index = message->obj_index;
  uintptr_t owner_user_id = message->owner_user_id;
  // LockClient* client = lock_clients_[MAX_USER*home_id+user_id];
  LockClient* client = lock_clients_[target_node_id];
  NotifyLockClient* notify_client = dynamic_cast<NotifyLockClient*>(client);
  if (notify_client == NULL) {
    cerr << "NotifyLockClient cast fail:" << id_ << "," << target_node_id << ","
         << owner_user_id << endl;
    exit(-1);
    return -1;
  }

  // sends the ACK message back to CommunicationClient
  this->SendGrantLockAck(context, seq_no, owner_user_id, lock_type, obj_index);

  return notify_client->TryLock(*message);
}

int LockManager::UnlockLocalDirect(uint32_t user_id, LockType lock_type,
                                   int obj_index) {
  int lock_result = FAILURE;
  uint64_t* lock_object = (lock_table_ + obj_index);
  uint32_t exclusive, shared;
  exclusive = (uint32_t)((*lock_object) >> 32);
  shared = (uint32_t)(*lock_object);

  // lock locally on lock table
  pthread_mutex_lock(lock_mutex_[obj_index]);

  // unlocking shared lock
  if (lock_type == SHARED) {
    if (shared > 0) {
      --shared;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    } else {
      // cerr << "client is trying to unlock shared lock with 0 counts." <<
      // endl;
      lock_result = FAILURE;
    }
  } else if (lock_type == EXCLUSIVE) {  // if exclusive lock is requested
    if (exclusive == user_id) {
      exclusive = 0;
      *lock_object = ((uint64_t)exclusive) << 32 | shared;
      lock_result = SUCCESS;
    } else {
      // cerr << "client is trying to unlock exclusive lock," <<
      //" which it does not own." << endl;
      lock_result = FAILURE;
    }
  }

  // unlock locally on lock table
  pthread_mutex_unlock(lock_mutex_[obj_index]);

  return lock_result;
}

std::promise<LockResultInfo>* LockManager::GetLockResult(uintptr_t user_id) {
  Poco::Mutex::ScopedLock lock(mutex_);
  return &lock_result_map_[user_id];
}

void LockManager::SetLockStatusInvalid(uint32_t node_id, uint32_t obj_index) {
  Poco::Mutex::ScopedLock lock(mutex_);
  lock_status_map_[node_id][obj_index].status = INVALID;
}

int LockManager::NotifyLockRequestResult(int seq_no, uintptr_t user_id,
                                         LockType lock_type, int target_node_id,
                                         int obj_index, int contention_count,
                                         LockResult result) {
  // if (lock_mode_ == LOCK_REMOTE_QUEUE || lock_mode_ == LOCK_REMOTE_NOTIFY) {
  // llm_->Lock(target_node_id, obj_index, user_id, lock_type, result);
  //}

  // LockSimulator* user = user_map[user_id];
  // user->NotifyResult(seq_no, LockManager::TASK_LOCK, lock_type, obj_index,
  // result);

  Poco::Mutex::ScopedLock lock(mutex_);
  LockResultInfo result_info(result, contention_count);
  auto& status = lock_status_map_[target_node_id][obj_index];
  if (status.seq_no == seq_no && status.status == LOCKING &&
      status.user_id == user_id) {
    lock_result_map_[user_id].set_value(result_info);
  } else {
#ifdef VERBOSE
    cout << "path9: incorrect lock status: " << id_ << "," << status.seq_no
         << "," << status.status << endl;
#endif
  }
  // lock_status_map_[target_node_id].erase(obj_index);

  if (result == QUEUED) {
    lock_result_map_[user_id] = std::promise<LockResultInfo>();
  }

  // keep the user id if it has been queued
  // if (result == RESULT_QUEUED) {
  // queued_user_[obj_index] = user_id;
  //}

  return 0;
}

int LockManager::NotifyLockRequestResult(int seq_no, uintptr_t user_id,
                                         LockType lock_type, int target_node_id,
                                         int obj_index, LockStat stat,
                                         LockResult result) {
  Poco::Mutex::ScopedLock lock(mutex_);
  LockResultInfo result_info(result, stat);
  auto& status = lock_status_map_[target_node_id][obj_index];
  if (status.seq_no == seq_no && status.status == LOCKING &&
      status.user_id == user_id) {
    lock_result_map_[user_id].set_value(result_info);
  }
  // lock_status_map_[target_node_id].erase(obj_index);

  if (result == QUEUED) {
    lock_result_map_[user_id] = std::promise<LockResultInfo>();
  }

  return 0;
}

int LockManager::NotifyUnlockRequestResult(int seq_no, uintptr_t user_id,
                                           LockType lock_type,
                                           int target_node_id, int obj_index,
                                           LockResult result) {
  Poco::Mutex::ScopedLock lock(mutex_);
  auto& status = lock_status_map_[target_node_id][obj_index];
  if (status.seq_no == seq_no && status.status == UNLOCKING &&
      status.user_id == user_id) {
    lock_result_map_[user_id].set_value(LockResultInfo(result, 0));
  } else {
    cerr << "Unlock failure. (5): " << seq_no << "," << id_ << ","
         << target_node_id << "," << obj_index << "," << user_id << endl;
    cerr << "Status = " << status.seq_no << "," << status.status << ","
         << status.user_id << endl;
    lock_result_map_[user_id].set_value(LockResultInfo(FAILURE, 0));
  }
  lock_status_map_[target_node_id].erase(obj_index);
  return 0;
}

int LockManager::SendMessage(Context* context) {
  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id = (uint64_t)context;
  send_work_request.opcode = IBV_WR_SEND;
  send_work_request.sg_list = &sge;
  send_work_request.num_sge = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  Message* msg = context->send_message_buffer->GetMessage();
  struct ibv_mr* mr = context->send_message_buffer->GetMR();

  sge.addr = (uint64_t)msg;
  sge.length = sizeof(*msg);
  sge.lkey = mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context->queue_pair, &send_work_request,
                           &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  ++num_rdma_send_;
  context->send_message_buffer->Rotate();
  // cout << "SendMessage(): message sent." << endl;

  return 0;
}

// Post receive to get message from clients
int LockManager::ReceiveMessage(Context* context) {
  struct ibv_recv_wr receive_work_request;
  struct ibv_recv_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&receive_work_request, 0x00, sizeof(receive_work_request));

  receive_work_request.wr_id = (uint64_t)context;
  receive_work_request.next = NULL;
  receive_work_request.sg_list = &sge;
  receive_work_request.num_sge = 1;

  pthread_mutex_lock(&msg_mutex_);

  context->receive_message_buffer->Rotate();
  Message* msg = context->receive_message_buffer->GetMessage();
  struct ibv_mr* mr = context->receive_message_buffer->GetMR();
  // pthread_mutex_lock(&PRINT_MUTEX);
  // cout << pthread_self() << ", ReceiveMessage: " <<
  // context->receive_message_buffer->GetIndex() << endl;
  // pthread_mutex_unlock(&PRINT_MUTEX);

  sge.addr = (uint64_t)msg;
  sge.length = sizeof(*msg);
  sge.lkey = mr->lkey;

  int ret = 0;
  // cout << "ibv_post_recv()" << endl;
  if ((ret = ibv_post_recv(context->queue_pair, &receive_work_request,
                           &bad_work_request))) {
    cerr << "ibv_post_recv failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }
  ++num_rdma_recv_;
  pthread_mutex_unlock(&msg_mutex_);

  return 0;
}

// Builds queue pair attributes
void LockManager::BuildQueuePairAttr(Context* context,
                                     struct ibv_exp_qp_init_attr* attributes) {
  memset(attributes, 0x00, sizeof(*attributes));

  struct ibv_device_attr device_attr;
  int ret = 0;
  if ((ret = ibv_query_device(context->device_context, &device_attr))) {
    cerr << "LockManager::ibv_exp_query_device() failed: " << strerror(ret)
         << endl;
    exit(-1);
  }

  attributes->pd = context->protection_domain;
  attributes->send_cq = context->completion_queue;
  attributes->recv_cq = context->completion_queue;
  attributes->qp_type = IBV_QPT_RC;
  attributes->cap.max_send_wr = 4096;
  attributes->cap.max_recv_wr = 4096;
  attributes->cap.max_send_sge = 4;
  attributes->cap.max_recv_sge = 4;
  if (device_attr.atomic_cap == IBV_ATOMIC_HCA ||
      device_attr.atomic_cap == IBV_ATOMIC_GLOB) {
    attributes->comp_mask = IBV_EXP_QP_INIT_ATTR_PD;
    is_atomic_hca_reply_be_ = false;
    //} else if (device_attr.exp_atomic_cap == IBV_EXP_ATOMIC_HCA_REPLY_BE) {
    // attributes->comp_mask =
    // IBV_EXP_QP_INIT_ATTR_PD | IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS;
    // attributes->max_atomic_arg = sizeof(uint64_t);
    // attributes->exp_create_flags = IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY;
    // is_atomic_hca_reply_be_ = true;
  } else {
    cerr << "atomic operation not supported: " << device_attr.atomic_cap
         << endl;
  }
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
           ibv_create_cq(new_context->device_context, 64, NULL,
                         new_context->completion_channel, 0)) == NULL) {
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
  Context* context = (Context*)work_completion->wr_id;

  if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "(LockManager) Work completion status is not IBV_WC_SUCCESS."
         << endl;
    return -1;
  }

  if (work_completion->opcode & IBV_WC_RECV) {
    Message* message = context->receive_message_buffer->GetMessage();
    // Post receive first.
    ReceiveMessage(context);

    // if client is requesting semaphore MR
    if (message->type == Message::LOCK_TABLE_MR_REQUEST) {
      SendLockTableMemoryRegion(context);
    } else if (message->type == Message::LOCK_REQUEST) {
      if (current_lock_mode_ == PROXY_RETRY) {
        LockLocallyWithRetry(context, message);
      } else if (current_lock_mode_ == PROXY_QUEUE) {
        LockLocallyWithQueue(context, message);
      } else {
        cerr << "Incompatible proxy lock mode: " << current_lock_mode_ << endl;
        return FUNC_FAIL;
      }
    } else if (message->type == Message::UNLOCK_REQUEST) {
      if (current_lock_mode_ == PROXY_RETRY) {
        UnlockLocallyWithRetry(context, message);
      } else if (current_lock_mode_ == PROXY_QUEUE) {
        UnlockLocallyWithQueue(context, message);
      } else {
        cerr << "Incompatible proxy lock mode: " << current_lock_mode_ << endl;
        return FUNC_FAIL;
      }
    } else if (message->type == Message::GRANT_LOCK) {
      if (current_lock_mode_ == REMOTE_NOTIFY) {
        TryLock(context, message);
      } else if (current_lock_mode_ == PROXY_QUEUE) {
        // sends the ACK message back to CommunicationClient
        this->SendGrantLockAck(context, message->seq_no, message->owner_user_id,
                               message->lock_type, message->obj_index);

        this->NotifyLockRequestResult(
            message->seq_no, message->owner_user_id, message->lock_type,
            message->target_node_id, message->obj_index, 0,
            SUCCESS_FROM_QUEUED);
      } else {
        cerr << "Unknown lock mode for GrantLock Message: "
             << current_lock_mode_ << endl;
        return FUNC_FAIL;
      }
    } else if (message->type == Message::NCOSED_LOCK_REQUEST) {
      this->HandleNCOSEDLockRequest(*message);
    } else if (message->type == Message::NCOSED_LOCK_GRANT) {
      this->HandleNCOSEDLockGrant(*message);
    } else if (message->type == Message::NCOSED_LOCK_RELEASE) {
      this->HandleNCOSEDLockRelease(*message);
    } else if (message->type == Message::NCOSED_LOCK_RELEASE_SUCCESS) {
      this->HandleNCOSEDLockReleaseSuccess(*message);
    } else {
      cerr << "Unknown message type: " << message->type << endl;
      return FUNC_FAIL;
    }
  }
  return FUNC_SUCCESS;
}

int LockManager::RegisterContext(Context* context) {
  context_set_.insert(context);
  return 0;
}

uint64_t LockManager::GetNumLocalLockDirectPass() const {
  return num_local_lock_direct_pass_;
}

uint64_t LockManager::GetNumLocalLockDirectFail() const {
  return num_local_lock_direct_fail_;
}

uint64_t LockManager::GetNumLocalLockWaitPass() const {
  return num_local_lock_wait_pass_;
}

uint64_t LockManager::GetNumLocalLockWaitFail() const {
  return num_local_lock_wait_fail_;
}

uint64_t LockManager::GetRequestLockCallTime() const {
  return request_lock_call_time_;
}

uint64_t LockManager::GetRequestLockCallCount() const {
  return request_lock_call_count_;
}

uint64_t LockManager::GetTotalLockContention() const {
  uint64_t count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    count += it->second->GetNumLockContention();
  }
  return count;
}

uint64_t LockManager::GetTotalLockSuccessWithPoll() const {
  uint64_t count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    count += it->second->GetNumLockSuccessWithPoll();
  }
  return count;
}

uint64_t LockManager::GetTotalSumPollWhenSuccess() const {
  uint64_t count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    count += it->second->GetSumPollWhenSuccess();
  }
  return count;
}

double LockManager::GetAverageLocalSharedLockTime() const {
  return (num_local_shared_lock_ > 0)
             ? total_local_shared_lock_time_ / num_local_shared_lock_
             : 0;
}

double LockManager::GetAverageLocalExclusiveLockTime() const {
  return (num_local_exclusive_lock_ > 0)
             ? total_local_exclusive_lock_time_ / num_local_exclusive_lock_
             : 0;
}

double LockManager::GetAverageRemoteExclusiveLockTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_time += it->second->GetAverageRemoteExclusiveLockTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageRemoteSharedLockTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_time += it->second->GetAverageRemoteSharedLockTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageSendMessageTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_time += it->second->GetAverageSendMessageTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageReceiveMessageTime() const {
  double num_clients = lock_clients_.size();
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_time += it->second->GetAverageReceiveMessageTime();
  }
  return total_time / num_clients;
}

double LockManager::GetAverageRDMAReadCount() const {
  double num_clients = lock_clients_.size();
  double total_count = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_count += (double)it->second->GetRDMAReadCount();
  }
  return total_count / num_clients;
}

double LockManager::GetAverageRDMAAtomicCount() const {
  double num_clients = lock_clients_.size();
  double total_count = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_count += (double)it->second->GetRDMAAtomicCount();
  }
  return total_count / num_clients;
}

uint64_t LockManager::GetTotalRDMAReadCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_count += it->second->GetRDMAReadCount();
  }
  for (it2 = communication_clients_.begin();
       it2 != communication_clients_.end(); ++it2) {
    total_count += it2->second->GetRDMAReadCount();
  }
  return total_count;
}

uint64_t LockManager::GetTotalRDMARecvCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_count += it->second->GetRDMARecvCount();
  }
  for (it2 = communication_clients_.begin();
       it2 != communication_clients_.end(); ++it2) {
    total_count += it2->second->GetRDMARecvCount();
  }
  total_count += num_rdma_recv_;
  return total_count;
}

uint64_t LockManager::GetTotalRDMASendCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_count += it->second->GetRDMASendCount();
  }
  for (it2 = communication_clients_.begin();
       it2 != communication_clients_.end(); ++it2) {
    total_count += it2->second->GetRDMASendCount();
  }
  total_count += num_rdma_send_;
  return total_count;
}

uint64_t LockManager::GetTotalRDMAWriteCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_count += it->second->GetRDMAWriteCount();
  }
  for (it2 = communication_clients_.begin();
       it2 != communication_clients_.end(); ++it2) {
    total_count += it2->second->GetRDMAWriteCount();
  }
  return total_count;
}

uint64_t LockManager::GetTotalRDMAAtomicCount() const {
  uint64_t total_count = 0;
  map<uint64_t, LockClient*>::const_iterator it;
  map<uint64_t, CommunicationClient*>::const_iterator it2;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_count += it->second->GetRDMAAtomicCount();
  }
  for (it2 = communication_clients_.begin();
       it2 != communication_clients_.end(); ++it2) {
    total_count += it2->second->GetRDMAAtomicCount();
  }
  return total_count;
}

double LockManager::GetTotalRDMAReadTime() const {
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_time += it->second->GetTotalRDMAReadTime();
  }
  return total_time;
}

double LockManager::GetTotalRDMAAtomicTime() const {
  double total_time = 0.0;
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    total_time += it->second->GetTotalRDMAAtomicTime();
  }
  return total_time;
}

bool LockManager::IsClientsInitialized() const {
  map<uint64_t, LockClient*>::const_iterator it;
  for (it = lock_clients_.begin(); it != lock_clients_.end(); ++it) {
    if (!it->second->IsInitialized()) {
      return false;
    }
  }
  return true;
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
    work->seq_no = m.seq_no;
    work->owner_user_id = m.owner_user_id;
    work->owner_node_id = m.owner_node_id;
    work->lock_type = m.lock_type;
    work->obj_index = m.obj_index;
    work->target_node_id = m.target_node_id;
    LockMode lock_mode = manager->GetCurrentLockMode();
    if (m.task == LOCK) {
      if (lock_mode == PROXY_RETRY) {
        manager->LockLocallyWithRetry(NULL, work);
      } else if (lock_mode == PROXY_QUEUE) {
        manager->LockLocallyWithQueue(NULL, work);
      } else {
        cerr << "Incompatible proxy lock mode: " << lock_mode << endl;
      }
    } else {
      if (lock_mode == PROXY_RETRY) {
        manager->UnlockLocallyWithRetry(NULL, work);
      } else if (lock_mode == PROXY_QUEUE) {
        manager->UnlockLocallyWithQueue(NULL, work);
      } else {
        cerr << "Incompatible proxy lock mode: " << lock_mode << endl;
      }
    }
  }
  return NULL;
}

void LockManager::ResetSharedReleaseCount(int node_id, int obj_index) {
  Poco::Mutex::ScopedLock lock(mutex_);
  shared_release_count_map_[node_id].erase(obj_index);
  shared_remaining_map_[node_id].erase(obj_index);
}

int LockManager::SendNCOSEDLockRequest(int seq_no, int current_owner_id,
                                       int node_id, int obj_index,
                                       int request_node_id,
                                       uintptr_t request_user_id,
                                       int shared_remaining,
                                       LockType lock_type) {
  Poco::Mutex::ScopedLock lock(mutex_);
  LockClient* lock_client = lock_clients_[current_owner_id];

  if (!lock_client->SendNCOSEDLockRequest(seq_no, node_id, obj_index,
                                          request_node_id, request_user_id,
                                          shared_remaining, lock_type)) {
    return FUNC_FAIL;
  }

  return FUNC_SUCCESS;
}

int LockManager::SendNCOSEDLockRelease(const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(mutex_);
  if (request.lock_type == EXCLUSIVE) {
    cerr << "LockManager::SendNCOSEDLockRelease(): must be shared." << endl;
    exit(ERROR_INVALID_LOCK_TYPE);
  }
  LockClient* lock_client = lock_clients_[request.lm_id];
  lock_client->SendNCOSEDLockRelease(request);
  // cout << "LM " << id_ << " sent lock release to " << request.lm_id << endl;
  return FUNC_SUCCESS;
}

int LockManager::SendNCOSEDLockReleaseSuccess(const LockRequest& request) {
  Poco::Mutex::ScopedLock lock(mutex_);
  if (request.lock_type == EXCLUSIVE) {
    cerr << "LockManager::SendNCOSEDLockRelease(): must be shared." << endl;
    exit(ERROR_INVALID_LOCK_TYPE);
  }
  LockClient* lock_client = lock_clients_[request.owner_node_id];
  lock_client->SendNCOSEDLockReleaseSuccess(request);
  // cout << "LM " << id_ << " sent lock release success to "
  //<< request.owner_node_id << endl;
  return FUNC_SUCCESS;
}

int LockManager::HandleNCOSEDLockGrant(const Message& msg) {
  Poco::Mutex::ScopedLock lock(mutex_);
  LockResultInfo result_info(SUCCESS, 0);
  auto& status = lock_status_map_[msg.node_id][msg.obj_index];
#ifdef VERBOSE
  if (id_ == 3) {
    cout << "path8: HandleNCOSEDLockGrant(): " << id_ << "," << msg.seq_no
         << "," << msg.node_id << "," << msg.obj_index << endl;
    cout << "path8: " << getpid() << endl;
  }
#endif
  if (status.seq_no == msg.seq_no && status.status == LOCKING &&
      status.user_id == msg.owner_user_id) {
    lock_result_map_[msg.owner_user_id].set_value(result_info);
  } else {
#ifdef VERBOSE
    if (id_ == 3) {
      cout << "path8-1: incorrect lock status: " << id_ << "," << status.seq_no
           << "," << status.status << endl;
    }
#endif
  }
  return FUNC_SUCCESS;
}

int LockManager::HandleNCOSEDLockRelease(const Message& msg) {
  Poco::Mutex::ScopedLock lock(mutex_);
  ++shared_release_count_map_[msg.node_id][msg.obj_index];
  --shared_remaining_map_[msg.node_id][msg.obj_index];

  if (shared_remaining_map_[msg.node_id][msg.obj_index] == 0) {
    this->SendNCOSEDLockGrant(msg.node_id, msg.obj_index);
  }
  NCOSEDLockClient* client =
      static_cast<NCOSEDLockClient*>(lock_clients_[msg.node_id]);
  client->UnlockShared(msg,
                       shared_release_count_map_[msg.node_id][msg.obj_index]);

  return FUNC_SUCCESS;
}

int LockManager::HandleNCOSEDLockReleaseSuccess(const Message& msg) {
  Poco::Mutex::ScopedLock lock(mutex_);
  LockType type = msg.lock_type;
  if (msg.lock_type == BOTH) {
    type = SHARED;
  }
  this->ResetSharedReleaseCount(msg.node_id, msg.obj_index);
  this->NotifyUnlockRequestResult(msg.seq_no, msg.owner_user_id, type,
                                  msg.node_id, msg.obj_index, SUCCESS);
  // cout << "LM " << id_ << " received lock release success for " << msg.seq_no
  //<< endl;

  return FUNC_SUCCESS;
}

int LockManager::SendNCOSEDLockGrant(int node_id, int obj_index) {
  Poco::Mutex::ScopedLock lock(mutex_);
  if (node_id != 1) {
    cerr << "WRONG1" << endl;
    exit(-1);
  }
#if VERBOSE
  cout << "path7: " << next_seq_no_map_[node_id][obj_index] << "," << node_id
       << "," << obj_index << "," << next_node_id_map_[node_id][obj_index]
       << "," << next_user_id_map_[node_id][obj_index] << ","
       << shared_remaining_map_[node_id][obj_index] << endl;
#endif
  if (next_node_id_map_[node_id][obj_index] != 0) {
#if VERBOSE
    cout << "path7-1: " << next_seq_no_map_[node_id][obj_index] << ","
         << node_id << "," << obj_index << ","
         << next_node_id_map_[node_id][obj_index] << ","
         << next_user_id_map_[node_id][obj_index] << ","
         << shared_remaining_map_[node_id][obj_index] << endl;
#endif
    LockClient* lock_client =
        lock_clients_[next_node_id_map_[node_id][obj_index]];
    lock_client->SendNCOSEDLockGrant(next_seq_no_map_[node_id][obj_index],
                                     next_user_id_map_[node_id][obj_index],
                                     node_id, obj_index,
                                     next_lock_type_map_[node_id][obj_index]);
    next_seq_no_map_[node_id].erase(obj_index);
    next_node_id_map_[node_id].erase(obj_index);
    next_user_id_map_[node_id].erase(obj_index);
    next_lock_type_map_[node_id].erase(obj_index);
    shared_remaining_map_[node_id].erase(obj_index);
    shared_release_count_map_[node_id].erase(obj_index);
    lock_grant_map_[node_id].erase(obj_index);
  } else {
    lock_grant_map_[node_id][obj_index] = true;
  }
  return FUNC_SUCCESS;
}

int LockManager::HandleNCOSEDLockRequest(const Message& msg) {
  Poco::Mutex::ScopedLock lock(mutex_);
  // Set information necessary to pass the lock first.
  next_seq_no_map_[msg.node_id][msg.obj_index] = msg.seq_no;
  next_node_id_map_[msg.node_id][msg.obj_index] = msg.request_node_id;
  next_user_id_map_[msg.node_id][msg.obj_index] = msg.request_user_id;
  next_lock_type_map_[msg.node_id][msg.obj_index] = msg.lock_type;
  shared_remaining_map_[msg.node_id][msg.obj_index] += msg.shared_remaining;

#if VERBOSE
  cout << "path6: " << id_ << "," << msg.seq_no << "," << msg.node_id << ","
       << msg.obj_index << "," << msg.request_node_id << ","
       << msg.request_user_id << ","
       << shared_remaining_map_[msg.node_id][msg.obj_index] << ","
       << msg.shared_remaining << ","
       << (lock_grant_map_[msg.node_id][msg.obj_index] ? "TRUE" : "FALSE")
       << endl;
#endif
  // Send lock grant message if it is okay to do so.
  if (lock_grant_map_[msg.node_id][msg.obj_index]) {
    switch (msg.lock_type) {
      case EXCLUSIVE: {
        if (shared_remaining_map_[msg.node_id][msg.obj_index] == 0) {
#if VERBOSE
          cout << "path6-1: " << msg.seq_no << "," << msg.node_id << ","
               << msg.obj_index << "," << msg.request_node_id << ","
               << msg.request_user_id << ","
               << shared_remaining_map_[msg.node_id][msg.obj_index] << ","
               << msg.shared_remaining << endl;
#endif
          this->SendNCOSEDLockGrant(msg.node_id, msg.obj_index);
        }
        break;
      }
      case SHARED: {
#if VERBOSE
        cout << "path6-2: " << msg.seq_no << "," << msg.node_id << ","
             << msg.obj_index << "," << msg.request_node_id << ","
             << msg.request_user_id << ","
             << shared_remaining_map_[msg.node_id][msg.obj_index] << ","
             << msg.shared_remaining << endl;
#endif
        this->SendNCOSEDLockGrant(msg.node_id, msg.obj_index);
        break;
      }
      default: {
        cerr << "LockManager::HandleNCOSEDLockRequest(): Invalid lock type: "
             << msg.lock_type << endl;
        exit(ERROR_INVALID_LOCK_TYPE);
        break;
      }
    }
  }
  return FUNC_SUCCESS;
}

LockMode LockManager::GetLockMode() const { return current_lock_mode_; }

void* LockManager::RunLockClient(void* args) {
  Client* client = static_cast<Client*>(args);
  client->Run();
  return NULL;
}

}  // namespace proto
}  // namespace rdma
