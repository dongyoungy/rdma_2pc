#include "lock_client.h"
#include "lock_request.h"

namespace rdma {
namespace proto {

// constructor
Client::Client(const string& work_dir, LockManager* local_manager,
               uint32_t local_user_count, uint32_t remote_lm_id) {
  work_dir_ = work_dir;
  context_ = NULL;
  event_channel_ = NULL;
  connection_ = NULL;
  address_ = NULL;
  local_manager_ = local_manager;
  local_user_count_ = local_user_count;
  remote_lm_id_ = remote_lm_id;
  initialized_ = false;
  terminate_ = false;
  total_exclusive_lock_remote_time_ = 0;
  total_shared_lock_remote_time_ = 0;
  total_send_message_time_ = 0;
  total_receive_message_time_ = 0;
  total_rdma_atomic_time_ = 0;
  total_rdma_read_time_ = 0;
  total_lock_success_ = 0;
  total_lock_success_with_poll_ = 0;
  total_lock_contention_ = 0;
  sum_poll_when_success_ = 0;

  num_exclusive_lock_ = 0;
  num_shared_lock_ = 0;
  num_send_message_ = 0;
  num_receive_message_ = 0;
  num_rdma_send_ = 0;
  num_rdma_recv_ = 0;
  num_rdma_write_ = 0;
  num_rdma_read_ = 0;
  num_rdma_atomic_ = 0;

  local_owner_id_ = local_manager_->GetRank();
  local_owner_bitvector_id_ = local_manager_->GetID();

  // initialize local lock mutex
  pthread_mutex_init(&msg_mutex_, NULL);
  pthread_mutex_init(&mutex_, NULL);
  pthread_cond_init(&lock_cond_, NULL);

  lock_requests_.reserve(MAX_LOCAL_THREADS);
  for (int i = 0; i < MAX_LOCAL_THREADS; ++i) {
    std::unique_ptr<LockRequest> request(new LockRequest);
    lock_requests_.push_back(std::move(request));
  }
  lock_request_idx_ = 0;
}

// destructor
Client::~Client() {}

Context* Client::GetContext() { return context_; }

int Client::Run() {
  // read server address from file
  if (ReadServerAddress()) {
    cerr << "Run(): ReadServerAddress() failed" << endl;
    return -1;
  }

  // cout << "connecting to server: " << server_name_ << ":" << server_port_
  //<< endl;
  int ret = 0;
  if ((ret = getaddrinfo(server_name_.c_str(), server_port_.c_str(), NULL,
                         &address_))) {
    cerr << "Run(): getaddrinfo() failed: " << gai_strerror(ret) << endl;
    return -1;
  }

  event_channel_ = rdma_create_event_channel();
  if (event_channel_ == NULL) {
    cerr << "Run(): rdma_create_event_channel() failed: " << strerror(errno)
         << endl;
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
  while (rdma_get_cm_event(event_channel_, &event) == 0 && !terminate_) {
    struct rdma_cm_event current_event;
    memcpy(&current_event, event, sizeof(current_event));
    rdma_ack_cm_event(event);
    if (HandleEvent(&current_event)) break;
  }

  rdma_destroy_event_channel(event_channel_);

  return 0;
}

void Client::Stop() {
  terminate_ = true;
  // exit(0);
}

int Client::ReadServerAddress() {
  char ip[64];
  char port[16];

  // open files
  char ip_filename[256];
  char port_filename[256];
  if (sprintf(ip_filename, "%s/lm%04d.ip", work_dir_.c_str(), remote_lm_id_) <
      0) {
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

int Client::HandleEvent(struct rdma_cm_event* event) {
  int ret = 0;
  pthread_mutex_lock(&mutex_);
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
  pthread_mutex_unlock(&mutex_);
  return ret;
}

int Client::HandleAddressResolved(struct rdma_cm_id* id) {
  context_ = BuildContext(id);
  if (context_ == NULL) {
    cerr << "Client: BuildContext() failed." << endl;
    exit(-1);
  }

  struct ibv_exp_qp_init_attr queue_pair_attributes;
  memset(&queue_pair_attributes, 0x00, sizeof(queue_pair_attributes));
  BuildQueuePairAttr(context_, &queue_pair_attributes);

  // if (rdma_create_qp(id, context_->protection_domain,
  //&queue_pair_attributes)) {
  // cerr << "rdma_create_qp() failed: " << strerror(errno) << endl;
  // return -1;
  //}

  struct ibv_qp* queue_pair =
      ibv_exp_create_qp(id->verbs, &queue_pair_attributes);
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

  return 0;
}

int Client::HandleRouteResolved(struct rdma_cm_id* id) {
  struct rdma_conn_param connection_parameters;
  memset(&connection_parameters, 0x00, sizeof(connection_parameters));
  connection_parameters.initiator_depth =
      connection_parameters.responder_resources = 7;
  connection_parameters.rnr_retry_count = 7;

  // connect
  if (rdma_connect(id, &connection_parameters)) {
    cerr << "rdma_connect() failed: " << strerror(errno) << endl;
    return -1;
  }

  return 0;
}

int Client::HandleConnection(Context* context) {
  context->connected = true;

  return 0;
}

int Client::HandleDisconnect(Context* context) {
  if (context->original_value_mr) ibv_dereg_mr(context->original_value_mr);

  delete context;

  return 0;
}

int Client::SendMessage(Context* context) {
  clock_gettime(CLOCK_MONOTONIC, &start_send_message_);

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  send_work_request.wr_id = (uint64_t)context;
  send_work_request.opcode = IBV_WR_SEND;
  send_work_request.sg_list = &sge;
  send_work_request.num_sge = 1;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  pthread_mutex_lock(&msg_mutex_);

  Message* msg = context->send_message_buffer->GetMessage();
  struct ibv_mr* mr = context->send_message_buffer->GetMR();

  sge.addr = (uint64_t)msg;
  sge.length = sizeof(*msg);
  sge.lkey = mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_send(context->queue_pair, &send_work_request,
                           &bad_work_request))) {
    cerr << "ibv_post_send() failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  context->send_message_buffer->Rotate();
  ++num_send_message_;
  ++num_rdma_send_;
  pthread_mutex_unlock(&msg_mutex_);

  return ret;
}

// Post receive to get message from clients
int Client::ReceiveMessage(Context* context) {
  clock_gettime(CLOCK_MONOTONIC, &start_receive_message_);

  struct ibv_recv_wr receive_work_request;
  struct ibv_recv_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&receive_work_request, 0x00, sizeof(receive_work_request));

  receive_work_request.wr_id = (uint64_t)context;
  receive_work_request.next = NULL;
  receive_work_request.sg_list = &sge;
  receive_work_request.num_sge = 1;

  pthread_mutex_lock(&msg_mutex_);

  Message* msg = context->receive_message_buffer->GetMessage();
  struct ibv_mr* mr = context->receive_message_buffer->GetMR();

  sge.addr = (uint64_t)msg;
  sge.length = sizeof(*msg);
  sge.lkey = mr->lkey;

  int ret = 0;
  if ((ret = ibv_post_recv(context->queue_pair, &receive_work_request,
                           &bad_work_request))) {
    cerr << "ibv_post_recv failed: " << strerror(ret) << endl;
    pthread_mutex_unlock(&msg_mutex_);
    return -1;
  }

  pthread_mutex_unlock(&msg_mutex_);
  clock_gettime(CLOCK_MONOTONIC, &end_receive_message_);
  double time_taken = ((double)end_receive_message_.tv_sec * 1e+9 +
                       (double)end_receive_message_.tv_nsec) -
                      ((double)start_receive_message_.tv_sec * 1e+9 +
                       (double)start_receive_message_.tv_nsec);
  total_receive_message_time_ += time_taken;
  ++num_receive_message_;
  ++num_rdma_recv_;

  return 0;
}

// Register local memory regions for RDMA
int Client::RegisterMemoryRegion(Context* context) {
  context->send_message_buffer.reset(new MessageBuffer);
  context->receive_message_buffer.reset(new MessageBuffer);

  if (context->send_message_buffer->Register(context)) {
    cerr << "MessageBuffer::Register failed()" << endl;
    return -1;
  }
  if (context->receive_message_buffer->Register(context)) {
    cerr << "MessageBuffer::Register failed()" << endl;
    return -1;
  }

  context->original_value_mr =
      ibv_reg_mr(context->protection_domain, &context->original_value,
                 sizeof(context->original_value),
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
  if (context->original_value_mr == NULL) {
    cerr << "ibv_reg_mr() failed for original_value_mr." << endl;
    return -1;
  }
  for (int i = 0; i < MAX_LOCAL_THREADS; ++i) {
    LockRequest* request = lock_requests_[i].get();
    request->original_value_mr =
        ibv_reg_mr(context->protection_domain, &request->original_value,
                   sizeof(request->original_value),
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (request->original_value_mr == NULL) {
      cerr << "ibv_reg_mr() failed for request->original_value_mr." << endl;
      return -1;
    }

    request->read_buffer_mr =
        ibv_reg_mr(context->protection_domain, &request->read_buffer,
                   sizeof(request->read_buffer),
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (request->read_buffer_mr == NULL) {
      cerr << "ibv_reg_mr() failed for read_buffer_mr." << endl;
      return -1;
    }

    request->read_buffer2_mr =
        ibv_reg_mr(context->protection_domain, &request->read_buffer2,
                   sizeof(request->read_buffer2),
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (request->read_buffer2_mr == NULL) {
      cerr << "ibv_reg_mr() failed for read_buffer2_mr." << endl;
      return -1;
    }
  }

  context->read_buffer_mr =
      ibv_reg_mr(context->protection_domain, &context->read_buffer,
                 sizeof(context->read_buffer),
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
  if (context->read_buffer_mr == NULL) {
    cerr << "ibv_reg_mr() failed for read_buffer_mr." << endl;
    return -1;
  }

  context->read_buffer2_mr =
      ibv_reg_mr(context->protection_domain, &context->read_buffer2,
                 sizeof(context->read_buffer2),
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
  if (context->read_buffer2_mr == NULL) {
    cerr << "ibv_reg_mr() failed for read_buffer2_mr." << endl;
    return -1;
  }

  return 0;
}

// Builds queue pair attributes
void Client::BuildQueuePairAttr(Context* context,
                                struct ibv_exp_qp_init_attr* attributes) {
  memset(attributes, 0x00, sizeof(*attributes));

  attributes->pd = context->protection_domain;
  attributes->send_cq = context->completion_queue;
  attributes->recv_cq = context->completion_queue;
  attributes->qp_type = IBV_QPT_RC;
  attributes->cap.max_send_wr = 2048;
  attributes->cap.max_recv_wr = 2048;
  attributes->cap.max_send_sge = 4;
  attributes->cap.max_recv_sge = 4;
  attributes->comp_mask =
      IBV_EXP_QP_INIT_ATTR_PD | IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS;
  attributes->exp_create_flags = IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY;
  attributes->max_atomic_arg = sizeof(uint64_t);
}

Context* Client::BuildContext(struct rdma_cm_id* id) {
  // create new context for the connection
  Context* new_context = new Context;
  new_context->server = NULL;
  new_context->client = this;
  new_context->connected = false;
  new_context->device_context = id->verbs;
  if ((new_context->protection_domain =
           ibv_alloc_pd(new_context->device_context)) == NULL) {
    cerr << "Client: ibv_alloc_pd() failed: " << strerror(errno) << endl;
    return NULL;
  }
  if ((new_context->completion_channel =
           ibv_create_comp_channel(new_context->device_context)) == NULL) {
    cerr << "Client: ibv_create_comp_channel() failed." << endl;
    return NULL;
  }
  if ((new_context->completion_queue =
           ibv_create_cq(new_context->device_context, 64, NULL,
                         new_context->completion_channel, 0)) == NULL) {
    cerr << "Client: ibv_create_cq() failed: " << strerror(errno) << endl;
    return NULL;
  }
  if (ibv_req_notify_cq(new_context->completion_queue, 0)) {
    cerr << "Client: ibv_req_notify_cq() failed." << endl;
    return NULL;
  }
  // create completion queue poller thread
  if (pthread_create(&new_context->cq_poller_thread, NULL,
                     &Client::PollCompletionQueue, new_context)) {
    cerr << "Client: pthread_create() failed." << endl;
    return NULL;
  }

  return new_context;
}

bool Client::IsInitialized() const { return initialized_; }

double Client::GetAverageSendMessageTime() const {
  return num_send_message_ > 0 ? total_send_message_time_ / num_send_message_
                               : 0;
}

double Client::GetAverageReceiveMessageTime() const {
  return num_receive_message_ > 0
             ? total_receive_message_time_ / num_receive_message_
             : 0;
}

uint64_t Client::GetRDMASendCount() const { return num_rdma_send_; }

uint64_t Client::GetRDMARecvCount() const { return num_rdma_recv_; }

uint64_t Client::GetRDMAReadCount() const { return num_rdma_read_; }

uint64_t Client::GetRDMAWriteCount() const { return num_rdma_write_; }

uint64_t Client::GetRDMAAtomicCount() const { return num_rdma_atomic_; }

uint64_t Client::GetNumLockContention() const { return total_lock_contention_; }

uint64_t Client::GetNumLockSuccess() const { return total_lock_success_; }

uint64_t Client::GetNumLockSuccessWithPoll() const {
  return total_lock_success_with_poll_;
}

uint64_t Client::GetSumPollWhenSuccess() const {
  return sum_poll_when_success_;
}

double Client::GetTotalRDMAReadTime() const { return total_rdma_read_time_; }

double Client::GetTotalRDMAAtomicTime() const {
  return total_rdma_atomic_time_;
}

double Client::GetAverageRDMAReadTime() const {
  return total_rdma_read_time_ / (double)num_rdma_read_;
}

double Client::GetAverageRDMAAtomicTime() const {
  return total_rdma_atomic_time_ / (double)num_rdma_atomic_;
}

double Client::GetAveragePollWhenSuccess() const {
  return total_lock_success_with_poll_ == 0
             ? 0
             : (double)sum_poll_when_success_ /
                   (double)total_lock_success_with_poll_;
}

// Polls work completion from completion queue
void* Client::PollCompletionQueue(void* arg) {
  struct ibv_cq* cq;
  struct ibv_cq* ev_cq;
  struct ibv_wc wc;
  Context* queue_context;
  Context* context = static_cast<Context*>(arg);
  cq = context->completion_queue;
  int ret = 0;

  while (true) {
    if (ibv_get_cq_event(context->completion_channel, &ev_cq,
                         (void**)&queue_context)) {
      cerr << "ibv_get_cq_event() failed." << endl;
    }
    ibv_ack_cq_events(ev_cq, 1);
    if (ibv_req_notify_cq(ev_cq, 0)) {
      cerr << "ibv_req_notify_cq() failed." << endl;
    }

    while ((ret = ibv_poll_cq(cq, 1, &wc)) > 0) {
      context->client->HandleWorkCompletion(&wc);
    }

    if (ret < 0) {
      cerr << "ibv_poll_cq() failed" << endl;
    }
  }

  return NULL;
}

}  // namespace proto
}  // namespace rdma
