#include "test_client.h"

namespace rdma { namespace test {

// constructor
//TestClient::TestClient(const string& server_name, const string& server_port,
    //int test_mode, size_t data_size) {
  //server_name_       = server_name;
  //server_port_       = server_port;
  //event_channel_     = NULL;
  //connection_        = NULL;
  //address_           = NULL;
  //current_semaphore_ = 0;
  //num_trial_         = 0;
  //total_cas_time_    = 0;
  //test_mode_         = test_mode;
  //data_size_         = data_size;
//}

// constructor
TestClient::TestClient(const string& work_dir, int test_mode) {
  work_dir_          = work_dir;
  event_channel_     = NULL;
  connection_        = NULL;
  address_           = NULL;
  current_semaphore_ = 0;
  num_trial_         = 0;
  total_cas_time_    = 0;
  total_read_time_   = 0;
  test_mode_         = test_mode;
  is_adding_sem_     = true;
  num_added_sem_     = 0;
  is_sem_reset_      = false;
  data_size_         = 1024;
}

// destructor
TestClient::~TestClient() {
}

int TestClient::Run() {
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

void TestClient::Stop() {
  exit(0);
}

int TestClient::ReadServerAddress() {
  char ip[64];
  char port[16];

  // open files
  string ip_filename = work_dir_ + "/server.ip";
  string port_filename = work_dir_ + "/server.port";
  FILE* ip_file = fopen(ip_filename.c_str(), "r");
  if (ip_file == NULL) {
    cerr << "ReadServerAddress(): fopen() failed: " << strerror(errno) << endl;
    return -1;
  }
  FILE* port_file = fopen(port_filename.c_str(), "r");
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

int TestClient::HandleEvent(struct rdma_cm_event* event) {
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

int TestClient::HandleAddressResolved(struct rdma_cm_id* id) {

  Context* context = BuildContext(id);
  if (context == NULL) {
    cerr << "BuildContext() failed." << endl;
    return -1;
  }

  struct ibv_exp_qp_init_attr queue_pair_attributes;
  BuildQueuePairAttr(context, &queue_pair_attributes);

  //if (rdma_create_qp(id, context->protection_domain, &queue_pair_attributes)) {
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

  // post receive to handle MR information from server
  if (ReceiveMessage(context)) {
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

int TestClient::HandleRouteResolved(struct rdma_cm_id* id) {
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

int TestClient::HandleConnection(Context* context) {
  //cout << "connected to server." << endl;
  context->connected = true;

  RequestSemaphore(context);

  return 0;
}

int TestClient::HandleDisconnect(Context* context) {

  //rdma_destroy_qp(context->id);

  if (context->send_mr)
    ibv_dereg_mr(context->send_mr);
  if (context->receive_mr)
    ibv_dereg_mr(context->receive_mr);
  if (context->rdma_local_mr)
    ibv_dereg_mr(context->rdma_local_mr);
  if (context->rdma_remote_mr)
    ibv_dereg_mr(context->rdma_remote_mr);

  delete context->send_message;
  delete context->receive_message;

  //rdma_destroy_id(context->id);

  delete context;

  //cout << "disconnected." << endl;
  return 0;
}

// Post receive to get message from clients
int TestClient::ReceiveMessage(Context* context) {
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
int TestClient::RegisterMemoryRegion(Context* context) {

  context->send_message = new Message;
  context->receive_message = new Message;
  context->client_semaphore = &semaphore_;

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
  context->local_buffer = new char[data_size_];
  context->rdma_local_mr = ibv_reg_mr(context->protection_domain,
      context->local_buffer,
      data_size_,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  if (context->rdma_local_mr == NULL) {
    cerr << "ibv_reg_mr() failed for rdma_local_mr." << endl;
    return -1;
  }

  context->zero_value = new uint64_t;
  memset(context->zero_value, 0x00, sizeof(uint64_t));
  context->zero_value_mr = ibv_reg_mr(context->protection_domain,
      context->zero_value,
      sizeof(uint64_t),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
      IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
  if (context->zero_value_mr == NULL) {
    cerr << "ibv_reg_mr() failed for zero_value_mr." << endl;
    return -1;
  }
  context->rdma_client_semaphore = ibv_reg_mr(context->protection_domain,
      context->client_semaphore,
      sizeof(*context->client_semaphore),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  if (context->rdma_client_semaphore == NULL) {
    cerr << "ibv_reg_mr() failed for rdma_server_semaphore." << endl;
    return -1;
  }

  context->rdma_server_semaphore = NULL;
  context->rdma_remote_mr = NULL;

  return 0;
}

// Builds queue pair attributes
void TestClient::BuildQueuePairAttr(Context* context,
    struct ibv_exp_qp_init_attr* attributes) {
  memset(attributes, 0x00, sizeof(*attributes));

  //attributes->send_cq          = context->completion_queue;
  //attributes->recv_cq          = context->completion_queue;
  //attributes->qp_type          = IBV_QPT_RC;
  //attributes->cap.max_send_wr  = 16;
  //attributes->cap.max_recv_wr  = 16;
  //attributes->cap.max_send_sge = 1;
  //attributes->cap.max_recv_sge = 1;

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

Context* TestClient::BuildContext(struct rdma_cm_id* id) {

  // create new context for the connection
  Context* new_context = new Context;
  new_context->server = NULL;
  new_context->client = this;
  new_context->connected = false;
  new_context->device_context = id->verbs;
  if ((new_context->protection_domain =
        ibv_alloc_pd(new_context->device_context)) == NULL) {
    cerr << "ibv_alloc_pd() failed." << endl;
    return NULL;
  }
  if ((new_context->completion_channel =
        ibv_create_comp_channel(new_context->device_context)) == NULL) {
    cerr << "ibv_create_comp_channel() failed." << endl;
    return NULL;
  }
  if ((new_context->completion_queue =
        ibv_create_cq(new_context->device_context, 64,
          NULL, new_context->completion_channel, 0)) == NULL) {
    cerr << "ibv_create_cq() failed." << endl;
    return NULL;
  }
  if (ibv_req_notify_cq(new_context->completion_queue, 0)) {
    cerr << "ibv_req_notify_cq() failed." << endl;
    return NULL;
  }
  // create completion queue poller thread
  if (pthread_create(&new_context->cq_poller_thread, NULL,
        &TestClient::PollCompletionQueue, new_context)) {
     cerr << "pthread_create() failed." << endl;
     return NULL;
  }

  return new_context;
}

int TestClient::HandleWorkCompletion(struct ibv_wc* work_completion) {
  Context* context = (Context *)work_completion->wr_id;

  if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "Work completion status is not IBV_WC_SUCCESS: " <<
      work_completion->status << endl;
    return -1;
  }

  if (work_completion->opcode == IBV_WC_RECV) {
    // post receive first.
    ReceiveMessage(context);

    // if received semaphore MR info
    if (context->receive_message->type == Message::MR_SEMAPHORE_INFO) {
      //cout << "received server memory region for semaphore." << endl;
      // copy server rdma semaphore region
      context->rdma_server_semaphore = new ibv_mr;
      memcpy(context->rdma_server_semaphore,
          &context->receive_message->memory_region,
          sizeof(*context->rdma_server_semaphore));

      //cerr << "test mode = " << test_mode_ << endl;

      if (test_mode_ == TEST_MODE_ADD_SEM) {
        AddSemaphore(context);
      } else if (test_mode_ == TEST_MODE_RESET_SEM) {
        ResetSemaphore(context);
      }

    } else if (context->receive_message->type == Message::MR_DATA_INFO) {
      //cout << "received server memory region for data." << endl;
      // copy server rdma data region
      context->rdma_server_data = new ibv_mr;
      memcpy(context->rdma_server_data,
          &context->receive_message->memory_region,
          sizeof(*context->rdma_server_data));

      // perform test
      time(&test_start_);
      clock_gettime(CLOCK_MONOTONIC, &start_);
      ReadData(context);
    }
  } else if (work_completion->opcode == IBV_WC_RDMA_WRITE) {
    is_sem_reset_ = true;
  } else if (work_completion->opcode == IBV_WC_FETCH_ADD) {
    if (is_adding_sem_) {
      AddSemaphore(context);
    }
  } else if (work_completion->opcode == IBV_WC_COMP_SWAP) {
    // completion of compare-and-swap

    // print stats for now
    //cout << "COMP_SWAP completed." << endl;
    //cout << "wr_id = " << work_completion->wr_id << endl;
    //cout << "opcode = " << work_completion->opcode << endl;
    //cout << "vendor_err = " << work_completion->vendor_err << endl;

    clock_gettime(CLOCK_MONOTONIC, &end_);
    double dt = ((double)end_.tv_sec *1.0e+9 + end_.tv_nsec) -
      ((double)start_.tv_sec * 1.0e+9 + start_.tv_nsec);
    total_cas_time_ += dt;
    ++num_trial_;

    if (num_trial_ >= TOTAL_TRIAL) {
      cout << "Average CAS Time = " << total_cas_time_ /(double)TOTAL_TRIAL <<
        " ns" << endl;
      exit(0);
    }

    // perform test
      uint64_t new_semaphore;
      if (current_semaphore_ == 0)
        new_semaphore = 1;
      else
        new_semaphore = 0;
    clock_gettime(CLOCK_MONOTONIC, &start_);
    //SetSemaphore(context, current_semaphore_, new_semaphore);
    current_semaphore_ = new_semaphore;
  } else if (work_completion->opcode == IBV_WC_RDMA_READ) {
    clock_gettime(CLOCK_MONOTONIC, &end_);
    double dt = ((double)end_.tv_sec *1.0e+9 + end_.tv_nsec) -
      ((double)start_.tv_sec * 1.0e+9 + start_.tv_nsec);
    total_read_time_ += dt;
    ++num_trial_;
    time(&test_end_);

    if (difftime(test_end_, test_start_) >= test_duration_) {
      cout << "Data size = " << data_size_ << " bytes" << endl;
      cout << "Average read time = " <<
        total_read_time_ / (double)num_trial_ << " ns" <<
        endl;
      cout << "# reads = " << num_trial_ << endl;
      exit(0);
    }

    clock_gettime(CLOCK_MONOTONIC, &start_);
    ReadData(context);
  }

  return 0;
}

// Requests semaphore MR region of the server via IBV_WR_SEND op.
int TestClient::RequestSemaphore(Context* context) {

  context->send_message->type = Message::MR_SEMAPHORE_REQUEST;
  memcpy(&context->send_message->memory_region, context->rdma_client_semaphore,
      sizeof(context->send_message->memory_region));

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

  //cout << "requested semaphore region: " << test_mode_ << endl;

  return 0;
}

int TestClient::RequestData(Context* context) {

  context->send_message->type = Message::MR_DATA_REQUEST;

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

  cout << "requested data region" << endl;

  return 0;
}

int TestClient::AddSemaphore(Context* context) {
  struct ibv_exp_send_wr send_work_request;
  struct ibv_exp_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  sge.addr = (uint64_t)context->send_message;
  sge.length = sizeof(*context->send_message);
  sge.lkey = context->send_mr->lkey;

  send_work_request.wr_id          = (uint64_t)context;
  send_work_request.exp_opcode     = IBV_EXP_WR_ATOMIC_FETCH_AND_ADD;
  send_work_request.num_sge        = 1;
  send_work_request.sg_list        = &sge;
  send_work_request.exp_send_flags = IBV_SEND_SIGNALED | IBV_SEND_FENCE;

  send_work_request.wr.atomic.remote_addr =
    (uint64_t)context->rdma_server_semaphore->addr;
  send_work_request.wr.atomic.rkey        =
    context->rdma_server_semaphore->rkey;
  send_work_request.wr.atomic.compare_add = 1;

  int ret = 0;
  if ((ret = ibv_exp_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "AddSemaphore(): ibv_exp_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  ++num_added_sem_;

  //cout << "SetSmaphore(): message sent." << endl;

  return 0;
}

int TestClient::ResetSemaphore(Context* context) {
  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  sge.addr = (uint64_t)context->zero_value;
  sge.length = sizeof(uint64_t);
  sge.lkey = context->zero_value_mr->lkey;

  send_work_request.wr_id      = (uint64_t)context;
  send_work_request.opcode     = IBV_WR_RDMA_WRITE;
  send_work_request.num_sge    = 1;
  send_work_request.sg_list    = &sge;
  send_work_request.send_flags = IBV_SEND_SIGNALED | IBV_SEND_FENCE;

  send_work_request.wr.rdma.remote_addr =
    (uint64_t)context->rdma_server_semaphore->addr;
  send_work_request.wr.rdma.rkey        =
    context->rdma_server_semaphore->rkey;

  int ret = 0;
  if ((ret = ibv_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ResetSemaphore(): ibv_post_send() failed: " <<
      strerror(ret) << endl;
    return -1;
  }

  cerr << "RESET SEMAPHORE" << endl;

  return 0;
}

// reads data from the server's local MR.
int TestClient::ReadData(Context* context) {
  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  sge.addr   = (uint64_t)context->local_buffer;
  sge.length = data_size_;
  sge.lkey   = context->rdma_local_mr->lkey;

  send_work_request.wr_id      = (uint64_t)context;
  send_work_request.opcode     = IBV_WR_RDMA_READ;
  send_work_request.num_sge    = 1;
  send_work_request.sg_list    = &sge;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  send_work_request.wr.rdma.remote_addr =
    (uint64_t)context->rdma_server_data->addr;
  send_work_request.wr.rdma.rkey        =
    context->rdma_server_data->rkey;

  int ret = 0;
  if ((ret = ibv_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "ReadData(): ibv_post_send() failed: " << strerror(ret) << endl;
    return -1;
  }

  return 0;
}

int TestClient::RepeatAddingSemaphore(Context* context) {
  while (is_adding_sem_) {
    AddSemaphore(context);
  }
}

void TestClient::StopAddingSem() {
  is_adding_sem_ = false;
}

uint64_t TestClient::GetNumAddedSemaphore() const {
  return num_added_sem_;
}

bool TestClient::IsSemReset() const {
  return is_sem_reset_;
}

// Polls work completion from completion queue
void* TestClient::PollCompletionQueue(void* arg) {
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
