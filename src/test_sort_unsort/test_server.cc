#include "test_server.h"

namespace rdma { namespace test {

// constructor
TestServer::TestServer(const string& work_dir, int* data, size_t data_size) {
  work_dir_                 = work_dir;
  listener_                 = NULL;
  event_channel_            = NULL;
  registered_memory_region_ = NULL;
  port_                     = 0;
  data_size_                = data_size;
  data_                     = data; // sorted
  pd_                       = NULL;
  count_                    = 0;
  count2_                   = 0;
  context_                  = NULL;
}

// destructor
TestServer::~TestServer() {
  if (buffer_)
    delete[] buffer_;
  if (data_)
    delete[] data_;
}

int TestServer::Run() {
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
  //cout << "listning on port " << port_ << endl;

  // print ip,port,lsf job id in the working directory
  if (PrintInfo()) {
    cerr << "PrintInfo() error." << endl;
    return -1;
  }


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

int TestServer::PrintInfo() {
  // write ip, port (infiniband) and current LSF job id in the work dir

  // open files
  string ip_filename = work_dir_ + "/server.ip";
  string port_filename = work_dir_ + "/server.port";
  string job_id_filename = work_dir_ + "/server.jobid";
  FILE* ip_file = fopen(ip_filename.c_str(), "w");
  if (ip_file == NULL) {
    cerr << "Run(): fopen() failed: " << strerror(errno) << endl;
    return -1;
  }
  FILE* port_file = fopen(port_filename.c_str(), "w");
  if (port_file == NULL) {
    cerr << "Run(): fopen() failed: " << strerror(errno) << endl;
    return -1;
  }
  FILE* job_id_file = fopen(job_id_filename.c_str(), "w");
  if (job_id_file == NULL) {
    cerr << "Run(): fopen() failed: " << strerror(errno) << endl;
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
  fclose(job_id_file);
}

int TestServer::GetInfinibandIP(string& ip_address) {
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

void TestServer::DestroyListener() {
  if (listener_)
    rdma_destroy_id(listener_);
  if (event_channel_)
    rdma_destroy_event_channel(event_channel_);
}

void TestServer::Stop() {
  DestroyListener();
  exit(0);
}

// Currently the server registers same memory region for each client.
int TestServer::RegisterMemoryRegion(Context* context) {

  context->send_message = new Message;
  context->receive_message = new Message;

  //semaphore_ = 0; // init to 0
  //context->server_semaphore = &semaphore_;
  context->server_data = data_;

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

  context->rdma_server_data = ibv_reg_mr(context->protection_domain,
      context->server_data,
      data_size_*sizeof(int),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
      IBV_ACCESS_REMOTE_WRITE);
  if (context->rdma_server_data == NULL) {
    cerr << "ibv_reg_mr() failed for rdma_server_data." << endl;
    return -1;
  }

  context->rdma_client_semaphore = NULL;
  context->rdma_local_mr = NULL;
  context->rdma_remote_mr = NULL;

  return 0;
}

int TestServer::HandleConnectRequest(struct rdma_cm_id* id) {
  Context* context = BuildContext(id);
  if (context == NULL) {
    cerr << "BuildContext() failed." << endl;
    return -1;
  }
  context_ = context;
  struct ibv_exp_qp_init_attr queue_pair_attributes;
  BuildQueuePairAttr(context, &queue_pair_attributes);

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

int TestServer::HandleConnection(Context* context) {
  //cout << "Client connected." << endl;
  context->connected = true;

  return 0;
}

int TestServer::HandleDisconnect(Context* context) {
  // rdma_destroy_qp() causes seg fault when client disconnects. why?
  //rdma_destroy_qp(context->id);

  if (context->send_mr)
    ibv_dereg_mr(context->send_mr);
  if (context->receive_mr)
    ibv_dereg_mr(context->receive_mr);
  if (context->rdma_local_mr)
    ibv_dereg_mr(context->rdma_local_mr);
  if (context->rdma_remote_mr)
    ibv_dereg_mr(context->rdma_remote_mr);
  if (context->rdma_server_semaphore)
    ibv_dereg_mr(context->rdma_server_semaphore);
  if (context->rdma_server_data)
    ibv_dereg_mr(context->rdma_server_data);

  delete context->send_message;
  delete context->receive_message;

  delete context;

  //cout << "client disconnected." << endl;
  return 0;
}

// Send local RDMA semaphore memory region to client.
int TestServer::SendSemaphoreMemoryRegion(Context* context) {
  // create thread that polls local semaphore
  context->send_message->type = Message::MR_SEMAPHORE_INFO;
  memcpy(&context->send_message->memory_region, context->rdma_server_semaphore,
      sizeof(context->send_message->memory_region));
  if (SendMessage(context)) {
    cerr << "SendSemaphoreMemoryRegion(): SendMessage() failed." << endl;
    return -1;
  }

  //cout << "SendSemaphoreMemoryRegion(): memory region sent." << endl;
  return 0;
}

// Send local RDMA semaphore memory region to client.
int TestServer::SendDataMemoryRegion(Context* context) {
  context->send_message->type = Message::MR_DATA_INFO;
  memcpy(&context->send_message->memory_region, context->rdma_server_data,
      sizeof(context->send_message->memory_region));
  if (SendMessage(context)) {
    cerr << "SendDataMemoryRegion(): SendMessage() failed." << endl;
    return -1;
  }

  //cout << "SendDataMemoryRegion(): memory region sent." << endl;
  return 0;
}

int TestServer::SendMessage(Context* context) {
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
int TestServer::ReceiveMessage(Context* context) {
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
  //if ((ret = ibv_post_srq_recv(srq_, &receive_work_request,
          //&bad_work_request))) {
    //cerr << "ibv_post_srq_recv failed: " << strerror(ret) << endl;
    //return -1;
  //}

  if ((ret = ibv_post_recv(context->queue_pair, &receive_work_request,
          &bad_work_request))) {
    cerr << "ibv_post_recv failed: " << strerror(ret) << endl;
    return -1;
  }

  return 0;
}

// Builds queue pair attributes
void TestServer::BuildQueuePairAttr(Context* context,
    struct ibv_exp_qp_init_attr* attributes) {
  memset(attributes, 0x00, sizeof(*attributes));

  attributes->pd               = context->protection_domain;
  attributes->send_cq          = context->completion_queue;
  attributes->recv_cq          = context->completion_queue;
  attributes->qp_type          = IBV_QPT_RC;
  attributes->cap.max_send_wr  = 256;
  attributes->cap.max_recv_wr  = 256;
  attributes->cap.max_send_sge = 2;
  attributes->cap.max_recv_sge = 2;
  attributes->comp_mask = IBV_EXP_QP_INIT_ATTR_PD |
    IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS;
  attributes->max_atomic_arg = sizeof(uint64_t);
  attributes->exp_create_flags = IBV_EXP_QP_CREATE_ATOMIC_BE_REPLY;
}

Context* TestServer::BuildContext(struct rdma_cm_id* id) {
  // create new context for the connection
  Context* new_context = new Context;
  new_context->server = this;
  new_context->client = NULL;
  new_context->connected = false;
  new_context->device_context = id->verbs;
  if (pd_ == NULL) {
    if ((pd_ =
          ibv_alloc_pd(new_context->device_context)) == NULL) {
      cerr << "ibv_alloc_pd() failed." << endl;
      return NULL;
    }
    //struct ibv_srq_init_attr attr;
    //memset(&attr, 0x00, sizeof(attr));
    //attr.attr.max_wr = 2;
    //attr.attr.max_sge = 1;
    //srq_ = ibv_create_srq(pd_, &attr);
  }
  new_context->protection_domain = pd_;
  //if ((new_context->protection_domain =
        //ibv_alloc_pd(new_context->device_context)) == NULL) {
    //cerr << "ibv_alloc_pd() failed." << endl;
    //return NULL;
  //}
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
        &TestServer::PollCompletionQueue, new_context)) {
     cerr << "pthread_create() failed." << endl;
     return NULL;
  }

  return new_context;
}

// Handles RDMA connection manager events
int TestServer::HandleEvent(struct rdma_cm_event* event) {
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
int TestServer::HandleWorkCompletion(struct ibv_wc* work_completion) {
  Context* context = (Context *)work_completion->wr_id;

  if (work_completion->status != IBV_WC_SUCCESS) {
    cerr << "Work completion status is not IBV_WC_SUCCESS." << endl;
    return -1;
  }

  if (work_completion->opcode & IBV_WC_RECV) {
    // Post receive first.
    ReceiveMessage(context);

    // if client is requesting semaphore MR
    if (context->receive_message->type == Message::MR_SEMAPHORE_REQUEST) {
      context->rdma_client_semaphore = new ibv_mr;
      memcpy(context->rdma_client_semaphore, &context->receive_message->memory_region,
          sizeof(*context->rdma_client_semaphore));
      SendSemaphoreMemoryRegion(context);
    } else if (context->receive_message->type == Message::MR_DATA_REQUEST) {
      context->range_data_mr = new ibv_mr;
      memcpy(context->range_data_mr, &context->receive_message->memory_region,
          sizeof(*context->range_data_mr));
      SendDataMemoryRegion(context);
    } else if (context->receive_message->type == Message::RANGE_DATA_REQUEST) {
      WriteRangeData(context, context->receive_message->min, context->receive_message->max);
    } else {
      cerr << "Unknown message type: " << context->receive_message->type
        << endl;
      return -1;
    }
  }
}

int TestServer::WriteRangeData(Context* context, int min, int max) {
  size_t min_index = FindMinIndex(min, 0, data_size_);
  size_t max_index = FindMaxIndex(max, 0, data_size_);

  //cout << "min index = " << min_index << ", val = " << data_[min_index] << endl;
  //cout << "max index = " << max_index << ", val = " << data_[max_index] << endl;

  struct ibv_send_wr send_work_request;
  struct ibv_send_wr* bad_work_request;
  struct ibv_sge sge;

  memset(&send_work_request, 0x00, sizeof(send_work_request));

  sge.addr = (uint64_t)context->server_data+(sizeof(int)*min_index);
  sge.length = sizeof(int)*(max_index-min_index+1);
  sge.lkey = context->rdma_server_data->lkey;

  send_work_request.wr_id      = (uint64_t)context;
  send_work_request.opcode     = IBV_WR_RDMA_WRITE;
  send_work_request.num_sge    = 1;
  send_work_request.sg_list    = &sge;
  send_work_request.send_flags = IBV_SEND_SIGNALED;

  send_work_request.wr.rdma.remote_addr =
    (uint64_t)context->range_data_mr->addr;
  send_work_request.wr.rdma.rkey        =
    context->range_data_mr->rkey;

  int ret = 0;
  if ((ret = ibv_post_send(context->queue_pair, &send_work_request,
          &bad_work_request))) {
    cerr << "WriteRangeData(): ibv_post_send() failed: " <<
      strerror(ret) << endl;
    return -1;
  }

  return 0;
}

size_t TestServer::FindMinIndex(int val, ssize_t low, ssize_t high) {
  ssize_t mid = low + ((high-low)/2);
  if (low >= high) {
    if (data_[mid] < val)
      return mid+1;
    else {
      if (mid < 0) return 0;
      else return mid;
    }
  }
  if (data_size_ <= mid) {
    return data_size_ - 1;
  } else if (data_[mid] == val) {
    while (mid >= 0 && data_[mid] == val) {
      --mid;
    }
    return mid+1;
  }

  if (data_[mid] > val) return FindMinIndex(val, low, mid-1);
  else return FindMinIndex(val, mid+1, high);
}

size_t TestServer::FindMaxIndex(int val, ssize_t low, ssize_t high) {
  ssize_t mid = low + ((high-low)/2);
  if (low >= high) {
    if (data_[mid] > val)
      return mid-1;
    else {
      if (mid > data_size_ - 1) return data_size_ - 1;
      else return mid;
    }
  }
  if (data_size_ <= mid) {
    return data_size_ - 1;
  } else if (data_[mid] == val) {
    while (mid < data_size_ && data_[mid] == val) {
      ++mid;
    }
    return mid-1;
  }

  if (data_[mid] > val) return FindMaxIndex(val, low, mid-1);
  else return FindMaxIndex(val, mid+1, high);
}

// Polls work completion from completion queue
void* TestServer::PollCompletionQueue(void* arg) {
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


}} // end namespace
