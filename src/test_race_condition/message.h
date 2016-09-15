#ifndef RDMA_TEST_MESSAGE_H
#define RDMA_TEST_MESSAGE_H

#include <rdma/rdma_cma.h>

namespace rdma { namespace test {

class Message {
  public:
    Message() {};
    enum {
      MR_SEMAPHORE_REQUEST,
      MR_DATA_REQUEST,
      MR_SEMAPHORE_INFO,
      MR_DATA_INFO
    } type;
    struct ibv_mr memory_region;
};

}}

#endif
