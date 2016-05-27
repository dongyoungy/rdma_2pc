#ifndef RDMA_TEST_MESSAGE_H
#define RDMA_TEST_MESSAGE_H

#include <rdma/rdma_cma.h>

namespace rdma { namespace test {

class Message {
  public:
    Message() {};
    enum {
      MR_REQUEST,
      MR_INFO
    } type;
    struct ibv_mr memory_region;
};

}}

#endif
