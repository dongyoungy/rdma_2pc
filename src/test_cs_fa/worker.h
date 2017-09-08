#ifndef RDMA_TEST_WORKER_H
#define RDMA_TEST_WORKER_H

#include <unistd.h>
#include <sys/types.h>

using namespace std;

namespace rdma { namespace test {

class Worker {
  public:
    Worker(int op_type, uint64_t op_count);
  private:
    int op_type_;
    uint64_t op_count_;


};

}}

#endif
