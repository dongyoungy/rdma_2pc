#ifndef RDMA_TEST_CONSTANTS_H
#define RDMA_TEST_CONSTANTS_H

namespace rdma { namespace test{
  const int TEST_UC_WRITE     = 0;
  const int TEST_RC_READ      = 1;
  const int TEST_RC_WRITE     = 2;
  const int TEST_RC_SEND_RECV = 3;
  const int TEST_RC_ATOMIC_FA    = 4;
  const int TEST_RC_ATOMIC_CAS   = 5;

}}

#endif
