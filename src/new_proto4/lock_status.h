#ifndef RDMA_PROTO_LOCKSTATUSINFO_H
#define RDMA_PROTO_LOCKSTATUSINFO_H

#include <future>
#include "constants.h"

namespace rdma {
namespace proto {

struct LockStatusInfo {
  LockResultInfo(LockResult r, int c) : result(r), contention_count(c) {}

  int contention_count;
};

}  // namespace proto
}  // namespace rdma

#endif
