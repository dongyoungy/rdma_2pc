#ifndef RDMA_PROTO_LOCKRESULTINFO_H
#define RDMA_PROTO_LOCKRESULTINFO_H

#include <future>

namespace rdma {
namespace proto {

struct LockResultInfo {
  LockResultInfo(LockResult r, int c) : result(r), contention_count(c) {}
  LockResult result;
  int contention_count;
};

}  // namespace proto
}  // namespace rdma

#endif
