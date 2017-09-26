#ifndef RDMA_PROTO_LOCKSTATUSINFO_H
#define RDMA_PROTO_LOCKSTATUSINFO_H

#include <future>
#include "constants.h"

namespace rdma {
namespace proto {

struct LockStatusInfo {
  LockStatusInfo() {
    seq_no = -1;
    user_id = 0;
    status = IDLE;
  }
  LockStatusInfo(int seq, uintptr_t u, LockStatus s)
      : seq_no(seq), user_id(u), status(s) {}

  int seq_no;
  uintptr_t user_id;
  LockStatus status;
};

}  // namespace proto
}  // namespace rdma

#endif
