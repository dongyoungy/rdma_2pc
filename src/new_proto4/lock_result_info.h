#ifndef RDMA_PROTO_LOCKRESULTINFO_H
#define RDMA_PROTO_LOCKRESULTINFO_H

#include <future>
#include "constants.h"

namespace rdma {
namespace proto {

struct LockStat {
  LockStat() {
    contention_count = 0;
    contention_count2 = 0;
    contention_count3 = 0;
    contention_count4 = 0;
    contention_count5 = 0;
    contention_count6 = 0;
  }
  LockStat(int c1, int c2, int c3, int c4, int c5, int c6)
      : contention_count(c1),
        contention_count2(c2),
        contention_count3(c3),
        contention_count4(c4),
        contention_count5(c5),
        contention_count6(c6) {}
  LockStat(const LockStat& other) {
    contention_count = other.contention_count;
    contention_count2 = other.contention_count2;
    contention_count3 = other.contention_count3;
    contention_count4 = other.contention_count4;
    contention_count5 = other.contention_count5;
    contention_count6 = other.contention_count6;
  }
  LockStat& operator=(const LockStat& other) {
    contention_count = other.contention_count;
    contention_count2 = other.contention_count2;
    contention_count3 = other.contention_count3;
    contention_count4 = other.contention_count4;
    contention_count5 = other.contention_count5;
    contention_count6 = other.contention_count6;
    return *this;
  }

  int contention_count = 0;
  int contention_count2 = 0;
  int contention_count3 = 0;
  int contention_count4 = 0;
  int contention_count5 = 0;
  int contention_count6 = 0;
};

struct LockResultInfo {
  LockResultInfo(LockResult r, int c) : result(r) { stat.contention_count = c; }
  LockResultInfo(LockResult r, LockStat s) : result(r), stat(s) {}
  LockResult result;
  LockStat stat;
};

}  // namespace proto
}  // namespace rdma

#endif
