#ifndef RDMA_PROTO_CONSTANTS_H
#define RDMA_PROTO_CONSTANTS_H

#include <pthread.h>
#include <unistd.h>
#include <cmath>
#include <cstdint>

namespace rdma {
namespace proto {

enum LockResult {
  SUCCESS,
  FAILURE,
  RETRY,
  QUEUED,
  SUCCESS_FROM_QUEUED,
  NODE_FAILURE
};
enum LockType {
  NONE,
  SHARED,
  EXCLUSIVE,
  BOTH,
  SHARED_EXTEND,
  EXCLUSIVE_EXTEND,
  UPDATE,
  TABLE_S,
  TABLE_IS,
  TABLE_IX
};
enum ReadType { READ_SHARED, READ_EXCLUSIVE, READ_ALL };
enum LockMode {
  LOCAL,
  PROXY_RETRY,
  PROXY_QUEUE,
  PROXY_QUEUE2,
  REMOTE_POLL,
  REMOTE_NOTIFY,
  REMOTE_DRTM,
  REMOTE_D2LM_V1,
  REMOTE_D2LM_V2,
  REMOTE_D2LM_UPDATE
};
enum Task {
  LOCK,
  UNLOCK,
  READ,
  READ_UNLOCK,
  READ_LOCK,
  RESET,
  UNDO,
  UNDO_NUMBER,
  LEAVE,
  RESET_FOR_DEADLOCK,
  TABLE_LOCK,
  TABLE_UNLOCK,
  UNDO_TABLE
};
enum ThinkTimeType { ZERO, NORMAL, SIMPLE, UNKNOWN };
enum LockStatus { IDLE, LOCKING, LOCKED, UNLOCKING, UNLOCKED, INVALID };

const uint64_t kTransactionMax = 100000000;
// const uint32_t kMaxBackoff = 100000;  // microseconds
// const uint32_t kBaseBackoff = 10;     // microseconds
const uint64_t kTPCCNumObjects = 700000;
const uint64_t kTPCCMaxWarehouse = 50;
const size_t kMaxWaitQueueSize = 1000000;

const uint32_t kDRTMSharedLimit = 16;
const int kD2LMBaseReadBackoff = 20;
const int kD2LMMaxReadBackoff = 1000;
const int kD2LMDefaultIncrement = 1;
const int kMaxClients = 8;

const int kValueIdx = 0;
const int kLeaverIdx = 1;
const int kCounterIdx = 2;
const int kNumFields = 3;

const int kSharedMaxBits = 16;
const int kExclusiveMaxBits = 16;
const int kSharedNumberBits = 16;
const int kExclusiveNumberBits = 16;

const int kSharedMaxBitShift = 0;
const int kExclusiveMaxBitShift = 16;
const int kSharedNumberBitShift = 32;
const int kExclusiveNumberBitShift = 48;

const int kTableSBitShift = 0;
const int kTableISBitShift = 20;
const int kTableIXBitShift = 40;

const uint64_t kTableSBitMask = 0xFFFFF;
const uint64_t kTableISBitMask = 0xFFFFF00000;
const uint64_t kTableIXBitMask = 0xFFFFF0000000000;

const int kD2LMSharedMaxBitShift = 0;
const int kD2LMExclusiveMaxBitShift = 10;
const int kD2LMUpdateMaxBitShift = 20;
const int kD2LMSharedNumberBitShift = 30;
const int kD2LMExclusiveNumberBitShift = 40;
const int kD2LMUpdateNumberBitShift = 50;

const int kDRTMLockBitShift = 63;
const int kDRTMOwnerBitShift = 55;
const int kDRTMSharedLeaseTime = 400;  // 400 microseconds
const uint64_t kDRTMEndTimeBitMask = 0x7FFFFFFFFFFFFF;

const uint64_t kExclusiveNumberBitMask = 0xFFFF000000000000;
const uint64_t kSharedNumberBitMask = 0xFFFF00000000;
const uint64_t kExclusiveMaxBitMask = 0xFFFF0000;
const uint64_t kSharedMaxBitMask = 0xFFFF;

const uint64_t kD2LMSharedMaxBitMask = 0x3FF;
const uint64_t kD2LMExclusiveMaxBitMask = 0xFFC00;
const uint64_t kD2LMUpdateMaxBitMask = 0x3FF00000;

const uint64_t kD2LMSharedNumberBitMask = 0xFFC0000000;
const uint64_t kD2LMExclusiveNumberBitMask = 0x3FF0000000000;
const uint64_t kD2LMUpdateNumberBitMask = 0xFFC000000000000;

constexpr uint16_t kMaxPossibleNumber = 32768;    // 2^15
constexpr uint16_t kD2LMMaxPossibleNumber = 512;  // 2^9
// constexpr uint16_t kMaxPossibleNumber = 4;  // 2^15

static const int WORKLOAD_UNIFORM = 0;
static const int WORKLOAD_HOTSPOT = 1;

static const int FAIL_RETRY = 3;
static const int POLL_RETRY = 3;

// static const int MAX_MESSAGE_BUFFER_SIZE = 128;
// static const int MAX_LOCK_REQUESTS = 64;
static const int MAX_LOCAL_THREADS = 128;
static const int MAX_WAIT_QUEUE_POOL_SIZE = 4096;
static const int MAX_USER = 65536;

static const int ERR_MORE_THAN_ONE_NODE = 1;

static const int FUNC_FAIL = -1;
static const int FUNC_SUCCESS = 0;

static const int LOCAL_LOCK_PASS = 1;
static const int LOCAL_LOCK_FAIL = 2;
static const int LOCAL_LOCK_RETRY = 3;
static const int LOCAL_LOCK_WAIT = 4;

static const int LOCAL_LOCK_EXIST = 0;
static const int LOCAL_LOCK_NOT_EXIST = 1;

static const int GLOBAL_LOCK_WAITING = 1;
static const int LOCAL_LOCK_WAITING = 2;

static const int LOCK_STATUS_IDLE = 0;
static const int LOCK_STATUS_LOCKING = 1;
static const int LOCK_STATUS_UNLOCKING = 2;
static const int LOCK_STATUS_FULL = 3;

static const int KV_UNIFORM = 0;
static const int KV_ZIPF = 1;

// purpose of remote reading
static const int READ_POLLING = 1;
static const int READ_NOTIFYING = 2;

static const int ERROR_UNKNOWN_THINK_TIME_TYPE = 10;
static const int ERROR_UNLOCK_FAIL = 11;
static const int ERROR_INVALID_LOCK_MODE = 12;
static const int ERROR_INVALID_RANDOM_BACKOFF = 13;
static const int ERROR_INVALID_OPCODE = 14;
static const int ERROR_INVALID_FUTURE_STATUS = 15;
static const int ERROR_FAILED_SANITY_CHECK = 16;
static const int ERROR_FA_FOR_EXCLUSIVE = 17;
static const int ERROR_INVALID_LOCK_TYPE = 18;
static const int ERROR_INVALID_TASK = 19;
}  // namespace proto
}  // namespace rdma

#endif
