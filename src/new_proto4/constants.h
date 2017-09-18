#ifndef RDMA_PROTO_CONSTANTS_H
#define RDMA_PROTO_CONSTANTS_H

#include <pthread.h>
#include <unistd.h>

namespace rdma {
namespace proto {

enum LockResult { SUCCESS, FAILURE, RETRY, QUEUED, SUCCESS_FROM_QUEUED };
enum LockType { SHARED, EXCLUSIVE };
enum ReadType { READ_SHARED, READ_EXCLUSIVE, READ_ALL };
enum LockMode {
  LOCAL,
  PROXY_RETRY,
  PROXY_QUEUE,
  REMOTE_POLL,
  REMOTE_NOTIFY,
  REMOTE_QUEUE
};
enum Task { LOCK, UNLOCK, READ, READ_UNLOCK, READ_LOCK };
enum ThinkTimeType { ZERO, NORMAL, UNKNOWN };

const uint64_t kTransactionMax = 100000000;

static const int WORKLOAD_UNIFORM = 0;
static const int WORKLOAD_HOTSPOT = 1;

static const int FAIL_RETRY = 3;
static const int POLL_RETRY = 3;

// static const int MAX_MESSAGE_BUFFER_SIZE = 128;
// static const int MAX_LOCK_REQUESTS = 64;
static const int MAX_LOCAL_THREADS = 2048;

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

static const int ERROR_UNKNOWN_THINK_TIME_TYPE = -1000;
}  // namespace proto
}  // namespace rdma

#endif
