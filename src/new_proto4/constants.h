#ifndef RDMA_PROTO_CONSTANTS_H
#define RDMA_PROTO_CONSTANTS_H

#include <pthread.h>

namespace rdma {
namespace proto {

enum LockResult { Success, Failure };

static const int NONE = 0;
static const int EXCLUSIVE = 1;
static const int SHARED = 2;
static const int ALL = 3;

static const int WORKLOAD_UNIFORM = 0;
static const int WORKLOAD_HOTSPOT = 1;

static const int LOCK_LOCAL = 0;
static const int LOCK_PROXY_RETRY = 0;  // LOCAL == PROXY
static const int LOCK_PROXY_QUEUE = 1;  // LOCAL == PROXY
static const int LOCK_REMOTE_POLL = 2;
static const int LOCK_REMOTE_NOTIFY = 3;
static const int LOCK_REMOTE_QUEUE = 4;
static const int LOCK_ADAPTIVE = 5;

static const int TASK_LOCK = 0;
static const int TASK_UNLOCK = 1;
static const int TASK_READ = 2;
static const int TASK_READ_UNLOCK = 3;
static const int TASK_READ_LOCK = 4;

static const int RESULT_SUCCESS = 0;
static const int RESULT_FAILURE = 1;
static const int RESULT_RETRY = 2;
static const int RESULT_QUEUED = 3;
static const int RESULT_SUCCESS_FROM_QUEUED = 4;
static const int RESULT_LOCAL_FAILURE = 5;

static const int RULE_FAIL = 0;
static const int RULE_POLL = 1;
static const int RULE_QUEUE = 2;
static const int RULE_NOTIFY = 3;

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
static pthread_mutex_t PRINT_MUTEX = PTHREAD_MUTEX_INITIALIZER;
}  // namespace proto
}  // namespace rdma

#endif
