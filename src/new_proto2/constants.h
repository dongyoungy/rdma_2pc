#ifndef RDMA_PROTO_CONSTANTS_H
#define RDMA_PROTO_CONSTANTS_H

#include <pthread.h>

namespace rdma { namespace proto {

  static const int EXCLUSIVE = 0;
  static const int SHARED = 1;
  static const int ALL = 2;

  static const int WORKLOAD_UNIFORM = 0;
  static const int WORKLOAD_HOTSPOT = 1;

  static const int LOCK_LOCAL = 0;
  static const int LOCK_PROXY_RETRY = 0; // LOCAL == PROXY
  static const int LOCK_PROXY_QUEUE = 1; // LOCAL == PROXY
  static const int LOCK_REMOTE_POLL = 2;
  static const int LOCK_REMOTE_NOTIFY = 3;
  static const int LOCK_REMOTE_QUEUE = 4;
  static const int LOCK_ADAPTIVE = 5;

  static const int TASK_LOCK = 0;
  static const int TASK_UNLOCK = 1;
  static const int TASK_READ = 2;
  static const int TASK_READ_UNLOCK = 3;

  static const int RESULT_SUCCESS = 0;
  static const int RESULT_FAILURE = 1;
  static const int RESULT_RETRY   = 2;
  static const int RESULT_QUEUED  = 3;
  static const int RESULT_SUCCESS_FROM_QUEUED = 4;

  static const int RULE_FAIL = 0;
  static const int RULE_POLL = 1;
  static const int RULE_QUEUE = 2;
  static const int RULE_NOTIFY = 3;

  static const int FAIL_RETRY = 3;
  static const int POLL_RETRY = 3;

  static const int MAX_MESSAGE_BUFFER_SIZE = 16;
  static const int MAX_LOCK_REQUESTS = 64;

  static const int ERR_MORE_THAN_ONE_NODE = 1;

  // purpose of remote reading
  static const int READ_POLLING = 1;
  static const int READ_NOTIFYING = 2;
  static pthread_mutex_t PRINT_MUTEX = PTHREAD_MUTEX_INITIALIZER;
}}

#endif
