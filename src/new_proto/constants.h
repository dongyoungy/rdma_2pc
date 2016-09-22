#ifndef RDMA_PROTO_CONSTANTS_H
#define RDMA_PROTO_CONSTANTS_H

namespace rdma { namespace proto {

  static const int EXCLUSIVE = 0;
  static const int SHARED = 1;

  static const int LOCK_LOCAL = 0;
  static const int LOCK_REMOTE = 1;
  static const int LOCK_ADAPTIVE = 2;

  static const int TASK_LOCK = 0;
  static const int TASK_UNLOCK = 1;

  static const int RESULT_SUCCESS = 0;
  static const int RESULT_FAILURE = 1;
  static const int RESULT_RETRY = 2;

  static const int RULE_FAIL = 0;
  static const int RULE_POLL = 1;
  static const int RULE_QUEUE = 2;

  static const int FAIL_RETRY = 3;
  static const int POLL_RETRY = 3;

  static const int MAX_MESSAGE_BUFFER_SIZE = 64;
}}

#endif
