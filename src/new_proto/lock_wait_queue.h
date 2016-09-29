#ifndef RDMA_PROTO_LOCKWAITQUEUE_H
#define RDMA_PROTO_LOCKWAITQUEUE_H

#include <queue>
#include <list>
#include <vector>
#include <pthread.h>
#include "lock_wait_element.h"

using namespace std;

namespace rdma { namespace proto {

class LockWaitQueue {
  public:
    LockWaitQueue(int max_size);
    ~LockWaitQueue();
    int RemoveAllElements(int seq_no, int user_id, int type);
    int Insert(int seq_no, int user_id, int type);
    LockWaitElement* Pop();
    inline int GetSize() const {
      return size_;
    }
  private:
    list<LockWaitElement*> queue_;
    list<LockWaitElement*> pool_;
    int size_;
    int max_size_;
    pthread_mutex_t mutex_;
};

}}

#endif
