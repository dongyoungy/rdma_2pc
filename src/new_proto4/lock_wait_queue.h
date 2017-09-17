#ifndef RDMA_PROTO_LOCKWAITQUEUE_H
#define RDMA_PROTO_LOCKWAITQUEUE_H

#include <pthread.h>
#include <iostream>
#include <list>
#include <queue>
#include <vector>
#include "lock_wait_element.h"

using namespace std;

namespace rdma {
namespace proto {

class LockWaitQueue {
 public:
  LockWaitQueue(int max_size);
  ~LockWaitQueue();
  int RemoveAllElements(uint32_t owner_node_id, int type);
  int Insert(int seq_no, uint32_t target_node_id, uint32_t owner_node_id,
             uint32_t owner_user_id, LockType type);
  void PrintAll();
  void RemoveAll();
  LockWaitElement* Pop();
  LockWaitElement* Front();
  inline int GetSize() const { return size_; }

 private:
  list<LockWaitElement*> queue_;
  list<LockWaitElement*> pool_;
  int size_;
  int max_size_;
  pthread_mutex_t mutex_;
};

}  // namespace proto
}  // namespace rdma

#endif
