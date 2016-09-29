#include "lock_wait_queue.h"

namespace rdma { namespace proto {

// constructor
LockWaitQueue::LockWaitQueue(int max_size) {
  max_size_ = max_size;
  size_     = 0;

  // initialize the memory pool
  for (int i = 0; i < max_size_; ++i) {
    LockWaitElement* elem = new LockWaitElement;
    pool_.push_back(elem);
  }
  pthread_mutex_init(&mutex_, NULL);
}

// destructor
LockWaitQueue::~LockWaitQueue() {
  // clean pool
  while (!pool_.empty()) {
    LockWaitElement* elem = pool_.back();
    delete elem;
    pool_.pop_back();
  }
  // clean queue
  while (!queue_.empty()) {
    LockWaitElement* elem = queue_.back();
    delete elem;
    queue_.pop_back();
  }
}

// insert new element into the queue
int LockWaitQueue::Insert(int seq_no, int user_id, int type) {
  pthread_mutex_lock(&mutex_);
  LockWaitElement* elem = pool_.front();
  pool_.pop_front();

  elem->seq_no  = seq_no;
  elem->user_id = user_id;
  elem->type    = type;

  queue_.push_back(elem);
  ++size_;
  pthread_mutex_unlock(&mutex_);
  return 0;
}

// pop first element in the queue
LockWaitElement* LockWaitQueue::Pop() {
  pthread_mutex_lock(&mutex_);
  LockWaitElement* elem = queue_.front();
  queue_.pop_front();
  --size_;
  pool_.push_back(elem);
  pthread_mutex_unlock(&mutex_);

  return elem;
}

// remove all elements from queue that match the given condition
int LockWaitQueue::RemoveAllElements(int seq_no, int user_id, int type) {
  int num_elem = 0;
  list<LockWaitElement*>::iterator it;
  pthread_mutex_lock(&mutex_);
  for (it = queue_.begin();it != queue_.end();) {
    LockWaitElement* elem = *it;
    if (elem->seq_no == seq_no && elem->user_id == user_id && elem->type == type) {
      // erase it from the queue
      queue_.erase(it);
      --size_;
      // add it back to the pool
      pool_.push_back(elem);
      ++num_elem;
    } else {
      ++it;
    }
  }
  pthread_mutex_unlock(&mutex_);
  return num_elem;
}

}}
