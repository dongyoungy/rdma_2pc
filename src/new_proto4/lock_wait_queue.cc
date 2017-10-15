#include "lock_wait_queue.h"

namespace rdma {
namespace proto {

std::list<LockWaitElement*> LockWaitQueue::pool_;
Poco::Mutex LockWaitQueue::pool_mutex_;

// constructor
LockWaitQueue::LockWaitQueue() {
  size_ = 0;

  pthread_mutex_init(&mutex_, NULL);
}

// destructor
LockWaitQueue::~LockWaitQueue() {
  // clean queue
  while (!queue_.empty()) {
    LockWaitElement* elem = queue_.back();
    delete elem;
    queue_.pop_back();
  }
}

void LockWaitQueue::InitializePool() {
  // initialize the memory pool
  for (int i = 0; i < MAX_WAIT_QUEUE_POOL_SIZE; ++i) {
    LockWaitElement* elem = new LockWaitElement;
    pool_.push_back(elem);
  }
}

LockWaitElement* LockWaitQueue::GetElementFromPool() {
  Poco::Mutex::ScopedLock lock(pool_mutex_);
  LockWaitElement* elem = pool_.front();
  pool_.pop_front();
  return elem;
}

void LockWaitQueue::PushElementToPool(LockWaitElement* elem) {
  Poco::Mutex::ScopedLock lock(pool_mutex_);
  pool_.push_back(elem);
}

// insert new element into the queue
int LockWaitQueue::Insert(int seq_no, uint32_t target_node_id,
                          uint32_t owner_node_id, uintptr_t owner_user_id,
                          LockType type) {
  pthread_mutex_lock(&mutex_);
  LockWaitElement* elem = LockWaitQueue::GetElementFromPool();

  elem->seq_no = seq_no;
  elem->target_node_id = target_node_id;
  elem->owner_node_id = owner_node_id;
  elem->owner_user_id = owner_user_id;
  elem->type = type;

  queue_.push_back(elem);
  ++size_;
  pthread_mutex_unlock(&mutex_);
  return 0;
}

// pop first element in the queue
LockWaitElement* LockWaitQueue::Pop() {
  pthread_mutex_lock(&mutex_);
  if (queue_.empty()) {
    pthread_mutex_unlock(&mutex_);
    return NULL;
  }
  LockWaitElement* elem = queue_.front();
  queue_.pop_front();
  --size_;
  LockWaitQueue::PushElementToPool(elem);
  pthread_mutex_unlock(&mutex_);

  return elem;
}

// return first element in the queue
LockWaitElement* LockWaitQueue::Front() {
  pthread_mutex_lock(&mutex_);
  if (queue_.empty()) {
    pthread_mutex_unlock(&mutex_);
    return NULL;
  }
  LockWaitElement* elem = queue_.front();
  pthread_mutex_unlock(&mutex_);

  return elem;
}

// remove all elements from queue that match the given condition
int LockWaitQueue::RemoveAllElements(uint32_t owner_node_id, int type) {
  int num_elem = 0;
  list<LockWaitElement*>::iterator it;
  pthread_mutex_lock(&mutex_);
  for (it = queue_.begin(); it != queue_.end();) {
    LockWaitElement* elem = *it;
    if (elem->type == type && elem->owner_node_id == owner_node_id) {
      // erase it from the queue
      it = queue_.erase(it);
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

void LockWaitQueue::PrintAll() {
  list<LockWaitElement*>::iterator it;
  int cnt = 0;
  for (it = queue_.begin(); it != queue_.end(); ++it) {
    LockWaitElement* elem = *it;
    cout << "queue elem #" << cnt << " = " << elem->seq_no << ","
         << elem->target_node_id << "," << elem->owner_node_id << ","
         << elem->owner_user_id << "," << elem->type << endl;
    ++cnt;
  }
}

void LockWaitQueue::RemoveAll() {
  list<LockWaitElement*>::iterator it;
  pthread_mutex_lock(&mutex_);
  for (it = queue_.begin(); it != queue_.end();) {
    LockWaitElement* elem = *it;
    // erase it from the queue
    it = queue_.erase(it);
    --size_;
    // add it back to the pool
    pool_.push_back(elem);
  }
  pthread_mutex_unlock(&mutex_);
}

}  // namespace proto
}  // namespace rdma
