#include "message.h"
#include "local_work_queue.h"

namespace rdma { namespace proto {

// constructor
template <typename T>
LocalWorkQueue<T>::LocalWorkQueue() {
  pthread_mutex_init(&mutex_, NULL);
  pthread_cond_init(&cond_, NULL);
  run_ = true;
}

// destructor
template <typename T>
LocalWorkQueue<T>::~LocalWorkQueue() {

}

template <typename T>
void LocalWorkQueue<T>::Stop() {
  run_ = false;
}

template <typename T>
void LocalWorkQueue<T>::Insert(T elem) {
  pthread_mutex_lock(&mutex_);
  this->queue_.push_back(elem);
  pthread_cond_signal(&cond_);
  pthread_mutex_unlock(&mutex_);
}

template <typename T>
T LocalWorkQueue<T>::Remove() {
  pthread_mutex_lock(&mutex_);
  while (this->queue_.empty()) {
    pthread_cond_wait(&cond_, &mutex_);
  }
  T elem = this->queue_.front();
  this->queue_.pop_front();
  pthread_mutex_unlock(&mutex_);

  return elem;
}

template class LocalWorkQueue<Message>;

}}
