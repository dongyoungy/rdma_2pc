#ifndef RDMA_PROTO_LOCALWORKQUEUE_H
#define RDMA_PROTO_LOCALWORKQUEUE_H

#include <pthread.h>
#include <list>

using namespace std;

namespace rdma { namespace proto {

template <typename T> class LocalWorkQueue {
  public:
    LocalWorkQueue();
    ~LocalWorkQueue();
    void Insert(T elem);
    void Stop();
    T Remove();
    inline bool IsRun() const {
      return run_;
    }

  private:
    list<T> queue_;
    pthread_mutex_t mutex_;
    pthread_cond_t cond_;
    volatile bool run_;
};

}}


#endif
