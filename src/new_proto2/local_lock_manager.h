#ifndef RDMA_PROTO_LOCALLOCKMANAGER_H
#define RDMA_PROTO_LOCALLOCKMANAGER_H

#include <queue>
#include <unordered_map>

#include "local_lock_wait_element.h"

using namespace std;

namespace rdma { namespace proto {

/**
 * This class manages globally obtained locks for local threads.
 * The class does not guarantee thread-safe. Therefore, other classes which utilize this class
 * must ensure that race condition does not happen with extra care.
 */
class LocalLockManager {
  public:
    LocalLockManager(int node_id);
    ~LocalLockManager();
    int CheckLock(int seq_no, int ownder_thread_id, int target_node_id,
        int target_obj_index, int lock_type);
    int Lock(int target_node_id, int target_obj_index, int lock_type);
    int Unlock(int target_node_id, int target_obj_index, int lock_type);
    int GetCount(int target_node_id, int target_obj_index, int lock_type);
    int GetStatus(int target_node_id, int target_obj_index);
    void SetStatus(int target_node_id, int target_obj_index, int status);
    queue<LocalLockWaitElement>& GetQueue(int target_node_id, int target_obj_index);
  private:
    int owner_node_id_;
    unordered_map<int, unordered_map<int, int> > shared_counter_;
    unordered_map<int, unordered_map<int, int> > exclusive_counter_;
    unordered_map<int, unordered_map<int, int> > lock_status_;
    unordered_map<int, unordered_map<int, queue<LocalLockWaitElement> > > wait_queue_;
};

}}

#endif
