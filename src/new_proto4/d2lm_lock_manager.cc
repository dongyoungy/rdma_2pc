#include "d2lm_lock_manager.h"

namespace rdma {
namespace proto {

// constructor
D2LMLockManager::D2LMLockManager(const string& work_dir, uint32_t rank,
                                 int num_manager, int num_lock_object,
                                 LockMode lock_mode, int num_total_user,
                                 int num_client) {
  work_dir_ = work_dir;
  rank_ = rank;
  id_ = (uint32_t)pow(2.0, rank_);
  num_manager_ = num_manager;
  num_client_ = num_client;
  num_total_user_ = num_total_user;
  num_lock_object_ = num_lock_object;
  lock_table_ = new uint64_t[num_lock_object_ * 3];
  last_lock_table_ = new uint64_t[num_lock_object_];
  fail_count_ = new uint64_t[num_lock_object_];
  lock_mode_table_ = new LockMode[num_manager_];
  listener_ = NULL;
  event_channel_ = NULL;
  registered_memory_region_ = NULL;
  wait_queues_ = NULL;
  port_ = 0;
  lock_mode_ = lock_mode;
  total_local_exclusive_lock_time_ = 0;
  total_local_shared_lock_time_ = 0;
  num_local_exclusive_lock_ = 0;
  num_local_shared_lock_ = 0;
  num_local_lock_ = 0;
  num_remote_lock_ = 0;
  num_rdma_send_ = 0;
  num_rdma_recv_ = 0;
  terminate_ = false;

  local_e_e_lock_pass_count_ = 0;

  num_local_lock_direct_pass_ = 0;
  num_local_lock_direct_fail_ = 0;
  num_local_lock_wait_pass_ = 0;
  num_local_lock_wait_fail_ = 0;

  request_lock_call_time_ = 0;
  request_lock_call_count_ = 0;

  current_lock_mode_ = lock_mode_;

  // initialize lock table with 0
  memset(lock_table_, 0x00, num_lock_object_ * 3 * sizeof(uint64_t));
  memset(last_lock_table_, 0x00, num_lock_object_ * sizeof(uint64_t));
  memset(fail_count_, 0x00, num_lock_object_ * sizeof(uint64_t));

  lock_mode_table_[rank_] = current_lock_mode_;

  // initialize local lock mutex
  lock_mutex_ = new pthread_mutex_t*[num_lock_object_];
  pthread_mutex_init(&msg_mutex_, NULL);
  pthread_mutex_init(&poll_mutex_, NULL);
  pthread_mutex_init(&seq_mutex_, NULL);
}

// Currently the server registers same memory region for each client.
int D2LMLockManager::RegisterMemoryRegion(Context* context) {
  context->send_message_buffer.reset(new MessageBuffer);
  context->receive_message_buffer.reset(new MessageBuffer);

  context->lock_table = lock_table_;

  if (context->send_message_buffer->Register(context)) {
    cerr << "MessageBuffer::Register failed()" << endl;
    return -1;
  }
  if (context->receive_message_buffer->Register(context)) {
    cerr << "MessageBuffer::Register failed()" << endl;
    return -1;
  }

  context->lock_table_mr =
      ibv_reg_mr(context->protection_domain, context->lock_table,
                 num_lock_object_ * kNumFields * sizeof(uint64_t),
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  if (context->lock_table_mr == NULL) {
    cerr << "ibv_reg_mr() failed for lock_table_mr." << endl;
    return -1;
  }

  return 0;
}

}  // namespace proto
}  // namespace rdma
