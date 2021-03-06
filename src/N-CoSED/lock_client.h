#ifndef RDMA_N_COSED_LOCKCLIENT_H
#define RDMA_N_COSED_LOCKCLIENT_H

#include <iostream>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

#include "lock_manager.h"
#include "context.h"

using namespace std;

namespace rdma { namespace n_cosed {

class LockSimulator;

class LockClient {

  public:
    LockClient(const string& work_dir, LockManager* local_manager,
        LockSimulator* local_user,
        int remote_lm_id);
    ~LockClient();
    int Run();
    void Stop();
    Context* GetContext();
    int RequestLock(int home_id, int user_id, int lock_type, int obj_index, int lock_mode,
        uint64_t old_value = 0);
    int RequestUnlock(int home_id, int user_id, int lock_type, int obj_index,
        int lock_mode, uint64_t old_value = 0, int last_user_id = -1);
    int SendSharedUnlockRequestResult(int home_id, int obj_index, int result);
    int SendSharedToExclusiveLockRequest(int new_owner, int home_id,
        int user_id, int obj_index, int shared_count = 0);
    int SendExclusiveToExclusiveLockRequest(int new_owner, int home_id,
        int user_id, int obj_index);
    int SendExclusiveToSharedLockRequest(int current_owner, int new_owner, int home_id,
        int user_id, int obj_index);
    int SendSharedToExclusiveLockGrant(int home_id, int user_id, int obj_index);
    int SendExclusiveToExclusiveLockGrant(int home_id, int prev_user_id, int user_id, int obj_index);
    int SendExclusiveToSharedLockGrant(int current_owner, int new_owner, int home_id, int user_id, int obj_index);
    int UnlockShared(int home_id, int user_id, int obj_index, int count);
    //int SendSwitchToLocal();
    //int SendSwitchToRemote();
    double GetAverageRemoteExclusiveLockTime() const;
    double GetAverageRemoteSharedLockTime() const;
    double GetAverageSendMessageTime() const;
    double GetAverageReceiveMessageTime() const;

    static void* PollCompletionQueue(void* context);

  private:
    Context* BuildContext(struct rdma_cm_id* id);
    void BuildQueuePairAttr(Context* context,
        struct ibv_exp_qp_init_attr* attributes);
    int BuildConnectionManagerParams(struct rdma_conn_param* params);
    int RegisterMemoryRegion(Context* context);
    int ReceiveMessage(Context* context);
    int SendMessage(Context* context);
    int ReadServerAddress();
    int LockRemotely(Context* context, int home_id, int user_id, int lock_type,
        int obj_index, uint64_t old_value = 0);
    int UnlockRemotely(Context* context, int home_id, int user_id, int lock_type,
        int obj_index, uint64_t old_value = 0, int last_user_id = -1);
    int SendLockTableRequest(Context* context);
    int SendLockModeRequest(Context* context);
    int SendLockRequest(Context* context, int user_id,
        int lock_type, int obj_index);
    int SendUnlockRequest(Context* context, int home_id, int user_id,
        int lock_type, int obj_index);
    int HandleEvent(struct rdma_cm_event* event);
    int HandleAddressResolved(struct rdma_cm_id* id);
    int HandleRouteResolved(struct rdma_cm_id* id);
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);


    static const int TOTAL_TRIAL = 1000000;
    static const int TEST_MODE_SEM = 0;
    static const int TEST_MODE_DATA = 1;

    bool previous_unlock_shared_running_;
    LockManager* local_manager_;
    LockSimulator* local_user_;
    Context* context_;
    int remote_lm_id_;
    int test_duration_;
    string work_dir_;
    double total_send_message_time_;
    double num_send_message_;
    double total_receive_message_time_;
    double num_receive_message_;
    double total_exclusive_lock_remote_time_;
    double total_shared_lock_remote_time_;
    double num_exclusive_lock_;
    double num_shared_lock_;
    int test_mode_;
    int num_trial_;
    string server_name_;
    string server_port_;
    struct rdma_event_channel* event_channel_;
    struct rdma_cm_id* connection_;
    struct addrinfo* address_;
    struct timespec start_;
    struct timespec end_;
    struct timespec start_remote_exclusive_lock_;
    struct timespec end_remote_exclusive_lock_;
    struct timespec start_remote_shared_lock_;
    struct timespec end_remote_shared_lock_;
    struct timespec start_send_message_;
    struct timespec end_send_message_;
    struct timespec start_receive_message_;
    struct timespec end_receive_message_;
    uint64_t current_semaphore_;
    size_t data_size_;
    time_t test_start_;
    time_t test_end_;
    pthread_mutex_t lock_mutex_;
    pthread_cond_t cond_;
};

}}

#endif
