#ifndef RDMA_N_COSED_LOCKMANAGER_H
#define RDMA_N_COSED_LOCKMANAGER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <rdma/rdma_cma.h>
#include <iostream>
#include <vector>
#include <queue>
#include <map>
#include <set>

#include "lock_simulator.h"
#include "lock_client.h"
#include "context.h"

using namespace std;

namespace rdma { namespace n_cosed {

class LockSimulator;

class LockManager {

  public:
    LockManager(const string& work_dir, uint32_t rank, int num_manager,
        int num_lock_object, int lock_mode);
    ~LockManager();
    int Initialize();
    int InitializeLockClients();
    int RegisterUser(int user_id, LockSimulator* user);
    int Run();
    int Lock(int user_id, int manager_id, int lock_type, int obj_index);
    int Unlock(int user_id, int manager_id, int lock_type, int obj_index);
    int LockLocalDirect(int user_id, int lock_type, int obj_index);
    int UnlockLocalDirect(int user_id, int lock_type, int obj_index);
    int UpdateLockModeTable(int manager_id, int mode);
    int NotifyLockRequestResult(int user_id, int lock_type, int home_id, int obj_index,
        int result);
    int NotifyUnlockRequestResult(int user_id, int lock_type, int home_id, int obj_index,
        int result, bool reset_counter = false);
    int SendExclusiveToExclusiveLockRequest(int current_owner, int home_id,
        int user_id, int obj_index);
    void ResetByUnlock(int home_id, int obj_index, int shared_count = 0);
    void ResetExclusiveToExclusive(int home_id, int obj_index);
    void ResetSharedToExclusive(int home_id, int obj_index);
    int SendExclusiveToSharedLockRequest(int current_owner, int home_id, int user_id,
        int obj_index);
    int BroadcastExclusiveToSharedLockGrant(int home_id, int obj_index);
    int GetID() const;
    int GetLockMode() const;
    void Stop();
    double GetAverageLocalExclusiveLockTime() const;
    double GetAverageLocalSharedLockTime() const;
    double GetAverageRemoteExclusiveLockTime() const;
    double GetAverageRemoteSharedLockTime() const;
    double GetAverageSendMessageTime() const;
    double GetAverageReceiveMessageTime() const;
    int SwitchToLocal();
    int SwitchToRemote();
    static void* PollCompletionQueue(void* context);
    static void* RunLockClient(void* args);

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

    static const int MAX_USER = 65536;
    static const int NUM_LOCK_HISTORY = 10000;
    static const double ADAPT_THRESHOLD = 0.8;
    static pthread_mutex_t print_mutex;

    inline void PrintFirstElem() {
      if (rank_ == 1) {
        uint64_t first_value = lock_table_[0];
        uint32_t exclusive = first_value >> 32;
        uint32_t shared = (uint32_t)first_value;
        pthread_mutex_lock(&print_mutex);
        cerr << "(" << exclusive << "," << shared << ")" << endl;
        pthread_mutex_unlock(&print_mutex);
      }
    }

  private:
    Context* BuildContext(struct rdma_cm_id* id);
    int PrintInfo();
    int GetInfinibandIP(string& ip_address);
    void BuildQueuePairAttr(Context* context,
        struct ibv_exp_qp_init_attr* attributes);
    int BuildConnectionManagerParams(struct rdma_conn_param* params);
    int RegisterContext(Context* context);
    int RegisterMemoryRegion(Context* context);
    int ReceiveMessage(Context* context);
    int NotifyLockMode(Context* context);
    int NotifyLockModeAll();
    int SendMessage(Context* context);
    int SendLockTableMemoryRegion(Context* context);
    int SendLockRequestResult(Context* context, int user_id,
        int lock_type, int obj_index, int result);
    int SendUnlockRequestResult(Context* context, int user_id,
        int lock_type, int obj_index, int result);
    int SendExclusiveToSharedLockGrantAck(Context* context, int user_id,
        int home_id, int obj_index);
    int SendSharedToExclusiveLockGrantAck(Context* context,
        int home_id, int user_id, int obj_index);
    int SendExclusiveToExclusiveLockGrantAck(Context* context,
        int home_id, int user_id, int obj_index);
    int LockLocally(Context* context);
    int LockLocally(Context* context, int user_id, int lock_type,
        int obj_index);
    int UnlockLocally(Context* context);
    int UnlockLocally(Context* context, int user_id, int lock_type,
        int obj_index);
    int DisableRemoteAtomicAccess();
    int EnableRemoteAtomicAccess();
    int UpdateLockTableLocal(Context* context);
    int UpdateLockTableRemote(Context* context);
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleEvent(struct rdma_cm_event* event);
    int HandleConnectRequest(struct rdma_cm_id* id);
    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);

    int HandleSharedLockRelease(Context* context);

    int HandleExclusiveToSharedLockRequest(Context* context);
    int HandleExclusiveToSharedLockGrant(Context* context);

    int HandleSharedToExclusiveLockRequest(Context* context);
    int HandleExclusiveToExclusiveLockRequest(Context* context);
    int HandleSharedToExclusiveLockGrant(Context* context);
    int HandleExclusiveToExclusiveLockGrant(Context* context);
    void DestroyListener();

    // each client connects to each lock manager in the cluster
    map<int, LockClient*> lock_clients_;
    // dedicated lock clients for communication
    map<int, LockClient*> communication_clients_;
    map<int, queue<int>* > shared_lock_owner_waitlist_;
    map<int, queue<int>* > shared_lock_user_id_waitlist_;
    //map<int, int> shared_lock_counter_;
    //map<int, int> next_exclusive_lock_owner_;
    set<Context*> context_set_;
    vector<pthread_t*> lock_client_threads_;

    // vector for actual user/clients/simulators
    vector<LockSimulator*> users;
    map<int, LockSimulator*> user_map;
    map<int, pthread_mutex_t*> user_mutex_map;

    LockClient* local_client_; // dedicated local lock client
    string work_dir_;
    uint64_t* lock_table_;
    bool* has_unlocked_shared_;
    int* shared_lock_counter_;
    int* shared_lock_to_receive_;
    //int* node_to_release_shared_lock_;
    int* node_to_send_shared_release_;
    int* node_to_unlock_exclusive_shared_;
    int* shared_to_exclusive_home_id_;
    int* shared_to_exclusive_user_id_;
    int* exclusive_to_exclusive_home_id_;
    int* exclusive_to_exclusive_user_id_;
    int* exclusive_lock_holders_;
    int* has_unlocked_exclusive_;
    int* lock_mode_table_;
    uint32_t rank_;
    int lock_mode_;
    int current_lock_mode_;
    int local_client_id_;
    int num_manager_;
    int num_lock_object_;
    int num_local_lock_;
    int num_remote_lock_;
    struct ibv_mr* registered_memory_region_;
    struct rdma_cm_id* listener_;
    struct rdma_event_channel* event_channel_;
    struct sockaddr_in6 address_;
    struct timespec start_local_lock_;
    struct timespec end_local_lock_;
    double total_local_exclusive_lock_time_;
    double total_local_shared_lock_time_;
    double num_local_exclusive_lock_;
    double num_local_shared_lock_;
    uint16_t port_;
    size_t data_size_;
    pthread_mutex_t mutex_;
    pthread_mutex_t** lock_mutex_;

};

}}

#endif
