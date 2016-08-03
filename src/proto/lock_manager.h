#ifndef RDMA_PROTO_LOCKMANAGER_H
#define RDMA_PROTO_LOCKMANAGER_H

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
#include <map>
#include <set>

#include "lock_simulator.h"
#include "lock_client.h"
#include "context.h"

using namespace std;

namespace rdma { namespace proto {

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
    int NotifyLockRequestResult(int user_id, int lock_type, int obj_index,
        int result);
    int NotifyUnlockRequestResult(int user_id, int lock_type, int obj_index,
        int result);
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
    void DestroyListener();

    // each client connects to each lock manager in the cluster
    map<int, LockClient*> lock_clients_;
    set<Context*> context_set_;
    vector<pthread_t*> lock_client_threads_;

    // vector for actual user/clients/simulators
    vector<LockSimulator*> users;
    map<int, LockSimulator*> user_map;
    map<int, pthread_mutex_t*> user_mutex_map;

    string work_dir_;
    uint64_t* lock_table_;
    int* lock_mode_table_;
    uint32_t rank_;
    int lock_mode_;
    int current_lock_mode_;
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
    pthread_mutex_t** lock_mutex_;

};

}}

#endif
