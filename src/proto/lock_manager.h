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
    int NotifyLockRequestResult(int user_id, int lock_type, int obj_index,
        bool result);
    int NotifyUnlockRequestResult(int user_id, int lock_type, int obj_index,
        bool result);
    void Stop();
    static void* PollCompletionQueue(void* context);
    static void* RunLockClient(void* args);

    static const int EXCLUSIVE = 0;
    static const int SHARED = 1;

    static const int LOCK_LOCAL = 0;
    static const int LOCK_REMOTE = 1;

    static const int TASK_LOCK = 0;
    static const int TASK_UNLOCK = 1;

  private:
    Context* BuildContext(struct rdma_cm_id* id);
    int PrintInfo();
    int GetInfinibandIP(string& ip_address);
    int GetID() const;
    void BuildQueuePairAttr(Context* context,
        struct ibv_exp_qp_init_attr* attributes);
    int BuildConnectionManagerParams(struct rdma_conn_param* params);
    int RegisterMemoryRegion(Context* context);
    int ReceiveMessage(Context* context);
    int SendMessage(Context* context);
    int SendLockTableMemoryRegion(Context* context);
    int SendLockRequestResult(Context* context, int user_id,
        int lock_type, int obj_index, bool result);
    int SendUnlockRequestResult(Context* context, int user_id,
        int lock_type, int obj_index, bool result);
    int LockLocally(Context* context);
    int UnlockLocally(Context* context);
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleEvent(struct rdma_cm_event* event);
    int HandleConnectRequest(struct rdma_cm_id* id);
    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);
    void DestroyListener();

    // each client connects to each lock manager in the cluster
    vector<LockClient*> lock_clients_;
    vector<pthread_t*> lock_client_threads_;

    // map for actual user/sclients/simulators
    map<int, LockSimulator*> user_map;

    string work_dir_;
    uint64_t* lock_table_;
    uint32_t rank_;
    int lock_mode_;
    int num_manager_;
    int num_lock_object_;
    struct ibv_mr* registered_memory_region_;
    struct rdma_cm_id* listener_;
    struct rdma_event_channel* event_channel_;
    struct sockaddr_in6 address_;
    uint16_t port_;
    size_t data_size_;
    pthread_mutex_t lock_mutex_;

};

}}

#endif
