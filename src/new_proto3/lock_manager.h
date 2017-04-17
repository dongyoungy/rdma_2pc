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
#include <unordered_map>
#include <map>
#include <set>

#include "tbb/concurrent_unordered_map.h"
#include "local_lock_manager.h"
#include "local_work_queue.h"
#include "lock_wait_queue.h"
#include "lock_simulator.h"
#include "context.h"
#include "client.h"

using namespace std;

namespace rdma { namespace proto {

class LockSimulator;
class LockClient;
class CommunicationClient;

class LockManager {

  public:
    LockManager(const string& work_dir, uint32_t rank, int num_manager,
        int num_lock_object, int lock_mode, int num_total_user = 0, int num_client = 1,
        int max_local_exclusive_locks = 4, int max_local_shared_locks = 16);
    ~LockManager();
    int Initialize();
    int InitializeLockClients();
    int RegisterUser(uint32_t user_id, LockSimulator* user);
    int Run();
    int Lock(int seq_no, uint32_t user_id, uint32_t manager_id, int lock_type, int obj_index);
    int Unlock(int seq_no, uint32_t user_id, uint32_t manager_id, int lock_type, int obj_index);
    int LockLocalDirect(uint32_t user_id, int lock_type, int obj_index);
    int UnlockLocalDirect(uint32_t user_id, int lock_type, int obj_index);
    int GrantLock(int seq_no, int target_node_id, int owner_node_id, int obj_index,
        int lock_type);
    int RejectLock(int seq_no, uint32_t user_id, uint32_t manager_id,
        int lock_type, int obj_index);
    int UpdateLockModeTable(int manager_id, int mode);
    int NotifyLockRequestResult(int seq_no, uint32_t user_id, int lock_type,
        int target_node_id, int obj_index,
        int result);
    int NotifyUnlockRequestResult(int seq_no, uint32_t user_id, int lock_type,
        int target_node_id, int obj_index,
        int result);
    int GetID() const;
    int GetRank() const;
    int GetLockMode() const;
    void SetTerminate(bool terminate);
    inline int GetNumManager() const {
      return num_manager_;
    }
    inline int GetNumUser() const {
      return num_user_;
    }
    inline int GetNumTotalUser() const {
      return num_total_user_;
    }
    inline int GetNumClient() const {
      return num_client_;
    }
    inline int GetCurrentLockMode() const {
      return current_lock_mode_;
    }
    inline LocalWorkQueue<Message>* GetLocalWorkQueue() {
      return local_work_queue_;
    }
    void Stop();
    bool IsClientsInitialized() const;
    double GetAverageLocalExclusiveLockTime() const;
    double GetAverageLocalSharedLockTime() const;
    double GetAverageRemoteExclusiveLockTime() const;
    double GetAverageRemoteSharedLockTime() const;
    double GetAverageSendMessageTime() const;
    double GetAverageReceiveMessageTime() const;
    double GetAverageRDMAReadCount() const;
    double GetAverageRDMAAtomicCount() const;

    uint64_t GetTotalRDMAReadCount() const;
    uint64_t GetTotalRDMASendCount() const;
    uint64_t GetTotalRDMARecvCount() const;
    uint64_t GetTotalRDMAWriteCount() const;
    uint64_t GetTotalRDMAAtomicCount() const;

    double GetTotalRDMAReadTime() const;
    double GetTotalRDMAAtomicTime() const;

    uint64_t GetTotalLockContention() const;
    uint64_t GetTotalLockSuccessWithPoll() const;
    uint64_t GetTotalSumPollWhenSuccess() const;

    int SwitchToLocal();
    int SwitchToRemote();
    static void* PollCompletionQueue(void* context);
    static void* PollLocalWorkQueue(void* arg);
    static void* RunLockClient(void* args);

    inline static int GetSharedExclusiveRule() {
      return shared_exclusive_rule_;
    }
    inline static int GetExclusiveSharedRule() {
      return exclusive_shared_rule_;
    }
    inline static int GetExclusiveExclusiveRule() {
      return exclusive_exclusive_rule_;
    }
    inline static int GetPollRetry() {
      return poll_retry_;
    }
    inline static int GetFailRetry() {
      return fail_retry_;
    }
    inline static void SetSharedExclusiveRule(int rule) {
      shared_exclusive_rule_ = rule;
    }
    inline static void SetExclusiveSharedRule(int rule) {
      exclusive_shared_rule_ = rule;
    }
    inline static void SetExclusiveExclusiveRule(int rule) {
      exclusive_exclusive_rule_ = rule;
    }
    inline static void SetPollRetry(int retry) {
      poll_retry_ = retry;
    }
    inline static void SetFailRetry(int retry) {
      fail_retry_ = retry;
    }
    inline void PrintTemp() {
       uint64_t value = lock_table_[0];
       uint32_t shared = (uint32_t)value;
       uint32_t exclusive = (uint32_t)(value >> 32);
       cout << rank_ << " = "  << shared << "," << exclusive << endl;
    }
    inline long GetLocalExclusiveToExclusiveFailCount() {
      return llm_->GetExclusiveToExclusiveFailCount();
    }
    inline long GetLocalSharedToExclusiveFailCount() {
      return llm_->GetSharedToExclusiveFailCount();
    }
    inline long GetLocalExclusiveToSharedFailCount() {
      return llm_->GetExclusiveToSharedFailCount();
    }
    inline long GetLocalMaxSharedLockCount() {
      return llm_->GetMaxSharedLockCount();
    }
    inline long GetLocalMaxExclusiveLockCount() {
      return llm_->GetMaxExclusiveLockCount();
    }
    // temp
    long local_e_e_lock_pass_count_;

    uint64_t GetNumLocalLockDirectPass() const;
    uint64_t GetNumLocalLockDirectFail() const;
    uint64_t GetNumLocalLockWaitPass() const;
    uint64_t GetNumLocalLockWaitFail() const;

    uint64_t GetRequestLockCallTime() const;
    uint64_t GetRequestLockCallCount() const;

    static const int EXCLUSIVE = 1;
    static const int SHARED = 2;

    static const int LOCK_LOCAL = 0;
    static const int LOCK_REMOTE = 1;
    static const int LOCK_ADAPTIVE = 2;

    static const int TASK_LOCK = 0;
    static const int TASK_UNLOCK = 1;

    static const int RESULT_SUCCESS = 0;
    static const int RESULT_FAILURE = 1;
    static const int RESULT_RETRY = 2;
    static const int RESULT_QUEUED = 3;

    static const int RULE_FAIL = 0;
    static const int RULE_POLL = 1;
    static const int RULE_QUEUE = 2;

    static const int MAX_USER = 65536;
    static const int NUM_LOCK_HISTORY = 10000;
    static constexpr double ADAPT_THRESHOLD = 0.8;
    static map<uint32_t, uint32_t> user_to_node_map_;
    uint64_t* lock_table_;


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
    int SendGrantLockAck(Context* context, int seq_no, uint32_t user_id, int lock_type, int obj_index);
    int SendLockRequestResult(Context* context, int seq_no, uint32_t user_id,
        int lock_type, int obj_index, int result);
    int SendUnlockRequestResult(Context* context, int seq_no, uint32_t user_id,
        int lock_type, int obj_index, int result);

    int LockLocallyWithRetry(Context* context, Message* message);
    int LockLocallyWithQueue(Context* context, Message* message);
    int UnlockLocallyWithRetry(Context* context, Message* message);
    int UnlockLocallyWithQueue(Context* context, Message* message);
    //int LockLocally(Context* context, int user_id, int lock_type,
        //int obj_index);
    //int UnlockLocally(Context* context, int user_id, int lock_type,
        //int obj_index);

    int TryLock(Context* context, Message* message);
    int DisableRemoteAtomicAccess();
    int EnableRemoteAtomicAccess();
    int HandleWorkCompletion(struct ibv_wc* work_completion);
    int HandleEvent(struct rdma_cm_event* event);
    int HandleConnectRequest(struct rdma_cm_id* id);
    int HandleConnection(Context* context);
    int HandleDisconnect(Context* context);
    void DestroyListener();

    // each client connects to each lock manager in the cluster
    map<uint64_t, LockClient*> lock_clients_;
    map<uint64_t, LockClient*> notify_lock_clients_;
    set<Context*> context_set_;
    vector<pthread_t*> lock_client_threads_;

    map<uint64_t, CommunicationClient*> communication_clients_;
    vector<pthread_t*> communication_client_threads_;

    tbb::concurrent_unordered_map<uint32_t, int> queued_user_; // map from obj index to the queued user

    // vector for actual user/clients/simulators
    vector<LockSimulator*> users;
    map<int, LockSimulator*> user_map;
    map<int, pthread_mutex_t*> user_mutex_map;
    map<uint32_t, map<uint32_t, int> > last_seq_no_map_;
    map<uint64_t, uint64_t> user_to_home_map_;

    // queue for lock waits
    //vector<LockWaitQueue*> wait_queues_;
    LockWaitQueue** wait_queues_;
    LocalWorkQueue<Message>* local_work_queue_;
    pthread_t local_work_poller_;

    // local lock manager
    LocalLockManager* llm_;

    string work_dir_;
    bool terminate_;
    uint64_t num_rdma_send_;
    uint64_t num_rdma_recv_;
    uint64_t* last_lock_table_;
    uint64_t* fail_count_;
    int* lock_mode_table_;
    uint32_t rank_;
    uint32_t id_;
    int lock_mode_;
    int current_lock_mode_;
    int proxy_fail_rule_;
    int num_manager_;
    int num_client_;
    int num_user_;
    int num_total_user_;
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
    pthread_mutex_t msg_mutex_;
    pthread_mutex_t poll_mutex_;
    pthread_mutex_t seq_mutex_;

    static int shared_exclusive_rule_;
    static int exclusive_shared_rule_;
    static int exclusive_exclusive_rule_;

    static int poll_retry_;
    static int fail_retry_;

    uint64_t num_local_lock_direct_pass_;
    uint64_t num_local_lock_direct_fail_;
    uint64_t num_local_lock_wait_pass_;
    uint64_t num_local_lock_wait_fail_;

    uint64_t request_lock_call_time_;
    uint64_t request_lock_call_count_;
};

}}

#endif
