#ifndef RDMA_PROTO_LOCKCLIENT_H
#define RDMA_PROTO_LOCKCLIENT_H

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>
#include <iostream>

#include "lock_manager.h"
#include "context.h"

using namespace std;

namespace rdma { namespace proto {

class LockClient {

  public:
    LockClient(const string& work_dir, LockManager* local_manager,
        int remote_lm_id);
    ~LockClient();
    int Run();
    void Stop();
    int RequestLock(int user_id, int lock_type, int obj_index, int lock_mode);
    int RequestUnlock(int user_id, int lock_type, int obj_index);

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
    int LockRemotely(Context* context, int user_id, int lock_type,
        int obj_index);
    int SendLockTableRequest(Context* context);
    int SendLockRequest(Context* context, int user_id,
        int lock_type, int obj_index);
    int SendUnlockRequest(Context* context, int user_id,
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

    LockManager* local_manager_;
    Context* context_;
    int remote_lm_id_;
    int test_duration_;
    string work_dir_;
    double total_cas_time_;
    double total_read_time_;
    int test_mode_;
    int num_trial_;
    string server_name_;
    string server_port_;
    struct rdma_event_channel* event_channel_;
    struct rdma_cm_id* connection_;
    struct addrinfo* address_;
    struct timespec start_;
    struct timespec end_;
    uint64_t current_semaphore_;
    size_t data_size_;
    time_t test_start_;
    time_t test_end_;
};

}}

#endif
