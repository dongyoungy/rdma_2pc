#ifndef RDMA_PROTO_CLIENT_H
#define RDMA_PROTO_CLIENT_H

#include <iostream>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <map>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <rdma/rdma_cma.h>

#include "context.h"
#include "constants.h"
#include "lock_manager.h"
#include "lock_request.h"

using namespace std;

namespace rdma { namespace proto {

class LockSimulator;
class LockManager;

class Client {

  public:
    Client(const string& work_dir, LockManager* local_manager,
        LockSimulator* local_user,
        int remote_lm_id);
    ~Client();
    int Run();
    void Stop();
    Context* GetContext();
    int RequestLock(int user_id, int lock_type, int obj_index, int lock_mode);
    int RequestUnlock(int user_id, int lock_type, int obj_index, int lock_mode);
    double GetAverageSendMessageTime() const;
    double GetAverageReceiveMessageTime() const;

    static void* PollCompletionQueue(void* context);

  protected:
    Context* BuildContext(struct rdma_cm_id* id);
    void BuildQueuePairAttr(Context* context,
        struct ibv_exp_qp_init_attr* attributes);
    int BuildConnectionManagerParams(struct rdma_conn_param* params);
    int RegisterMemoryRegion(Context* context);
    int ReceiveMessage(Context* context);
    int SendMessage(Context* context);
    int ReadServerAddress();

    int HandleEvent(struct rdma_cm_event* event);
    int HandleAddressResolved(struct rdma_cm_id* id);
    int HandleRouteResolved(struct rdma_cm_id* id);
    virtual int HandleConnection(Context* context);
    virtual int HandleDisconnect(Context* context);

    virtual int HandleWorkCompletion(struct ibv_wc* work_completion) = 0;


    uint64_t num_rdma_atomic_;
    uint64_t num_rdma_read_;

    LockRequest** lock_requests_;
    LockManager* local_manager_;
    LockSimulator* local_user_;
    Context* context_;
    int local_manager_id_;
    int remote_lm_id_;
    int test_duration_;
    int lock_request_idx_;
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
    pthread_mutex_t msg_mutex_;
};

}}

#endif
