#include <iostream>
#include <vector>
#include <cmath>
#include <arpa/inet.h>
#include <limits.h>
#include <pthread.h>
#include <infiniband/verbs.h>
#include <sys/times.h>
#include "mpi.h"
#include "tpcc_lock_simulator.h"
#include "lock_manager.h"

using namespace std;
using namespace rdma::proto;

void* RunLockManager(void* args);
void* RunLockSimulator(void* args);
void* MeasureCPUUsage(void* args);

struct CPUUsage {
  double total_cpu;
  double num_sample;
  bool terminate;
};

int main(int argc, char** argv) {

  MPI_Init(&argc, &argv);

  if (argc != 20) {
    cout << argv[0] << " <work_dir> <workload_type> <num_tx>" <<
      " <num_warehouses_per_node> <num_users> <lock_mode>" <<
      " <shared_exclusive_rule> <exclusive_shared_rule> <exclusive_exclusive_rule>" <<
      " <fail_retry> <poll_retry> <sleep_time_for_timeout> <think_time>"
      " <transaction_delay> <transaction_delay_min> " <<
      "<transaction_delay_max> <miin_backoff_time> <max_backoff_time> <rand_seed>" << endl;
    exit(1);
  }

  int num_nodes, rank;
  int num_servers, num_clients;
  int client_start_idx;

  MPI_Comm_size(MPI_COMM_WORLD, &num_nodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0) {
    if (1 == htons(1)) {
      cout << "The current machine uses BIG ENDIAN" << endl;
    } else {
      cout << "The current machine uses LITTLE ENDIAN" << endl;
    }
  }

  int k=2;
  int workload_type            = atoi(argv[k++]);
  long num_tx                  = atol(argv[k++]);
  int num_warehouses_per_node  = atoi(argv[k++]);
  int num_users                = atoi(argv[k++]);
  int lock_mode                = atoi(argv[k++]);
  int shared_exclusive_rule    = atoi(argv[k++]);
  int exclusive_shared_rule    = atoi(argv[k++]);
  int exclusive_exclusive_rule = atoi(argv[k++]);
  int fail_retry               = atoi(argv[k++]);
  int poll_retry               = atoi(argv[k++]);
  int sleep_time               = atoi(argv[k++]);
  int think_time               = atoi(argv[k++]);
  int transaction_delay_num    = atoi(argv[k++]);
  int transaction_delay_min    = atoi(argv[k++]);
  int transaction_delay_max    = atoi(argv[k++]);
  int min_backoff_time         = atoi(argv[k++]);
  int max_backoff_time         = atoi(argv[k++]);
  long seed                    = atol(argv[k++]);

  //if (num_users != 1) {
     //cerr << "# of users must be 1." << endl;
     //exit(-1);
  //}
  //if (num_users > 32) {
     //cerr << "# of node/clients must be less than 33" << endl;
     //exit(-1);
  //}

  // 1 node = server/client on the same node
  // 2 node = 1 server, 1 client
  // n nodes = n/2 servers, n/2 clients
  //if (num_nodes == 1) {
    //num_servers = 1;
    //num_clients = 1;
    //server_start_idx = 0;
    //client_start_idx = 0;
  //} else if (num_nodes == 2) {
    //num_servers = 1;
    //num_clients = 1;
    //server_start_idx = 0;
    //client_start_idx = 1;
  ////} else if (num_nodes == 5) {
    ////num_servers = 1;
    ////num_clients = 2;
    ////server_start_idx = 0;
    ////client_start_idx = 3;
  //} else {
    //num_servers = num_nodes;
    //num_clients = num_nodes;
    //server_start_idx = 0;
    //client_start_idx = 0;
  //}

  num_servers = num_nodes;
  num_clients = num_nodes;
  client_start_idx = 0;

  bool transaction_delay = (transaction_delay_num == 0) ? false : true;

  string lock_mode_str;
  if (lock_mode == LOCK_PROXY_RETRY) {
    lock_mode_str = "SERVER-BASED/PROXY/RETRY";
  } else if (lock_mode == LOCK_PROXY_QUEUE) {
    lock_mode_str = "SERVER-BASED/PROXY/QUEUE";
  } else if (lock_mode == LOCK_REMOTE_POLL) {
    lock_mode_str = "CLIENT-BASED/DIRECT/RETRY";
  } else if (lock_mode == LOCK_REMOTE_NOTIFY) {
    lock_mode_str = "CLIENT-BASED/DIRECT/NOTIFY";
  } else if (lock_mode == LOCK_REMOTE_QUEUE) {
    lock_mode_str = "CLIENT-BASED/DIRECT/QUEUE";
  }

  string workload_type_str, shared_lock_ratio_str;
  workload_type_str = "TPC-C";
  shared_lock_ratio_str = "N/A";
  string local_workload_ratio_str = "N/A";

  string transaction_delay_str = (transaction_delay ? "TRUE" : "FALSE");
  string shared_exclusive_rule_str, exclusive_shared_rule_str, exclusive_exclusive_rule_str;
  switch (shared_exclusive_rule) {
    case RULE_FAIL:
      shared_exclusive_rule_str = "FAIL";
      break;
    case RULE_POLL:
      shared_exclusive_rule_str = "POLL";
      break;
    case RULE_QUEUE:
      shared_exclusive_rule_str = "QUEUE";
      break;
    default:
      cerr << "Unsupported Rule: " << shared_exclusive_rule << endl;
      exit(-1);
  }
  switch (exclusive_shared_rule) {
    case RULE_FAIL:
      exclusive_shared_rule_str = "FAIL";
      break;
    case RULE_POLL:
      exclusive_shared_rule_str = "POLL";
      break;
    case RULE_QUEUE:
      exclusive_shared_rule_str = "QUEUE";
      break;
    default:
      cerr << "Unsupported Rule: " << exclusive_shared_rule << endl;
      exit(-1);
  }
  switch (exclusive_exclusive_rule) {
    case RULE_FAIL:
      exclusive_exclusive_rule_str = "FAIL";
      break;
    case RULE_POLL:
      exclusive_exclusive_rule_str = "POLL";
      break;
    case RULE_QUEUE:
      exclusive_exclusive_rule_str = "QUEUE";
      break;
    default:
      cerr << "Unsupported Rule: " << exclusive_exclusive_rule << endl;
      exit(-1);
  }

  if (lock_mode == LOCK_REMOTE_NOTIFY || lock_mode == LOCK_PROXY_RETRY ||
      lock_mode == LOCK_PROXY_QUEUE || lock_mode == LOCK_REMOTE_QUEUE) {
    shared_exclusive_rule_str = "N/A";
    exclusive_shared_rule_str = "N/A";
    exclusive_exclusive_rule_str = "N/A";
  }

  if (workload_type == WORKLOAD_UNIFORM) {
    workload_type_str = "TPC-C/UNIFORM";
  } else if (workload_type == WORKLOAD_HOTSPOT) {
    workload_type_str = "TPC-C/HOTSPOT";
  }

  int num_users_per_client = num_users;
  if (rank == 0) {
    cout << "Type of Workload = " << workload_type_str << endl;
    cout << "SHARED -> EXCLUSIVE = " << shared_exclusive_rule_str << endl;
    cout << "EXCLUSIVE -> SHARED = " << exclusive_shared_rule_str << endl;
    cout << "EXCLUSIVE -> EXCLUSIVE = " << exclusive_exclusive_rule_str << endl;
    cout << "Num Tx = " << num_tx << endl;
    cout << "Num Nodes = " << num_nodes << endl;
    cout << "Num Warehouse/Managers Per Node = " << num_warehouses_per_node << endl;
    cout << "Num Users Per Node = " << num_users_per_client << endl;
  }

  LockManager::SetSharedExclusiveRule(shared_exclusive_rule);
  LockManager::SetExclusiveSharedRule(exclusive_shared_rule);
  LockManager::SetExclusiveExclusiveRule(exclusive_exclusive_rule);
  LockManager::SetFailRetry(fail_retry);
  LockManager::SetPollRetry(poll_retry);

  vector<LockManager*> managers;
  vector<LockSimulator*> users;
  for (int i = 0; i < num_warehouses_per_node; ++i) {
    LockManager* lock_manager = new LockManager(argv[1], rank*num_warehouses_per_node+i,
        num_nodes*num_warehouses_per_node,
        700000, lock_mode, num_users, num_clients);
    managers.push_back(lock_manager);

    if (lock_manager->Initialize()) {
      cerr << "LockManager initialization failure." << endl;
      exit(-1);
    }

    pthread_t lock_manager_thread;
    if (pthread_create(&lock_manager_thread, NULL, RunLockManager,
          (void*)lock_manager)) {
      cerr << "pthread_create() error." << endl;
      exit(-1);
    }
  }

  if (rank >= client_start_idx) {
    for (int i=0;i<num_users_per_client;++i) {
      uint32_t seq = (rank-client_start_idx)*num_users_per_client+i;
      //uint32_t id = (uint32_t)pow(2.0, seq);
      uint32_t id = i + 1;
      bool verbose = false;
      TPCCLockSimulator* simulator = new TPCCLockSimulator(managers[i%num_warehouses_per_node],
          id, // id
          //seq % (num_servers*num_warehouses_per_node), // home id
          managers[i%num_warehouses_per_node]->GetRank(), // home id
          workload_type,
          num_servers*num_warehouses_per_node,
          num_tx,
          seed+i+(rank*num_users_per_client),
          verbose, // verbose
          true, // measure lock time
          lock_mode,
          transaction_delay,
          transaction_delay_min,
          transaction_delay_max,
          min_backoff_time,
          max_backoff_time,
          sleep_time,
          think_time
          );
      //lock_manager->RegisterUser(id, simulator);
      managers[i % num_warehouses_per_node]->RegisterUser(id, simulator);
      users.push_back(simulator);
    }
  }

  for (int i = client_start_idx; i < num_nodes; ++i) {
    for (int j =0;j<num_users_per_client;++j) {
      uint32_t seq = (i-client_start_idx)*num_users_per_client+j;
      LockManager::user_to_node_map_[seq] = i;
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  for (unsigned int i = 0; i < managers.size(); ++i) {
    if (managers[i]->InitializeLockClients()) {
      cerr << "InitializeLockClients() failed." << endl;
      exit(-1);
    }
    while (!managers[i]->IsClientsInitialized()) {
      usleep(250000);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  time_t start_time;
  time_t end_time;

  time(&start_time);

  if (rank >= client_start_idx) {
    for (unsigned int i=0;i<users.size();++i) {
      pthread_t lock_simulator_thread;
      if (pthread_create(&lock_simulator_thread, NULL, &RunLockSimulator,
            (void*)users[i])) {
        cerr << "pthread_create() error." << endl;
        exit(-1);
      }
    }
  }

  // measure cpu usage
  pthread_t cpu_measure_thread;
  CPUUsage usage;
  if (pthread_create(&cpu_measure_thread, NULL, &MeasureCPUUsage,
        (void*)&usage)) {
    cerr << "pthread_create() error." << endl;
    exit(-1);
  }

  int min_time_taken = 0;
  int time_taken2 = 0;
  int time_taken3 = 0;
  uint64_t* tx_done = new uint64_t[64000];
  uint64_t* locks_done = new uint64_t[64000];
  memset(tx_done, 0x00, sizeof(uint64_t)*64000);
  memset(locks_done, 0x00, sizeof(uint64_t)*64000);

  bool simulator_done = false;
  while (rank >= client_start_idx) {
    for (unsigned int i=0;i<users.size();++i) {
      tx_done[time_taken2] += users[i]->GetCount();
      locks_done[time_taken2] += users[i]->GetTotalNumLockSuccess();
      if (users[i]->GetState() == LockSimulator::STATE_DONE) {
        simulator_done = true;
      }
    }
    cout << rank << "," << time_taken2 << ":" << tx_done[time_taken2] << endl;
    ++time_taken2;
    //cout << rank << "," << users[0]->GetID() << "," << time_taken2 << " : " << users[0]->GetCount() <<
      //"," << users[0]->GetCurrentBackoff() << endl;
    if (simulator_done) break;
    sleep(1);
  }
  time_taken3 = time_taken2;

  for (unsigned int i=0;i<users.size();++i) {
    LockSimulator* simulator = users[i];
    while (simulator->GetState() != LockSimulator::STATE_DONE) {
      ++time_taken2;
      //if (time_taken2 > 40) {
        //users[i]->SetVerbose(true);
      //}
      //if (rank == 0) {
        cout << rank << "," << users[i]->GetID() << "," << time_taken2 << " : " << users[i]->GetCount() <<
          "," << users[i]->GetCurrentBackoff() << endl;
      //}
      sleep(1);
    }
  }

  //int temp1 = 0;
  //while (rank < client_start_idx) {
    //sleep(1);
    //++temp1;
    //if (temp1 > 30) {
       //lock_manager->PrintTemp();
    //}
  //}

  time(&end_time);
  double time_taken = difftime(end_time, start_time);

  long local_sum                           = 0;
  long global_sum                          = 0;
  long local_unlock_sum                    = 0;
  long global_unlock_sum                   = 0;
  long local_lock_success                  = 0;
  long global_lock_success                 = 0;
  long local_lock_contention               = 0;
  long global_lock_contention              = 0;
  long local_lock_success_with_retry       = 0;
  long global_lock_success_with_retry      = 0;
  long local_lock_success_with_poll        = 0;
  long global_lock_success_with_poll       = 0;
  long local_sum_poll_when_success         = 0;
  long global_sum_poll_when_success        = 0;
  long local_sum_retry_when_success        = 0;
  long global_sum_retry_when_success       = 0;
  long local_sum_index_when_timeout        = 0;
  long global_sum_index_when_timeout       = 0;
  long local_lock_failure                  = 0;
  long global_lock_failure                 = 0;
  long local_timeout                       = 0;
  long global_timeout                      = 0;
  double local_lock_time                   = 0;
  double global_lock_time                  = 0;
  double local_cpu_usage                   = 0;
  double global_cpu_usage                  = 0;
  double global_cpu_usage_avg              = 0;
  double local_remote_exclusive_lock_time  = 0;
  double global_remote_exclusive_lock_time = 0;
  double local_remote_shared_lock_time     = 0;
  double global_remote_shared_lock_time    = 0;
  double local_local_exclusive_lock_time   = 0;
  double global_local_exclusive_lock_time  = 0;
  double local_local_shared_lock_time      = 0;
  double global_local_shared_lock_time     = 0;
  double local_send_message_time           = 0;
  double global_send_message_time          = 0;
  double local_receive_message_time        = 0;
  double global_receive_message_time       = 0;
  double local_99_lock_time                = 0;
  double global_99_lock_time               = 0;
  double local_95_lock_time                = 0;
  double global_95_lock_time               = 0;

  double local_avg_rdma_read_count = 0;
  double global_avg_rdma_read_count = 0;
  double local_avg_rdma_atomic_count = 0;
  double global_avg_rdma_atomic_count = 0;

  double local_rdma_read_time = 0;
  double global_rdma_read_time = 0;
  double local_rdma_atomic_time = 0;
  double global_rdma_atomic_time = 0;

  long local_rdma_read_count = 0;
  long local_rdma_send_count = 0;
  long local_rdma_recv_count = 0;
  long local_rdma_atomic_count = 0;
  long local_rdma_write_count = 0;

  long global_rdma_read_count = 0;
  long global_rdma_send_count = 0;
  long global_rdma_recv_count = 0;
  long global_rdma_atomic_count = 0;
  long global_rdma_write_count = 0;

  double local_cpu_diff = 0;
  double local_time_taken_diff = 0;
  double local_time_taken = 0;
  double local_time_taken_sum = 0;
  double global_cpu_diff = 0;
  double global_time_taken_sum = 0;
  double global_time_taken_avg = 0;
  double global_cpu_usage_std = 0;
  double global_time_taken_std = 0;
  double global_time_taken_diff = 0;

  MPI_Barrier(MPI_COMM_WORLD);

  if (time_taken3 == 0) {
    time_taken3 = INT_MAX;
  }

  // calculate max throughput
  MPI_Allreduce(&time_taken3, &min_time_taken, 1, MPI_INT, MPI_MIN,
      MPI_COMM_WORLD);

  uint64_t* local_tx_throughput = new uint64_t[min_time_taken];
  uint64_t* global_tx_throughput = new uint64_t[min_time_taken];
  uint64_t* local_lock_throughput = new uint64_t[min_time_taken];
  uint64_t* global_lock_throughput = new uint64_t[min_time_taken];

  for (int i = 0; i < min_time_taken; ++i) {
    if (i == 0) {
      local_tx_throughput[i] = tx_done[i];
      local_lock_throughput[i] = locks_done[i];
    } else {
      local_tx_throughput[i] = tx_done[i] - tx_done[i-1];
      local_lock_throughput[i] = locks_done[i] - locks_done[i-1];
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  for (int i = 0; i < min_time_taken; ++i) {
    uint64_t local = local_tx_throughput[i];
    uint64_t global;
    MPI_Reduce(&local, &global, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
        MPI_COMM_WORLD);
    global_tx_throughput[i] = global;
    MPI_Barrier(MPI_COMM_WORLD);
    local = local_lock_throughput[i];
    MPI_Reduce(&local, &global, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
        MPI_COMM_WORLD);
    global_lock_throughput[i] = global;
    MPI_Barrier(MPI_COMM_WORLD);
  }

  uint64_t max_tx_throughput = 0;
  uint64_t max_lock_throughput = 0;

  if (rank == 0) {
    for (int i = 0; i < min_time_taken; ++i) {
      if (max_tx_throughput < global_tx_throughput[i])
        max_tx_throughput = global_tx_throughput[i];

      if (max_lock_throughput < global_lock_throughput[i])
        max_lock_throughput = global_lock_throughput[i];
    }
  }

  if (rank >= client_start_idx) {
    for (unsigned int j=0;j<users.size();++j) {
      LockSimulator* simulator = users[j];
      local_sum += simulator->GetTotalNumLocks();
      local_unlock_sum += simulator->GetTotalNumUnlocks();
      local_lock_success += simulator->GetTotalNumLockSuccess();
      local_lock_success_with_retry += simulator->GetTotalNumLockSuccessWithRetry();
      local_sum_retry_when_success += simulator->GetSumRetryWhenSuccess();
      local_sum_index_when_timeout += simulator->GetSumIndexWhenTimeout();
      local_lock_failure += simulator->GetTotalNumLockFailure();
      local_timeout += simulator->GetTotalNumTimeout();
      if (simulator->IsLockTimeMeasured()) {
        local_lock_time += simulator->GetAverageTimeTakenToLock();
        local_99_lock_time += simulator->Get99PercentileLockTime();
        local_95_lock_time += simulator->Get95PercentileLockTime();
      }
    }
    for (unsigned int j=0;j<users.size();++j) {
      LockSimulator* simulator = users[j];
      local_time_taken_sum += simulator->GetTimeTaken();
    }
  }

  for (int i = 0; i < num_warehouses_per_node; ++i) {
    LockManager* lock_manager = managers[i];
    local_lock_contention += lock_manager->GetTotalLockContention();
    local_lock_success_with_poll += lock_manager->GetTotalLockSuccessWithPoll();
    local_sum_poll_when_success += lock_manager->GetTotalSumPollWhenSuccess();

    local_remote_shared_lock_time +=
      lock_manager->GetAverageRemoteSharedLockTime();
    local_remote_exclusive_lock_time +=
      lock_manager->GetAverageRemoteExclusiveLockTime();
    local_local_shared_lock_time +=
      lock_manager->GetAverageLocalSharedLockTime();
    local_local_exclusive_lock_time +=
      lock_manager->GetAverageLocalExclusiveLockTime();
    local_send_message_time += lock_manager->GetAverageSendMessageTime();
    local_receive_message_time += lock_manager->GetAverageReceiveMessageTime();
    local_avg_rdma_read_count += lock_manager->GetAverageRDMAReadCount();
    local_avg_rdma_atomic_count += lock_manager->GetAverageRDMAAtomicCount();

    local_rdma_read_count += lock_manager->GetTotalRDMAReadCount();
    local_rdma_recv_count += lock_manager->GetTotalRDMARecvCount();
    local_rdma_send_count += lock_manager->GetTotalRDMASendCount();
    local_rdma_write_count += lock_manager->GetTotalRDMAWriteCount();
    local_rdma_atomic_count += lock_manager->GetTotalRDMAAtomicCount();

    local_rdma_read_time += lock_manager->GetTotalRDMAReadTime();
    local_rdma_atomic_time += lock_manager->GetTotalRDMAAtomicTime();
  }
  MPI_Barrier(MPI_COMM_WORLD);
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  double local_cpu_usage_local = usage.total_cpu / usage.num_sample;
  local_cpu_usage = local_cpu_usage_local;
  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Reduce(&local_sum, &global_sum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_unlock_sum, &global_unlock_sum, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_lock_success, &global_lock_success, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_lock_failure, &global_lock_failure, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_lock_time, &global_lock_time, 1, MPI_DOUBLE, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_timeout, &global_timeout, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Allreduce(&local_cpu_usage, &global_cpu_usage, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_99_lock_time, &global_99_lock_time, 1, MPI_DOUBLE, MPI_MAX, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_95_lock_time, &global_95_lock_time, 1, MPI_DOUBLE, MPI_MAX, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_remote_shared_lock_time,
      &global_remote_shared_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_remote_exclusive_lock_time,
      &global_remote_exclusive_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_local_shared_lock_time,
      &global_local_shared_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_local_exclusive_lock_time,
      &global_local_exclusive_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_send_message_time,
      &global_send_message_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_receive_message_time,
      &global_receive_message_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_avg_rdma_read_count,
      &global_avg_rdma_read_count,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_avg_rdma_atomic_count,
      &global_avg_rdma_atomic_count,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&local_rdma_read_time,
      &global_rdma_read_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_rdma_atomic_time,
      &global_rdma_atomic_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&local_rdma_atomic_count,
      &global_rdma_atomic_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_rdma_read_count,
      &global_rdma_read_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_rdma_write_count,
      &global_rdma_write_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_rdma_send_count,
      &global_rdma_send_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_rdma_recv_count,
      &global_rdma_recv_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&local_lock_contention, &global_lock_contention, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_lock_success_with_retry, &global_lock_success_with_retry, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_lock_success_with_poll, &global_lock_success_with_poll, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_sum_poll_when_success, &global_sum_poll_when_success, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_sum_retry_when_success, &global_sum_retry_when_success, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_sum_index_when_timeout, &global_sum_index_when_timeout, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);

    cout << "Local Avg CPU Usage = " << local_cpu_usage_local << "% "
      "(" << "ID = " << rank <<  " ,# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
  MPI_Barrier(MPI_COMM_WORLD);

  // Get standard deviation for cpu usage
  if (rank < client_start_idx) {
    global_cpu_usage_avg = global_cpu_usage / (double)num_servers;
  }
  if (rank < client_start_idx) {
    local_cpu_diff = (global_cpu_usage_avg - local_cpu_usage) *
      (global_cpu_usage_avg - local_cpu_usage);
  }
  MPI_Reduce(&local_cpu_diff, &global_cpu_diff, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  global_cpu_usage_std = sqrt(global_cpu_diff / (double)num_servers);

  // Get standard deviation for time taken
  MPI_Allreduce(&local_time_taken_sum, &global_time_taken_sum, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);
  global_time_taken_avg = global_time_taken_sum / (double)num_users;
  if (rank >= client_start_idx) {
    for (unsigned int j=0;j<users.size();++j) {
      double time = users[j]->GetTimeTaken();
      local_time_taken_diff += (time - global_time_taken_avg) * (time - global_time_taken_avg);
    }
  }
  MPI_Reduce(&local_time_taken_diff, &global_time_taken_diff, 1, MPI_DOUBLE, MPI_SUM, 0,
      MPI_COMM_WORLD);
  global_time_taken_std = sqrt(global_time_taken_diff / (double)num_users);

  double global_99_lock_time_diff;
  double global_99_lock_time_avg = global_99_lock_time / (double)(num_users);
  double global_lock_time_avg = global_lock_time / (double)(num_users);
  double global_95_lock_time_avg = global_95_lock_time / (double)(num_users);
  double local_99_lock_time_diff = 0;
  if (rank >= client_start_idx) {
    local_99_lock_time_diff = (global_99_lock_time_avg - local_99_lock_time) *
      (global_99_lock_time_avg - local_99_lock_time);
  }
  MPI_Reduce(&local_99_lock_time_diff, &global_99_lock_time_diff, 1, MPI_DOUBLE, MPI_SUM,
      0, MPI_COMM_WORLD);
  double global_99_lock_time_std = sqrt(global_99_lock_time_diff / (double)num_users);

  MPI_Barrier(MPI_COMM_WORLD);
  if (rank==0) {
    cout << endl;
    cout << "Global Total Lock # = " << global_sum << "(# nodes: " <<
      num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Unlock # = " << global_unlock_sum << "(# nodes: " <<
      num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Success # = " << global_lock_success <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Failure # = " << global_lock_failure <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Timeout # = " << global_timeout <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Lock Time = " << global_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Shared Lock Time = " <<
      global_remote_shared_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Exclusive Lock Time = " <<
      global_remote_exclusive_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Shared Lock Time = " <<
      global_local_shared_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Exclusive Lock Time = " <<
      global_local_exclusive_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Send Message Time = " <<
      global_send_message_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Receive Message Time = " <<
      global_receive_message_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Read Count = " <<
      global_avg_rdma_read_count / num_servers <<
      "" << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Atomic Count = " <<
      global_avg_rdma_atomic_count / num_servers <<
      "" << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Read Time = " <<
      global_rdma_read_time / (double)global_rdma_read_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total RDMA Atomic Time = " <<
      global_rdma_atomic_time <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total RDMA Atomic Count = " <<
      global_rdma_atomic_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Atomic Time = " <<
      global_rdma_atomic_time / (double)global_rdma_atomic_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Overall Avg CPU Usage = " <<
      global_cpu_usage / num_servers << "% "
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Local Time Taken Sum = " << local_time_taken_sum << endl;

    cerr << lock_mode_str << "," << workload_type_str <<
      "," << shared_exclusive_rule_str << "," << exclusive_shared_rule_str << "," <<
      exclusive_exclusive_rule_str << "," << fail_retry << "," << poll_retry << "," <<
      sleep_time << "," << think_time << "," << num_nodes << "," <<
      num_warehouses_per_node << "," << num_users <<
      "," << "N/A" << "," << num_tx << "," << "N/A" << "," <<
      local_workload_ratio_str << "," << shared_lock_ratio_str << "," <<
      min_backoff_time << "," <<
      max_backoff_time << "," <<
      transaction_delay_str << ",";
    if (transaction_delay) {
      cerr << transaction_delay_min << "," << transaction_delay_max << ",";
    } else {
      cerr << "N/A,N/A,";
    }
    double avg_poll_when_success = (global_lock_success_with_poll == 0) ? 0.0 :
      (double)global_sum_poll_when_success / (double)global_lock_success_with_poll;
    double avg_retry_when_success = (global_lock_success_with_retry == 0) ? 0.0 :
      (double)global_sum_retry_when_success / (double)global_lock_success_with_retry;
    double avg_index_when_timeout = (global_timeout == 0) ? 0.0 :
      (double)global_sum_index_when_timeout / (double)global_timeout;

    cerr << global_lock_time_avg <<
      "," << global_99_lock_time << "," << global_95_lock_time << "," <<
      (long)((double)global_sum / (double)global_time_taken_avg) << "," <<
      global_sum << "," << global_lock_success << "," <<
      global_lock_success_with_retry << "," <<
      avg_retry_when_success << "," <<
      global_lock_success_with_poll << "," <<
      avg_poll_when_success << "," <<
      global_lock_failure << "," <<
      global_lock_contention << "," <<
      global_timeout << "," <<
      avg_index_when_timeout << "," <<
      global_cpu_usage_avg << "," << global_cpu_usage_std <<
      "," << global_time_taken_avg / (double)(1000*1000*1000) << "," <<
      global_time_taken_std / (double)(1000*1000*1000) << "," <<
      max_tx_throughput << "," << max_lock_throughput << "," <<
      global_rdma_send_count << "," << global_rdma_recv_count << "," <<
      global_rdma_write_count << "," << global_rdma_read_count << "," <<
      global_rdma_atomic_count <<
      endl;
  }

  MPI_Finalize();
}

void* RunLockManager(void* args) {
  LockManager* lock_manager = (LockManager*)args;
  lock_manager->Run();
  return NULL;
}

void* RunLockSimulator(void* args) {
  LockSimulator* user = (LockSimulator*)args;
  user->Run();
  return NULL;
}

void* MeasureCPUUsage(void* args) {
  CPUUsage* usage = (CPUUsage*)args;

  usage->total_cpu = 0;
  usage->num_sample = 0;
  usage->terminate = false;

  int numProcessors;
  clock_t lastCPU, lastSysCPU, lastUserCPU, now;
  double percent;

  FILE* file;
  struct tms timeSample;
  char line[128];

  lastCPU     = times(&timeSample);
  lastSysCPU  = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;

  file          = fopen("/proc/cpuinfo", "r");
  numProcessors = 0;
  percent       = 0;
  while(fgets(line, 128, file) != NULL){
    if (strncmp(line, "processor", 9) == 0) numProcessors++;
  }
  fclose(file);

  while (!usage->terminate) {
    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
        timeSample.tms_utime < lastUserCPU){
      //Overflow detection. Just skip this value.
      //            percent = -1.0;
      //
    }
    else{
      percent = (timeSample.tms_stime - lastSysCPU) +
        (timeSample.tms_utime - lastUserCPU);
      percent /= (now - lastCPU);
      //percent /= numProcessors;
      percent *= 100;

    }
    lastCPU = now;
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;

    usage->total_cpu += percent;
    usage->num_sample += 1;
    //cout << percent << endl;
    sleep(1);
  }
  return NULL;
}
