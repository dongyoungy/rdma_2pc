#include <iostream>
#include <vector>
#include <cmath>
#include <arpa/inet.h>
#include <pthread.h>
#include <infiniband/verbs.h>
#include <sys/times.h>
#include <limits.h>
#include "mpi.h"
#include "fs_lock_simulator.h"
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

  if (argc != 21) {
    cout << argv[0] << " <work_dir> <num_lock_object> <workload_type> <num_tx>" <<
      " <num_request_per_tx> <num_users> <lock_mode>" <<
      " <shared_exclusive_rule> <exclusive_shared_rule> <exclusive_exclusive_rule>" <<
      " <fail_retry> <poll_retry> <sleep_time_for_timeout> <think_time>"
      " <transaction_delay> <transaction_delay_min> " <<
      "<transaction_delay_max> <miin_backoff_time> <max_backoff_time> <rand_seed>" << endl;
    exit(1);
  }

  int num_nodes, rank;
  int num_servers, num_clients;
  int server_start_idx, group1_start_idx, group2_start_idx;
  int group1_users_per_client, group2_users_per_client;
  int group1_size, group2_size;

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
  int num_lock_object          = atoi(argv[k++]);
  int workload_type            = atoi(argv[k++]);
  long num_tx                  = atol(argv[k++]);
  int num_request_per_tx       = atoi(argv[k++]);
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
  //if (num_managers > 32) {
     //cerr << "# of nodes must be less than 33" << endl;
     //exit(-1);
  //}
  if (num_users > 32) {
     cerr << "# of node/clients must be less than 33" << endl;
     exit(-1);
  }

  // 1 node = server/client on the same node
  // 2 node = 1 server, 1 client
  // n nodes = n-2 servers, 2 clients
  if (num_nodes < 5) {
     cerr << "# of nodes must be greater than 4" << endl;
     exit(-1);
  } else {
    num_servers             = num_nodes - 4;
    num_clients             = 4;
    server_start_idx        = 0;
    group1_start_idx        = num_nodes - 4;
    group2_start_idx        = num_nodes - 2;
    group1_users_per_client = (num_users - 2) / 2;
    group2_users_per_client = 1;
    //group1_users_per_client = num_users/2;
    //group2_users_per_client = num_users/2;
    group1_size = num_users - 2;
    group2_size = 2;
  }


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

  double local_workload_ratio = 0.5;
  double shared_lock_ratio = 0.5;
  string workload_type_str, shared_lock_ratio_str;
  if (workload_type == LockSimulator::WORKLOAD_UNIFORM) {
    workload_type_str = "UNIFORM";
  } else if (workload_type == LockSimulator::WORKLOAD_HOTSPOT) {
    workload_type_str = "HOTSPOT";
  } else if (workload_type == LockSimulator::WORKLOAD_ALL_LOCAL) {
    workload_type_str = "ALL_LOCAL";
  } else if (workload_type == LockSimulator::WORKLOAD_MIXED) {
    char buf[32];
    sprintf(buf, "MIXED (local: %.0f %%)", local_workload_ratio * 100);
    workload_type_str = buf;
  }

  string local_workload_ratio_str;
  char buf[32];
  sprintf(buf, "%.4f%%", local_workload_ratio * 100);
  local_workload_ratio_str = buf;
  if (workload_type != LockSimulator::WORKLOAD_MIXED) {
    local_workload_ratio_str = "N/A";
  }

  sprintf(buf, "%.4f%%", shared_lock_ratio * 100);
  shared_lock_ratio_str = buf;

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
      lock_mode == LOCK_PROXY_QUEUE) {
    shared_exclusive_rule_str = "N/A";
    exclusive_shared_rule_str = "N/A";
    exclusive_exclusive_rule_str = "N/A";
  }

  if (rank == 0) {
    cout << "Type of Workload = " << workload_type_str << " (Shared Lock: " <<
      shared_lock_ratio * 100.0 << "%)" << endl;
    cout << "Shared Lock Ratio = " << shared_lock_ratio_str << endl;
    cout << "SHARED -> EXCLUSIVE = " << shared_exclusive_rule_str << endl;
    cout << "EXCLUSIVE -> SHARED = " << exclusive_shared_rule_str << endl;
    cout << "EXCLUSIVE -> EXCLUSIVE = " << exclusive_exclusive_rule_str << endl;
    cout << "Num Tx = " << num_tx << endl;
    cout << "Num Requests per Tx = " << num_request_per_tx << endl;
  }

  LockManager::SetSharedExclusiveRule(shared_exclusive_rule);
  LockManager::SetExclusiveSharedRule(exclusive_shared_rule);
  LockManager::SetExclusiveExclusiveRule(exclusive_exclusive_rule);
  LockManager::SetFailRetry(fail_retry);
  LockManager::SetPollRetry(poll_retry);

  LockManager* lock_manager = new LockManager(argv[1], rank, num_nodes,
      num_lock_object, lock_mode, num_users, num_clients);

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

  vector<LockSimulator*> group1_users;
  vector<LockSimulator*> group2_users;
  vector<LockSimulator*> users;
  bool verbose = false;
  if (rank >= group1_start_idx && rank < group2_start_idx) {
    for (int i=0;i<group1_users_per_client;++i) {
      uint32_t seq = (rank-group1_start_idx)*group1_users_per_client+i;
      uint32_t id = (uint32_t)pow(2.0, seq);
      FSLockSimulator* simulator = new FSLockSimulator(lock_manager,
          id, // id
          seq % num_servers, // home id
          workload_type,
          num_servers,
          num_lock_object,
          num_tx,
          num_request_per_tx,
          seed,
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
      lock_manager->RegisterUser(id, simulator);
      users.push_back(simulator);
      group1_users.push_back(simulator);
    }
  } else if (rank >= group2_start_idx) {
    for (int i=0;i<group2_users_per_client;++i) {
      uint32_t seq = group1_size+(rank-group2_start_idx)*group2_users_per_client+i;
      uint32_t id = (uint32_t)pow(2.0, seq);
      FSLockSimulator* simulator = new FSLockSimulator(lock_manager,
          id, // id
          seq % num_servers, // home id
          workload_type,
          num_servers,
          num_lock_object,
          num_tx,
          num_request_per_tx,
          seed,
          verbose, // verbose
          true, // measure lock time
          lock_mode,
          transaction_delay,
          transaction_delay_min,
          transaction_delay_max,
          min_backoff_time,
          max_backoff_time,
          sleep_time,
          think_time + 30
          );
      lock_manager->RegisterUser(id, simulator);
      users.push_back(simulator);
      group2_users.push_back(simulator);
    }
  }

  for (int j=group1_start_idx; j < group2_start_idx; ++j) {
    for (int i=0;i<group1_users_per_client;++i) {
      uint32_t seq = (j-group1_start_idx)*group1_users_per_client+i;
      LockManager::user_to_node_map_[seq] = j;
    }
  }
  for (int j=group2_start_idx; j < num_nodes; ++j) {
    for (int i=0;i<group2_users_per_client;++i) {
      uint32_t seq = group1_size+(j-group2_start_idx)*group2_users_per_client+i;
      LockManager::user_to_node_map_[seq] = j;
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  sleep(1);

  if (lock_manager->InitializeLockClients()) {
     cerr << "InitializeLockClients() failed." << endl;
     exit(-1);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  sleep(1);

  time_t start_time;
  time_t end_time;

  time(&start_time);

  if (rank >= group1_start_idx) {
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
  while (rank >= group1_start_idx) {
    for (unsigned int i=0;i<users.size();++i) {
      tx_done[time_taken2] += users[i]->GetCount();
      locks_done[time_taken2] += users[i]->GetTotalNumLockSuccess();
      if (users[i]->GetState() == LockSimulator::STATE_DONE) {
        simulator_done = true;
      }
    }
    if (users.size() == 0) {
      simulator_done = true;
    }
    ++time_taken2;
    cout << rank << "," << users[0]->GetID() << "," << time_taken2 << " : " << users[0]->GetCount() <<
      "," << users[0]->GetCurrentBackoff() << endl;
    if (simulator_done) break;
    sleep(1);
  }
  time_taken3 = time_taken2;

  for (unsigned int i=0;i<users.size();++i) {
    LockSimulator* simulator = users[i];
    while (simulator->GetState() != LockSimulator::STATE_DONE) {
      ++time_taken2;
      //if (rank == 0) {
        cout << rank << "," << users[i]->GetID() << "," << time_taken2 << " : " << users[i]->GetCount() <<
          "," << users[i]->GetCurrentBackoff() << endl;
      //}
      sleep(1);
    }
  }

  time(&end_time);
  //double time_taken = difftime(end_time, start_time);

  // group1
  long group1_local_sum                           = 0;
  long group1_global_sum                          = 0;
  long group1_local_unlock_sum                    = 0;
  long group1_global_unlock_sum                   = 0;
  long group1_local_lock_success                  = 0;
  long group1_global_lock_success                 = 0;
  long group1_local_lock_contention               = 0;
  long group1_global_lock_contention              = 0;
  long group1_local_lock_success_with_retry       = 0;
  long group1_global_lock_success_with_retry      = 0;
  long group1_local_lock_success_with_poll        = 0;
  long group1_global_lock_success_with_poll       = 0;
  long group1_local_sum_poll_when_success         = 0;
  long group1_global_sum_poll_when_success        = 0;
  long group1_local_sum_retry_when_success        = 0;
  long group1_global_sum_retry_when_success       = 0;
  long group1_local_sum_index_when_timeout        = 0;
  long group1_global_sum_index_when_timeout       = 0;
  long group1_local_lock_failure                  = 0;
  long group1_global_lock_failure                 = 0;
  long group1_local_timeout                       = 0;
  long group1_global_timeout                      = 0;
  double group1_local_lock_time                   = 0;
  double group1_global_lock_time                  = 0;
  double group1_local_cpu_usage                   = 0;
  double group1_global_cpu_usage                  = 0;
  double group1_global_cpu_usage_avg              = 0;
  double group1_local_remote_exclusive_lock_time  = 0;
  double group1_global_remote_exclusive_lock_time = 0;
  double group1_local_remote_shared_lock_time     = 0;
  double group1_global_remote_shared_lock_time    = 0;
  double group1_local_group1_local_exclusive_lock_time   = 0;
  double group1_global_group1_local_exclusive_lock_time  = 0;
  double group1_local_group1_local_shared_lock_time      = 0;
  double group1_global_group1_local_shared_lock_time     = 0;
  double group1_local_send_message_time           = 0;
  double group1_global_send_message_time          = 0;
  double group1_local_receive_message_time        = 0;
  double group1_global_receive_message_time       = 0;
  double group1_local_99_lock_time                = 0;
  double group1_global_99_lock_time               = 0;
  double group1_local_95_lock_time                = 0;
  double group1_global_95_lock_time               = 0;

  double group1_local_avg_rdma_read_count = 0;
  double group1_global_avg_rdma_read_count = 0;
  double group1_local_avg_rdma_atomic_count = 0;
  double group1_global_avg_rdma_atomic_count = 0;

  double group1_local_rdma_read_time = 0;
  double group1_global_rdma_read_time = 0;
  double group1_local_rdma_atomic_time = 0;
  double group1_global_rdma_atomic_time = 0;

  long group1_local_rdma_read_count = 0;
  long group1_local_rdma_send_count = 0;
  long group1_local_rdma_recv_count = 0;
  long group1_local_rdma_atomic_count = 0;
  long group1_local_rdma_write_count = 0;

  long group1_global_rdma_read_count = 0;
  long group1_global_rdma_send_count = 0;
  long group1_global_rdma_recv_count = 0;
  long group1_global_rdma_atomic_count = 0;
  long group1_global_rdma_write_count = 0;

  double group1_local_cpu_diff = 0;
  double group1_local_time_taken_diff = 0;
  double group1_local_time_taken = 0;
  double group1_local_time_taken_sum = 0;
  double group1_global_cpu_diff = 0;
  double group1_global_time_taken_sum = 0;
  double group1_global_time_taken_avg = 0;
  double group1_global_cpu_usage_std = 0;
  double group1_global_time_taken_std = 0;
  double group1_global_time_taken_diff = 0;

  MPI_Barrier(MPI_COMM_WORLD);

  if (time_taken3 == 0) {
    time_taken3 = INT_MAX;
  }

  // calculate max throughput
  MPI_Allreduce(&time_taken3, &min_time_taken, 1, MPI_INT, MPI_MIN,
      MPI_COMM_WORLD);

  uint64_t* group1_local_tx_throughput = new uint64_t[min_time_taken];
  uint64_t* group1_global_tx_throughput = new uint64_t[min_time_taken];
  uint64_t* group1_local_lock_throughput = new uint64_t[min_time_taken];
  uint64_t* group1_global_lock_throughput = new uint64_t[min_time_taken];

  for (int i = 0; i < min_time_taken; ++i) {
    if (rank >= group1_start_idx && rank < group2_start_idx) {
      if (i == 0) {
        group1_local_tx_throughput[i] = tx_done[i];
        group1_local_lock_throughput[i] = locks_done[i];
      } else {
        group1_local_tx_throughput[i] = tx_done[i] - tx_done[i-1];
        group1_local_lock_throughput[i] = locks_done[i] - locks_done[i-1];
      }
    } else {
      group1_local_tx_throughput[i] = 0;
      group1_local_lock_throughput[i] = 0;
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  for (int i = 0; i < min_time_taken; ++i) {
    uint64_t local = group1_local_tx_throughput[i];
    uint64_t global;
    MPI_Reduce(&local, &global, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
        MPI_COMM_WORLD);
    group1_global_tx_throughput[i] = global;
    MPI_Barrier(MPI_COMM_WORLD);
    local = group1_local_lock_throughput[i];
    MPI_Reduce(&local, &global, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
        MPI_COMM_WORLD);
    group1_global_lock_throughput[i] = global;
    MPI_Barrier(MPI_COMM_WORLD);
  }

  uint64_t group1_max_tx_throughput = 0;
  uint64_t group1_max_lock_throughput = 0;

  if (rank == 0) {
    for (int i = 0; i < min_time_taken; ++i) {
      if (group1_max_tx_throughput < group1_global_tx_throughput[i])
        group1_max_tx_throughput = group1_global_tx_throughput[i];

      if (group1_max_lock_throughput < group1_global_lock_throughput[i])
        group1_max_lock_throughput = group1_global_lock_throughput[i];
    }
  }

  if (rank >= group1_start_idx && rank < group2_start_idx) {
    for (unsigned int j=0;j<users.size();++j) {
      LockSimulator* simulator = users[j];
      group1_local_sum += simulator->GetTotalNumLocks();
      group1_local_unlock_sum += simulator->GetTotalNumUnlocks();
      group1_local_lock_success += simulator->GetTotalNumLockSuccess();
      group1_local_lock_success_with_retry += simulator->GetTotalNumLockSuccessWithRetry();
      group1_local_sum_retry_when_success += simulator->GetSumRetryWhenSuccess();
      group1_local_sum_index_when_timeout += simulator->GetSumIndexWhenTimeout();
      group1_local_lock_failure += simulator->GetTotalNumLockFailure();
      group1_local_timeout += simulator->GetTotalNumTimeout();
      if (simulator->IsLockTimeMeasured()) {
        group1_local_lock_time += simulator->GetAverageTimeTakenToLock();
        if (group1_local_99_lock_time < simulator->Get99PercentileLockTime())
          group1_local_99_lock_time = simulator->Get99PercentileLockTime();
        if (group1_local_95_lock_time < simulator->Get95PercentileLockTime())
          group1_local_95_lock_time = simulator->Get95PercentileLockTime();
      }
    }
    for (unsigned int j=0;j<users.size();++j) {
      LockSimulator* simulator = users[j];
      group1_local_time_taken_sum += simulator->GetTimeTaken();
    }
  }
  group1_local_lock_contention = lock_manager->GetTotalLockContention();
  group1_local_lock_success_with_poll = lock_manager->GetTotalLockSuccessWithPoll();
  group1_local_sum_poll_when_success = lock_manager->GetTotalSumPollWhenSuccess();

  group1_local_remote_shared_lock_time =
    lock_manager->GetAverageRemoteSharedLockTime();
  group1_local_remote_exclusive_lock_time =
    lock_manager->GetAverageRemoteExclusiveLockTime();
  group1_local_group1_local_shared_lock_time =
    lock_manager->GetAverageLocalSharedLockTime();
  group1_local_group1_local_exclusive_lock_time =
    lock_manager->GetAverageLocalExclusiveLockTime();
  group1_local_send_message_time = lock_manager->GetAverageSendMessageTime();
  group1_local_receive_message_time = lock_manager->GetAverageReceiveMessageTime();
  group1_local_avg_rdma_read_count = lock_manager->GetAverageRDMAReadCount();
  group1_local_avg_rdma_atomic_count = lock_manager->GetAverageRDMAAtomicCount();

  group1_local_rdma_read_count = lock_manager->GetTotalRDMAReadCount();
  group1_local_rdma_recv_count = lock_manager->GetTotalRDMARecvCount();
  group1_local_rdma_send_count = lock_manager->GetTotalRDMASendCount();
  group1_local_rdma_write_count = lock_manager->GetTotalRDMAWriteCount();
  group1_local_rdma_atomic_count = lock_manager->GetTotalRDMAAtomicCount();

  group1_local_rdma_read_time = lock_manager->GetTotalRDMAReadTime();
  group1_local_rdma_atomic_time = lock_manager->GetTotalRDMAAtomicTime();

  MPI_Barrier(MPI_COMM_WORLD);
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Reduce(&group1_local_sum, &group1_global_sum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_unlock_sum, &group1_global_unlock_sum, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_lock_success, &group1_global_lock_success, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_lock_failure, &group1_global_lock_failure, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_lock_time, &group1_global_lock_time, 1, MPI_DOUBLE, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_timeout, &group1_global_timeout, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Allreduce(&group1_local_cpu_usage, &group1_global_cpu_usage, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_99_lock_time, &group1_global_99_lock_time, 1, MPI_DOUBLE, MPI_MAX, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_95_lock_time, &group1_global_95_lock_time, 1, MPI_DOUBLE, MPI_MAX, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_remote_shared_lock_time,
      &group1_global_remote_shared_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_remote_exclusive_lock_time,
      &group1_global_remote_exclusive_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_group1_local_shared_lock_time,
      &group1_global_group1_local_shared_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_group1_local_exclusive_lock_time,
      &group1_global_group1_local_exclusive_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_send_message_time,
      &group1_global_send_message_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_receive_message_time,
      &group1_global_receive_message_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_avg_rdma_read_count,
      &group1_global_avg_rdma_read_count,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_avg_rdma_atomic_count,
      &group1_global_avg_rdma_atomic_count,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&group1_local_rdma_read_time,
      &group1_global_rdma_read_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_rdma_atomic_time,
      &group1_global_rdma_atomic_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&group1_local_rdma_atomic_count,
      &group1_global_rdma_atomic_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_rdma_read_count,
      &group1_global_rdma_read_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_rdma_write_count,
      &group1_global_rdma_write_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_rdma_send_count,
      &group1_global_rdma_send_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_rdma_recv_count,
      &group1_global_rdma_recv_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&group1_local_lock_contention, &group1_global_lock_contention, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_lock_success_with_retry, &group1_global_lock_success_with_retry, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_lock_success_with_poll, &group1_global_lock_success_with_poll, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_sum_poll_when_success, &group1_global_sum_poll_when_success, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_sum_retry_when_success, &group1_global_sum_retry_when_success, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group1_local_sum_index_when_timeout, &group1_global_sum_index_when_timeout, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD);

  // Get standard deviation for time taken
  MPI_Allreduce(&group1_local_time_taken_sum, &group1_global_time_taken_sum, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);
  group1_global_time_taken_avg = group1_global_time_taken_sum / (double)group1_size;
  if (rank >= group1_start_idx && rank < group2_start_idx) {
    for (unsigned int j=0;j<users.size();++j) {
      double time = users[j]->GetTimeTaken();
      group1_local_time_taken_diff += (time - group1_global_time_taken_avg) * (time - group1_global_time_taken_avg);
    }
  }
  MPI_Reduce(&group1_local_time_taken_diff, &group1_global_time_taken_diff, 1, MPI_DOUBLE, MPI_SUM, 0,
      MPI_COMM_WORLD);
  group1_global_time_taken_std = sqrt(group1_global_time_taken_diff / (double)group1_size);

  double group1_global_99_lock_time_diff;
  double group1_global_99_lock_time_avg = group1_global_99_lock_time / (double)(group1_size);
  double group1_global_lock_time_avg = group1_global_lock_time / (double)(group1_size);
  double group1_global_95_lock_time_avg = group1_global_95_lock_time / (double)(group1_size);
  double group1_local_99_lock_time_diff = 0;
  if (rank >= group1_start_idx && rank < group2_start_idx) {
    group1_local_99_lock_time_diff = (group1_global_99_lock_time_avg - group1_local_99_lock_time) *
      (group1_global_99_lock_time_avg - group1_local_99_lock_time);
  }
  MPI_Reduce(&group1_local_99_lock_time_diff, &group1_global_99_lock_time_diff, 1, MPI_DOUBLE, MPI_SUM,
      0, MPI_COMM_WORLD);
  double group1_global_99_lock_time_std = sqrt(group1_global_99_lock_time_diff / (double)group1_size);

  if (rank==0) {
    cout << endl;
    cout << "Global Total Lock # = " << group1_global_sum << "(# nodes: " <<
      num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Unlock # = " << group1_global_unlock_sum << "(# nodes: " <<
      num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Success # = " << group1_global_lock_success <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Failure # = " << group1_global_lock_failure <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Timeout # = " << group1_global_timeout <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Lock Time = " << group1_global_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Shared Lock Time = " <<
      group1_global_remote_shared_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Exclusive Lock Time = " <<
      group1_global_remote_exclusive_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Shared Lock Time = " <<
      group1_global_group1_local_shared_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Exclusive Lock Time = " <<
      group1_global_group1_local_exclusive_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Send Message Time = " <<
      group1_global_send_message_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Receive Message Time = " <<
      group1_global_receive_message_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Read Count = " <<
      group1_global_avg_rdma_read_count / num_servers <<
      "" << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Atomic Count = " <<
      group1_global_avg_rdma_atomic_count / num_servers <<
      "" << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Read Time = " <<
      group1_global_rdma_read_time / (double)group1_global_rdma_read_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total RDMA Atomic Time = " <<
      group1_global_rdma_atomic_time <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total RDMA Atomic Count = " <<
      group1_global_rdma_atomic_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Atomic Time = " <<
      group1_global_rdma_atomic_time / (double)group1_global_rdma_atomic_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Overall Avg CPU Usage = " <<
      group1_global_cpu_usage / num_servers << "% "
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Local Time Taken Sum = " << group1_local_time_taken_sum << endl;

    cerr << lock_mode_str << "_GROUP1," << workload_type_str <<
      "," << shared_exclusive_rule_str << "," << exclusive_shared_rule_str << "," <<
      exclusive_exclusive_rule_str << "," << fail_retry << "," << poll_retry << "," <<
      sleep_time << "," << think_time << "," << num_servers << "," << group1_size <<
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
    double avg_poll_when_success = (group1_global_lock_success_with_poll == 0) ? 0.0 :
      (double)group1_global_sum_poll_when_success / (double)group1_global_lock_success_with_poll;
    double avg_retry_when_success = (group1_global_lock_success_with_retry == 0) ? 0.0 :
      (double)group1_global_sum_retry_when_success / (double)group1_global_lock_success_with_retry;
    double avg_index_when_timeout = (group1_global_timeout == 0) ? 0.0 :
      (double)group1_global_sum_index_when_timeout / (double)group1_global_timeout;

    cerr << group1_global_lock_time_avg <<
      "," << group1_global_99_lock_time << "," << group1_global_95_lock_time << "," <<
      (long)((double)group1_global_sum / (double)group1_global_time_taken_avg) << "," <<
      group1_global_sum << "," << group1_global_lock_success << "," <<
      group1_global_lock_success_with_retry << "," <<
      avg_retry_when_success << "," <<
      group1_global_lock_success_with_poll << "," <<
      avg_poll_when_success << "," <<
      group1_global_lock_failure << "," <<
      group1_global_lock_contention << "," <<
      group1_global_timeout << "," <<
      avg_index_when_timeout << "," <<
      group1_global_cpu_usage_avg << "," << group1_global_cpu_usage_std <<
      "," << group1_global_time_taken_avg / (double)(1000*1000*1000) << "," <<
      group1_global_time_taken_std / (double)(1000*1000*1000) << "," <<
      group1_max_tx_throughput << "," << group1_max_lock_throughput << "," <<
      group1_global_rdma_send_count << "," << group1_global_rdma_recv_count << "," <<
      group1_global_rdma_write_count << "," << group1_global_rdma_read_count << "," <<
      group1_global_rdma_atomic_count <<
      endl;
  }


  // group2
  long group2_local_sum                           = 0;
  long group2_global_sum                          = 0;
  long group2_local_unlock_sum                    = 0;
  long group2_global_unlock_sum                   = 0;
  long group2_local_lock_success                  = 0;
  long group2_global_lock_success                 = 0;
  long group2_local_lock_contention               = 0;
  long group2_global_lock_contention              = 0;
  long group2_local_lock_success_with_retry       = 0;
  long group2_global_lock_success_with_retry      = 0;
  long group2_local_lock_success_with_poll        = 0;
  long group2_global_lock_success_with_poll       = 0;
  long group2_local_sum_poll_when_success         = 0;
  long group2_global_sum_poll_when_success        = 0;
  long group2_local_sum_retry_when_success        = 0;
  long group2_global_sum_retry_when_success       = 0;
  long group2_local_sum_index_when_timeout        = 0;
  long group2_global_sum_index_when_timeout       = 0;
  long group2_local_lock_failure                  = 0;
  long group2_global_lock_failure                 = 0;
  long group2_local_timeout                       = 0;
  long group2_global_timeout                      = 0;
  double group2_local_lock_time                   = 0;
  double group2_global_lock_time                  = 0;
  double group2_local_cpu_usage                   = 0;
  double group2_global_cpu_usage                  = 0;
  double group2_global_cpu_usage_avg              = 0;
  double group2_local_remote_exclusive_lock_time  = 0;
  double group2_global_remote_exclusive_lock_time = 0;
  double group2_local_remote_shared_lock_time     = 0;
  double group2_global_remote_shared_lock_time    = 0;
  double group2_local_group2_local_exclusive_lock_time   = 0;
  double group2_global_group2_local_exclusive_lock_time  = 0;
  double group2_local_group2_local_shared_lock_time      = 0;
  double group2_global_group2_local_shared_lock_time     = 0;
  double group2_local_send_message_time           = 0;
  double group2_global_send_message_time          = 0;
  double group2_local_receive_message_time        = 0;
  double group2_global_receive_message_time       = 0;
  double group2_local_99_lock_time                = 0;
  double group2_global_99_lock_time               = 0;
  double group2_local_95_lock_time                = 0;
  double group2_global_95_lock_time               = 0;

  double group2_local_avg_rdma_read_count = 0;
  double group2_global_avg_rdma_read_count = 0;
  double group2_local_avg_rdma_atomic_count = 0;
  double group2_global_avg_rdma_atomic_count = 0;

  double group2_local_rdma_read_time = 0;
  double group2_global_rdma_read_time = 0;
  double group2_local_rdma_atomic_time = 0;
  double group2_global_rdma_atomic_time = 0;

  long group2_local_rdma_read_count = 0;
  long group2_local_rdma_send_count = 0;
  long group2_local_rdma_recv_count = 0;
  long group2_local_rdma_atomic_count = 0;
  long group2_local_rdma_write_count = 0;

  long group2_global_rdma_read_count = 0;
  long group2_global_rdma_send_count = 0;
  long group2_global_rdma_recv_count = 0;
  long group2_global_rdma_atomic_count = 0;
  long group2_global_rdma_write_count = 0;

  double group2_local_cpu_diff = 0;
  double group2_local_time_taken_diff = 0;
  double group2_local_time_taken = 0;
  double group2_local_time_taken_sum = 0;
  double group2_global_cpu_diff = 0;
  double group2_global_time_taken_sum = 0;
  double group2_global_time_taken_avg = 0;
  double group2_global_cpu_usage_std = 0;
  double group2_global_time_taken_std = 0;
  double group2_global_time_taken_diff = 0;

  MPI_Barrier(MPI_COMM_WORLD);

  if (time_taken3 == 0) {
    time_taken3 = INT_MAX;
  }

  // calculate max throughput
  MPI_Allreduce(&time_taken3, &min_time_taken, 1, MPI_INT, MPI_MIN,
      MPI_COMM_WORLD);

  uint64_t* group2_local_tx_throughput = new uint64_t[min_time_taken];
  uint64_t* group2_global_tx_throughput = new uint64_t[min_time_taken];
  uint64_t* group2_local_lock_throughput = new uint64_t[min_time_taken];
  uint64_t* group2_global_lock_throughput = new uint64_t[min_time_taken];

  for (int i = 0; i < min_time_taken; ++i) {
    if (rank >= group2_start_idx) {
      if (i == 0) {
        group2_local_tx_throughput[i] = tx_done[i];
        group2_local_lock_throughput[i] = locks_done[i];
      } else {
        group2_local_tx_throughput[i] = tx_done[i] - tx_done[i-1];
        group2_local_lock_throughput[i] = locks_done[i] - locks_done[i-1];
      }
    } else {
      group2_local_tx_throughput[i] = 0;
      group2_local_lock_throughput[i] = 0;
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  for (int i = 0; i < min_time_taken; ++i) {
    uint64_t local = group2_local_tx_throughput[i];
    uint64_t global;
    MPI_Reduce(&local, &global, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
        MPI_COMM_WORLD);
    group2_global_tx_throughput[i] = global;
    MPI_Barrier(MPI_COMM_WORLD);
    local = group2_local_lock_throughput[i];
    MPI_Reduce(&local, &global, 1, MPI_LONG_LONG_INT, MPI_SUM, 0,
        MPI_COMM_WORLD);
    group2_global_lock_throughput[i] = global;
    MPI_Barrier(MPI_COMM_WORLD);
  }

  uint64_t group2_max_tx_throughput = 0;
  uint64_t group2_max_lock_throughput = 0;

  if (rank == 0) {
    for (int i = 0; i < min_time_taken; ++i) {
      if (group2_max_tx_throughput < group2_global_tx_throughput[i])
        group2_max_tx_throughput = group2_global_tx_throughput[i];

      if (group2_max_lock_throughput < group2_global_lock_throughput[i])
        group2_max_lock_throughput = group2_global_lock_throughput[i];
    }
  }

  if (rank >= group2_start_idx) {
    for (unsigned int j=0;j<users.size();++j) {
      LockSimulator* simulator = users[j];
      group2_local_sum += simulator->GetTotalNumLocks();
      group2_local_unlock_sum += simulator->GetTotalNumUnlocks();
      group2_local_lock_success += simulator->GetTotalNumLockSuccess();
      group2_local_lock_success_with_retry += simulator->GetTotalNumLockSuccessWithRetry();
      group2_local_sum_retry_when_success += simulator->GetSumRetryWhenSuccess();
      group2_local_sum_index_when_timeout += simulator->GetSumIndexWhenTimeout();
      group2_local_lock_failure += simulator->GetTotalNumLockFailure();
      group2_local_timeout += simulator->GetTotalNumTimeout();
      if (simulator->IsLockTimeMeasured()) {
        group2_local_lock_time += simulator->GetAverageTimeTakenToLock();
        if (group2_local_99_lock_time < simulator->Get99PercentileLockTime())
          group2_local_99_lock_time = simulator->Get99PercentileLockTime();
        if (group2_local_95_lock_time < simulator->Get95PercentileLockTime())
          group2_local_95_lock_time = simulator->Get95PercentileLockTime();
      }
    }
    for (unsigned int j=0;j<users.size();++j) {
      LockSimulator* simulator = users[j];
      group2_local_time_taken_sum += simulator->GetTimeTaken();
    }
  }
  group2_local_lock_contention = lock_manager->GetTotalLockContention();
  group2_local_lock_success_with_poll = lock_manager->GetTotalLockSuccessWithPoll();
  group2_local_sum_poll_when_success = lock_manager->GetTotalSumPollWhenSuccess();

  group2_local_remote_shared_lock_time =
    lock_manager->GetAverageRemoteSharedLockTime();
  group2_local_remote_exclusive_lock_time =
    lock_manager->GetAverageRemoteExclusiveLockTime();
  group2_local_group2_local_shared_lock_time =
    lock_manager->GetAverageLocalSharedLockTime();
  group2_local_group2_local_exclusive_lock_time =
    lock_manager->GetAverageLocalExclusiveLockTime();
  group2_local_send_message_time = lock_manager->GetAverageSendMessageTime();
  group2_local_receive_message_time = lock_manager->GetAverageReceiveMessageTime();
  group2_local_avg_rdma_read_count = lock_manager->GetAverageRDMAReadCount();
  group2_local_avg_rdma_atomic_count = lock_manager->GetAverageRDMAAtomicCount();

  group2_local_rdma_read_count = lock_manager->GetTotalRDMAReadCount();
  group2_local_rdma_recv_count = lock_manager->GetTotalRDMARecvCount();
  group2_local_rdma_send_count = lock_manager->GetTotalRDMASendCount();
  group2_local_rdma_write_count = lock_manager->GetTotalRDMAWriteCount();
  group2_local_rdma_atomic_count = lock_manager->GetTotalRDMAAtomicCount();

  group2_local_rdma_read_time = lock_manager->GetTotalRDMAReadTime();
  group2_local_rdma_atomic_time = lock_manager->GetTotalRDMAAtomicTime();

  MPI_Barrier(MPI_COMM_WORLD);
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Reduce(&group2_local_sum, &group2_global_sum, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_unlock_sum, &group2_global_unlock_sum, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_lock_success, &group2_global_lock_success, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_lock_failure, &group2_global_lock_failure, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_lock_time, &group2_global_lock_time, 1, MPI_DOUBLE, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_timeout, &group2_global_timeout, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Allreduce(&group2_local_cpu_usage, &group2_global_cpu_usage, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_99_lock_time, &group2_global_99_lock_time, 1, MPI_DOUBLE, MPI_MAX, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_95_lock_time, &group2_global_95_lock_time, 1, MPI_DOUBLE, MPI_MAX, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_remote_shared_lock_time,
      &group2_global_remote_shared_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_remote_exclusive_lock_time,
      &group2_global_remote_exclusive_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_group2_local_shared_lock_time,
      &group2_global_group2_local_shared_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_group2_local_exclusive_lock_time,
      &group2_global_group2_local_exclusive_lock_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_send_message_time,
      &group2_global_send_message_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_receive_message_time,
      &group2_global_receive_message_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_avg_rdma_read_count,
      &group2_global_avg_rdma_read_count,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_avg_rdma_atomic_count,
      &group2_global_avg_rdma_atomic_count,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&group2_local_rdma_read_time,
      &group2_global_rdma_read_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_rdma_atomic_time,
      &group2_global_rdma_atomic_time,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&group2_local_rdma_atomic_count,
      &group2_global_rdma_atomic_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_rdma_read_count,
      &group2_global_rdma_read_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_rdma_write_count,
      &group2_global_rdma_write_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_rdma_send_count,
      &group2_global_rdma_send_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_rdma_recv_count,
      &group2_global_rdma_recv_count,
      1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(&group2_local_lock_contention, &group2_global_lock_contention, 1, MPI_LONG, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_lock_success_with_retry, &group2_global_lock_success_with_retry, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_lock_success_with_poll, &group2_global_lock_success_with_poll, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_sum_poll_when_success, &group2_global_sum_poll_when_success, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_sum_retry_when_success, &group2_global_sum_retry_when_success, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&group2_local_sum_index_when_timeout, &group2_global_sum_index_when_timeout, 1, MPI_LONG,
      MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD);

  // Get standard deviation for time taken
  MPI_Allreduce(&group2_local_time_taken_sum, &group2_global_time_taken_sum, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);
  group2_global_time_taken_avg = group2_global_time_taken_sum / (double)group2_size;
  if (rank >= group2_start_idx) {
    for (int j=0;j<users.size();++j) {
      double time = users[j]->GetTimeTaken();
      group2_local_time_taken_diff += (time - group2_global_time_taken_avg) * (time - group2_global_time_taken_avg);
    }
  }
  MPI_Reduce(&group2_local_time_taken_diff, &group2_global_time_taken_diff, 1, MPI_DOUBLE, MPI_SUM, 0,
      MPI_COMM_WORLD);
  group2_global_time_taken_std = sqrt(group2_global_time_taken_diff / (double)group2_size);

  double group2_global_99_lock_time_diff;
  double group2_global_99_lock_time_avg = group2_global_99_lock_time / (double)(group2_size);
  double group2_global_lock_time_avg = group2_global_lock_time / (double)(group2_size);
  double group2_global_95_lock_time_avg = group2_global_95_lock_time / (double)(group2_size);
  double group2_local_99_lock_time_diff = 0;
  if (rank >= group2_start_idx) {
    group2_local_99_lock_time_diff = (group2_global_99_lock_time_avg - group2_local_99_lock_time) *
      (group2_global_99_lock_time_avg - group2_local_99_lock_time);
  }
  MPI_Reduce(&group2_local_99_lock_time_diff, &group2_global_99_lock_time_diff, 1, MPI_DOUBLE, MPI_SUM,
      0, MPI_COMM_WORLD);
  double group2_global_99_lock_time_std = sqrt(group2_global_99_lock_time_diff / (double)group2_size);

  if (rank==0) {
    cout << endl;
    cout << "Global Total Lock # = " << group2_global_sum << "(# nodes: " <<
      num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Unlock # = " << group2_global_unlock_sum << "(# nodes: " <<
      num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Success # = " << group2_global_lock_success <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Failure # = " << group2_global_lock_failure <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Timeout # = " << group2_global_timeout <<
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Lock Time = " << group2_global_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Shared Lock Time = " <<
      group2_global_remote_shared_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Exclusive Lock Time = " <<
      group2_global_remote_exclusive_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Shared Lock Time = " <<
      group2_global_group2_local_shared_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Exclusive Lock Time = " <<
      group2_global_group2_local_exclusive_lock_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Send Message Time = " <<
      group2_global_send_message_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Receive Message Time = " <<
      group2_global_receive_message_time / num_servers <<
      " ns " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Read Count = " <<
      group2_global_avg_rdma_read_count / num_servers <<
      "" << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Atomic Count = " <<
      group2_global_avg_rdma_atomic_count / num_servers <<
      "" << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Read Time = " <<
      group2_global_rdma_read_time / (double)group2_global_rdma_read_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total RDMA Atomic Time = " <<
      group2_global_rdma_atomic_time <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total RDMA Atomic Count = " <<
      group2_global_rdma_atomic_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Atomic Time = " <<
      group2_global_rdma_atomic_time / (double)group2_global_rdma_atomic_count <<
      " " << "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Overall Avg CPU Usage = " <<
      group2_global_cpu_usage / num_servers << "% "
      "(# nodes: " << num_servers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Local Time Taken Sum = " << group2_local_time_taken_sum << endl;

    cerr << lock_mode_str << "_GROUP2," << workload_type_str <<
      "," << shared_exclusive_rule_str << "," << exclusive_shared_rule_str << "," <<
      exclusive_exclusive_rule_str << "," << fail_retry << "," << poll_retry << "," <<
      sleep_time << "," << think_time << "," << num_servers << "," << group2_size <<
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
    double avg_poll_when_success = (group2_global_lock_success_with_poll == 0) ? 0.0 :
      (double)group2_global_sum_poll_when_success / (double)group2_global_lock_success_with_poll;
    double avg_retry_when_success = (group2_global_lock_success_with_retry == 0) ? 0.0 :
      (double)group2_global_sum_retry_when_success / (double)group2_global_lock_success_with_retry;
    double avg_index_when_timeout = (group2_global_timeout == 0) ? 0.0 :
      (double)group2_global_sum_index_when_timeout / (double)group2_global_timeout;

    cerr << group2_global_lock_time_avg <<
      "," << group2_global_99_lock_time << "," << group2_global_95_lock_time << "," <<
      (long)((double)group2_global_sum / (double)group2_global_time_taken_avg) << "," <<
      group2_global_sum << "," << group2_global_lock_success << "," <<
      group2_global_lock_success_with_retry << "," <<
      avg_retry_when_success << "," <<
      group2_global_lock_success_with_poll << "," <<
      avg_poll_when_success << "," <<
      group2_global_lock_failure << "," <<
      group2_global_lock_contention << "," <<
      group2_global_timeout << "," <<
      avg_index_when_timeout << "," <<
      0 << "," << 0 <<
      "," << group2_global_time_taken_avg / (double)(1000*1000*1000) << "," <<
      group2_global_time_taken_std / (double)(1000*1000*1000) << "," <<
      group2_max_tx_throughput << "," << group2_max_lock_throughput << "," <<
      group2_global_rdma_send_count << "," << group2_global_rdma_recv_count << "," <<
      group2_global_rdma_write_count << "," << group2_global_rdma_read_count << "," <<
      group2_global_rdma_atomic_count <<
      endl;
  }




  // all
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

  uint64_t global_max_tx_throughput = 0;
  uint64_t global_max_lock_throughput = 0;

  if (rank == 0) {
    for (int i = 0; i < min_time_taken; ++i) {
      if (global_max_tx_throughput < global_tx_throughput[i])
        global_max_tx_throughput = global_tx_throughput[i];

      if (global_max_lock_throughput < global_lock_throughput[i])
        global_max_lock_throughput = global_lock_throughput[i];
    }
  }

  if (rank >= group1_start_idx) {
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
        if (local_99_lock_time < simulator->Get99PercentileLockTime())
          local_99_lock_time = simulator->Get99PercentileLockTime();
        if (local_95_lock_time < simulator->Get95PercentileLockTime())
          local_95_lock_time = simulator->Get95PercentileLockTime();
      }
    }
    for (unsigned int j=0;j<users.size();++j) {
      LockSimulator* simulator = users[j];
      local_time_taken_sum += simulator->GetTimeTaken();
    }
  }

  local_lock_contention = lock_manager->GetTotalLockContention();
  local_lock_success_with_poll = lock_manager->GetTotalLockSuccessWithPoll();
  local_sum_poll_when_success = lock_manager->GetTotalSumPollWhenSuccess();

  local_remote_shared_lock_time =
    lock_manager->GetAverageRemoteSharedLockTime();
  local_remote_exclusive_lock_time =
    lock_manager->GetAverageRemoteExclusiveLockTime();
  local_local_shared_lock_time =
    lock_manager->GetAverageLocalSharedLockTime();
  local_local_exclusive_lock_time =
    lock_manager->GetAverageLocalExclusiveLockTime();
  local_send_message_time = lock_manager->GetAverageSendMessageTime();
  local_receive_message_time = lock_manager->GetAverageReceiveMessageTime();
  local_avg_rdma_read_count = lock_manager->GetAverageRDMAReadCount();
  local_avg_rdma_atomic_count = lock_manager->GetAverageRDMAAtomicCount();

  local_rdma_read_count = lock_manager->GetTotalRDMAReadCount();
  local_rdma_recv_count = lock_manager->GetTotalRDMARecvCount();
  local_rdma_send_count = lock_manager->GetTotalRDMASendCount();
  local_rdma_write_count = lock_manager->GetTotalRDMAWriteCount();
  local_rdma_atomic_count = lock_manager->GetTotalRDMAAtomicCount();

  local_rdma_read_time = lock_manager->GetTotalRDMAReadTime();
  local_rdma_atomic_time = lock_manager->GetTotalRDMAAtomicTime();

  MPI_Barrier(MPI_COMM_WORLD);
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  double local_cpu_usage_local = usage.total_cpu / usage.num_sample;
  if (rank < group1_start_idx) {
    local_cpu_usage = local_cpu_usage_local;
  }
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
  if (rank < group1_start_idx) {
    global_cpu_usage_avg = global_cpu_usage / (double)num_servers;
  }
  if (rank < group1_start_idx) {
    local_cpu_diff = (global_cpu_usage_avg - local_cpu_usage) *
      (global_cpu_usage_avg - local_cpu_usage);
  }
  MPI_Reduce(&local_cpu_diff, &global_cpu_diff, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  global_cpu_usage_std = sqrt(global_cpu_diff / (double)num_servers);

  // Get standard deviation for time taken
  MPI_Allreduce(&local_time_taken_sum, &global_time_taken_sum, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);
  global_time_taken_avg = global_time_taken_sum / (double)num_users;
  if (rank >= group1_start_idx) {
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
  if (rank >= group1_start_idx) {
    local_99_lock_time_diff = (global_99_lock_time_avg - local_99_lock_time) *
      (global_99_lock_time_avg - local_99_lock_time);
  }
  MPI_Reduce(&local_99_lock_time_diff, &global_99_lock_time_diff, 1, MPI_DOUBLE, MPI_SUM,
      0, MPI_COMM_WORLD);
  double global_99_lock_time_std = sqrt(global_99_lock_time_diff / (double)num_users);

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

    cerr << lock_mode_str << "_ALL," << workload_type_str <<
      "," << shared_exclusive_rule_str << "," << exclusive_shared_rule_str << "," <<
      exclusive_exclusive_rule_str << "," << fail_retry << "," << poll_retry << "," <<
      sleep_time << "," << think_time << "," << num_servers << "," << num_users <<
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
      global_max_tx_throughput << "," << global_max_lock_throughput << "," <<
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
}

void* RunLockSimulator(void* args) {
  LockSimulator* user = (LockSimulator*)args;
  user->Run();
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

  lastCPU = times(&timeSample);
  lastSysCPU = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;

  file = fopen("/proc/cpuinfo", "r");
  numProcessors = 0;
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
}
