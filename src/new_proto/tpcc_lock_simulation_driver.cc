#include <iostream>
#include <vector>
#include <cmath>
#include <arpa/inet.h>
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

  if (argc != 18) {
    cout << argv[0] << " <work_dir> <num_tx>" <<
      " <num_users> <lock_mode>" <<
      " <shared_exclusive_rule> <exclusive_shared_rule> <exclusive_exclusive_rule>" <<
      " <fail_retry> <poll_retry> <sleep_time_for_timeout> <think_time>"
      " <transaction_delay> <transaction_delay_min> " <<
      "<transaction_delay_max> <miin_backoff_time> <max_backoff_time> <rand_seed>" << endl;
    exit(1);
  }

  int num_managers, rank;

  MPI_Comm_size(MPI_COMM_WORLD, &num_managers);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0) {
    if (1 == htons(1)) {
      cout << "The current machine uses BIG ENDIAN" << endl;
    } else {
      cout << "The current machine uses LITTLE ENDIAN" << endl;
    }
  }

  int k=2;
  long num_tx                  = atol(argv[k++]);
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

  if (num_users != 1) {
     cerr << "# of users must be 1." << endl;
     exit(-1);
  }
  if (num_managers > 32) {
     cerr << "# of nodes must be less than 33" << endl;
     exit(-1);
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
      lock_mode == LOCK_PROXY_QUEUE || LOCK_REMOTE_QUEUE) {
    shared_exclusive_rule_str = "N/A";
    exclusive_shared_rule_str = "N/A";
    exclusive_exclusive_rule_str = "N/A";
  }

  if (rank == 0) {
    cout << "Type of Workload = " << workload_type_str << endl;
    cout << "SHARED -> EXCLUSIVE = " << shared_exclusive_rule_str << endl;
    cout << "EXCLUSIVE -> SHARED = " << exclusive_shared_rule_str << endl;
    cout << "EXCLUSIVE -> EXCLUSIVE = " << exclusive_exclusive_rule_str << endl;
    cout << "Num Tx = " << num_tx << endl;
  }

  LockManager::SetSharedExclusiveRule(shared_exclusive_rule);
  LockManager::SetExclusiveSharedRule(exclusive_shared_rule);
  LockManager::SetExclusiveExclusiveRule(exclusive_exclusive_rule);
  LockManager::SetFailRetry(fail_retry);
  LockManager::SetPollRetry(poll_retry);

  LockManager* lock_manager = new LockManager(argv[1], rank, num_managers,
      700000, lock_mode);

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

  vector<LockSimulator*> users;
  for (int i=0;i<num_users;++i) {
    TPCCLockSimulator* simulator = new TPCCLockSimulator(lock_manager,
        (int)pow(2.0, rank), // id
        num_managers,
        num_tx,
        seed,
        false, // verbose
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
    lock_manager->RegisterUser((int)pow(2.0, rank), simulator);
    users.push_back(simulator);
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

  for (int i=0;i<num_users;++i) {
    pthread_t lock_simulator_thread;
    if (pthread_create(&lock_simulator_thread, NULL, &RunLockSimulator,
          (void*)users[i])) {
      cerr << "pthread_create() error." << endl;
      exit(-1);
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

  int time_taken2 = 0;
  for (int i=0;i<users.size();++i) {
    LockSimulator* simulator = users[i];
    while (simulator->GetState() != LockSimulator::STATE_DONE) {
      ++time_taken2;
      if (rank == 0) {
        cout << time_taken2 << " : " << users[0]->GetCount() <<
          "," << users[0]->GetCurrentBackoff() << endl;
      }
       sleep(1);
    }
  }

  time(&end_time);
  double time_taken = difftime(end_time, start_time);

  long local_sum                           = 0;
  long global_sum                          = 0;
  long local_unlock_sum                    = 0;
  long global_unlock_sum                   = 0;
  long local_lock_success                  = 0;
  long global_lock_success                 = 0;
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

  double local_rdma_read_count = 0;
  double global_rdma_read_count = 0;
  double local_rdma_atomic_count = 0;
  double global_rdma_atomic_count = 0;

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

  for (int i=0;i<num_managers;++i) {
    if (rank==i) {
      //cout << "Node = " << rank << endl;
      for (int j=0;j<users.size();++j) {
        LockSimulator* simulator = users[j];
        local_sum += simulator->GetTotalNumLocks();
        local_unlock_sum += simulator->GetTotalNumUnlocks();
        local_lock_success += simulator->GetTotalNumLockSuccess();
        local_lock_failure += simulator->GetTotalNumLockFailure();
        local_timeout += simulator->GetTotalNumTimeout();
        if (simulator->IsLockTimeMeasured()) {
          local_lock_time += simulator->GetAverageTimeTakenToLock();
          local_99_lock_time += simulator->Get99PercentileLockTime();
          local_95_lock_time += simulator->Get95PercentileLockTime();
        }
      }
      //MPI_Barrier(MPI_COMM_WORLD);
    }
  }
  for (int j=0;j<users.size();++j) {
    LockSimulator* simulator = users[j];
    local_time_taken_sum += simulator->GetTimeTaken();
  }

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
  local_rdma_read_count = lock_manager->GetAverageRDMAReadCount();
  local_rdma_atomic_count = lock_manager->GetAverageRDMAAtomicCount();

  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  local_cpu_usage = usage.total_cpu / usage.num_sample;
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
  MPI_Reduce(&local_rdma_read_count,
      &global_rdma_read_count,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(&local_rdma_atomic_count,
      &global_rdma_atomic_count,
      1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    cout << "Local Avg CPU Usage = " << local_cpu_usage << "% "
      "(" << "ID = " << rank <<  " ,# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
  MPI_Barrier(MPI_COMM_WORLD);

  // Get standard deviation for cpu usage
  global_cpu_usage_avg = global_cpu_usage / (double)num_managers;
  local_cpu_diff = (global_cpu_usage_avg - local_cpu_usage) *
    (global_cpu_usage_avg - local_cpu_usage);
  MPI_Reduce(&local_cpu_diff, &global_cpu_diff, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  global_cpu_usage_std = sqrt(global_cpu_diff / num_managers);

  // Get standard deviation for time taken
  MPI_Allreduce(&local_time_taken_sum, &global_time_taken_sum, 1, MPI_DOUBLE, MPI_SUM,
      MPI_COMM_WORLD);
  global_time_taken_avg = global_time_taken_sum / (double)(num_managers * users.size());
  for (int j=0;j<users.size();++j) {
    double time = users[j]->GetTimeTaken();
    local_time_taken_diff += (time - global_time_taken_avg) * (time - global_time_taken_avg);
  }
  MPI_Reduce(&local_time_taken_diff, &global_time_taken_diff, 1, MPI_DOUBLE, MPI_SUM, 0,
      MPI_COMM_WORLD);
  global_time_taken_std = sqrt(global_time_taken_diff / (num_managers * users.size()));

  double global_99_lock_time_diff;
  double global_99_lock_time_avg = global_99_lock_time / (double)num_managers;
  double local_99_lock_time_diff = (global_99_lock_time_avg - local_99_lock_time) *
    (global_99_lock_time_avg - local_99_lock_time);
  MPI_Reduce(&local_99_lock_time_diff, &global_99_lock_time_diff, 1, MPI_DOUBLE, MPI_SUM,
      0, MPI_COMM_WORLD);
  double global_99_lock_time_std = sqrt(global_99_lock_time_diff / num_managers);

  if (rank==0) {
    cout << endl;
    cout << "Global Total Lock # = " << global_sum << "(# nodes: " <<
      num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Unlock # = " << global_unlock_sum << "(# nodes: " <<
      num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Success # = " << global_lock_success <<
      "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Failure # = " << global_lock_failure <<
      "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Timeout # = " << global_timeout <<
      "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Lock Time = " << global_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Shared Lock Time = " <<
      global_remote_shared_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Exclusive Lock Time = " <<
      global_remote_exclusive_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Shared Lock Time = " <<
      global_local_shared_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Exclusive Lock Time = " <<
      global_local_exclusive_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Send Message Time = " <<
      global_send_message_time / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Receive Message Time = " <<
      global_receive_message_time / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Read Count = " <<
      global_rdma_read_count / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average RDMA Atomic Count = " <<
      global_rdma_atomic_count / num_managers <<
      " ns " << "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Overall Avg CPU Usage = " <<
      global_cpu_usage / num_managers << "% "
      "(# nodes: " << num_managers <<
      ", mode: " << lock_mode_str << ")" << endl;
    cout << "Local Time Taken Sum = " << local_time_taken_sum << endl;

    cerr << lock_mode_str << "," << workload_type_str <<
      "," << shared_exclusive_rule_str << "," << exclusive_shared_rule_str << "," <<
      exclusive_exclusive_rule_str << "," << fail_retry << "," << poll_retry << "," <<
      sleep_time << "," << think_time << "," << num_managers <<
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
    cerr << global_lock_time / num_managers <<
      "," << global_99_lock_time << "," << global_95_lock_time << "," <<
      (long)((double)global_sum / (double)time_taken) << "," <<
      global_sum << "," << global_lock_success << "," << global_lock_failure << "," <<
      global_timeout << "," <<
      global_cpu_usage_avg << "," << global_cpu_usage_std <<
      "," << global_time_taken_avg / (double)(1000*1000*1000) << "," <<
      global_time_taken_std / (double)(1000*1000*1000) << endl;
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
