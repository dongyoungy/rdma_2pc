#include <iostream>
#include <vector>
#include <arpa/inet.h>
#include <pthread.h>
#include <infiniband/verbs.h>
#include <sys/times.h>
#include "mpi.h"
#include "lock_simulator.h"
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

  if (argc < 9) {
    cout << argv[0] << " <work_dir> <num_lock_object> <num_requests>" <<
      " <num_users> <lock_mode> <workload_type> <local_workload_ratio> " <<
      "<shared_lock_ratio> <duration> <transaction_delay> <transaction_delay_min> <transaction_delay_max>" << endl;
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

  int num_lock_object         = atoi(argv[2]);
  int num_requests            = atoi(argv[3]);
  int num_users               = atoi(argv[4]);
  int lock_mode               = atoi(argv[5]);
  int workload_type           = atoi(argv[6]);
  double local_workload_ratio = atof(argv[7]);
  double shared_lock_ratio    = atof(argv[8]);
  int duration                = atoi(argv[9]);
  int transaction_delay_num   = atoi(argv[10]);
  int transaction_delay_min   = atoi(argv[11]);
  int transaction_delay_max   = atoi(argv[12]);

  bool transaction_delay = (transaction_delay_num == 0) ? false : true;

  string lock_mode_str;
  if (lock_mode == LockManager::LOCK_LOCAL) {
    lock_mode_str = "LOCK_MODE_PROXY";
  } else if (lock_mode == LockManager::LOCK_REMOTE) {
    lock_mode_str = "LOCK_MODE_DIRECT";
  }

  string workload_type_str;
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
  if (workload_type == LockSimulator::WORKLOAD_HOTSPOT) {
    local_workload_ratio_str = "N/A";
  }

  string shared_lock_ratio_str;
  sprintf(buf, "%.4f%%", shared_lock_ratio * 100);
  shared_lock_ratio_str = buf;

  string transaction_delay_str = (transaction_delay ? "TRUE" : "FALSE");

  if (rank == 0) {
    cout << "Type of Workload = " << workload_type_str << " (Shared Lock: " <<
      shared_lock_ratio * 100.0 << "%)" << endl;
    cout << "Duration = " << duration << " seconds"  << endl;
  }

  LockManager* lock_manager = new LockManager(argv[1], rank, num_managers,
      num_lock_object, lock_mode);

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
    LockSimulator* simulator = new LockSimulator(lock_manager,
        rank*num_managers+(i+1), // id
        num_managers,
        num_lock_object,
        num_requests,
        duration,
        false, // verbose
        true, // measure lock time
        workload_type, // type of workload
        lock_mode,
        local_workload_ratio,
        shared_lock_ratio,
        transaction_delay,
        transaction_delay_min,
        transaction_delay_max
        );
    lock_manager->RegisterUser(rank*num_managers+(i+1), simulator);
    users.push_back(simulator);
  }

  sleep(3);

  if (lock_manager->InitializeLockClients()) {
     cerr << "InitializeLockClients() failed." << endl;
     exit(-1);
  }

  sleep(1);

  for (int i=0;i<num_users;++i) {
    //users[i]->Run();
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

  for (int i=0;i<users.size();++i) {
    LockSimulator* simulator = users[i];
    while (simulator->GetState() != LockSimulator::STATE_DONE) {
       sleep(1);
    }
  }

  long local_sum                            = 0;
  long global_sum                           = 0;
  long local_unlock_sum                     = 0;
  long global_unlock_sum                    = 0;
  long local_lock_success                   = 0;
  long global_lock_success                  = 0;
  long local_lock_failure                   = 0;
  long global_lock_failure                  = 0;
  double local_lock_time                   = 0;
  double global_lock_time                  = 0;
  double local_cpu_usage                   = 0;
  double global_cpu_usage                  = 0;
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
        if (simulator->IsLockTimeMeasured()) {
          local_lock_time += simulator->GetAverageTimeTakenToLock();
          local_99_lock_time += simulator->Get99PercentileLockTime();
        }
      }
      //MPI_Barrier(MPI_COMM_WORLD);
    }
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
  MPI_Reduce(&local_cpu_usage, &global_cpu_usage, 1, MPI_DOUBLE, MPI_SUM, 0,
      MPI_COMM_WORLD);
  MPI_Reduce(&local_99_lock_time, &global_99_lock_time, 1, MPI_DOUBLE, MPI_SUM, 0,
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

    cout << "Local Avg CPU Usage = " << local_cpu_usage << "% "
      "(" << "ID = " << rank <<  " ,# nodes: " << num_managers <<
      ", duration: " << duration << ", mode: " << lock_mode_str << ")" << endl;
  MPI_Barrier(MPI_COMM_WORLD);

  if (rank==0) {
    cout << endl;
    cout << "Global Total Lock # = " << global_sum << "(# nodes: " <<
      num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Unlock # = " << global_unlock_sum << "(# nodes: " <<
      num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Success # = " << global_lock_success <<
      "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Total Lock Failure # = " << global_lock_failure <<
      "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Lock Time = " << global_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Shared Lock Time = " <<
      global_remote_shared_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Remote Exclusive Lock Time = " <<
      global_remote_exclusive_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Shared Lock Time = " <<
      global_local_shared_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Local Exclusive Lock Time = " <<
      global_local_exclusive_lock_time / num_managers <<
      " ns " << "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Send Message Time = " <<
      global_send_message_time / num_managers <<
      " ns " << "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Global Average Receive Message Time = " <<
      global_receive_message_time / num_managers <<
      " ns " << "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;
    cout << "Overall Avg CPU Usage = " <<
      global_cpu_usage / num_managers << "% "
      "(# nodes: " << num_managers << ", duration: " <<
      duration << ", mode: " << lock_mode_str << ")" << endl;

    cerr << lock_mode_str << "," << workload_type_str << "," << num_managers
      << "," << num_lock_object << "," << num_requests << "," <<
      local_workload_ratio_str << "," << shared_lock_ratio_str << "," <<
      transaction_delay_str << ",";
    if (transaction_delay) {
      cerr << transaction_delay_min << "," << transaction_delay_max << ",";
    } else {
      cerr << "N/A,N/A,";
    }
    cerr << global_lock_time / num_managers <<
      "," << global_99_lock_time / num_managers << "," <<
      (long)((double)global_sum / (double)duration) << "," << global_cpu_usage / num_managers << endl;
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
