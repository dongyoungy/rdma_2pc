#include <iostream>
#include <vector>
#include <cmath>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/times.h>
#include <infiniband/verbs.h>
#include "mpi.h"
#include "constants.h"
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

  if (argc != 16) {
    cout << "USAGE: " << argv[0] << " <work_dir> <num_lock_object>" <<
      " <num_tx> <num_request_per_tx> <num_users> <lock_mode> <shared_exclusive_rule> " <<
      "<exclusive_shared_rule> <exclusive_exclusive_rule> <workload_type> <local_workload_ratio> "<<
      "<shared_lock_ratio> <min_backoff_time> <max_backoff_time> <rand_seed>" << endl;
    exit(1);
  }

  int num_managers = 1;
  int rank = 0;

  if (1 == htons(1)) {
    cout << "The current machine uses BIG ENDIAN" << endl;
  } else {
    cout << "The current machine uses LITTLE ENDIAN" << endl;
  }

  int num_lock_object          = atoi(argv[2]);
  long num_tx                  = atol(argv[3]);
  int num_request_per_tx       = atoi(argv[4]);
  int num_users                = atoi(argv[5]);
  int lock_mode                = atoi(argv[6]);
  int shared_exclusive_rule    = atoi(argv[7]);
  int exclusive_shared_rule    = atoi(argv[8]);
  int exclusive_exclusive_rule = atoi(argv[9]);
  int workload_type            = atoi(argv[10]);
  double local_workload_ratio  = atof(argv[11]);
  double shared_lock_ratio     = atof(argv[12]);
  int min_backoff_time         = atoi(argv[13]);
  int max_backoff_time         = atoi(argv[14]);
  long seed                    = atol(argv[15]);

  string workload_type_str, shared_lock_ratio_str;
  if (workload_type == LockSimulator::WORKLOAD_UNIFORM) {
    workload_type_str = "UNIFORM";
  } else if (workload_type == LockSimulator::WORKLOAD_HOTSPOT) {
    workload_type_str = "HOTSPOT";
  } else if (workload_type == LockSimulator::WORKLOAD_ALL_LOCAL) {
    workload_type_str = "ALL_LOCAL";
  } else if (workload_type == LockSimulator::WORKLOAD_MIXED) {
    char buf[32];
    sprintf(buf, "MIXED (local: %.2f %%)", local_workload_ratio * 100);
    workload_type_str = buf;
    sprintf(buf, "Shared Lock Ratio = %.2f %%", shared_lock_ratio * 100);
    shared_lock_ratio_str = buf;
  }

  string lock_method_str;
  if (lock_mode == LOCK_REMOTE_POLL) {
    lock_method_str = "CLIENT-BASED/DIRECT/POLL";
  } else if (lock_mode == LOCK_PROXY_RETRY) {
    lock_method_str = "SERVER-BASED/PROXY/RETRY";
  } else if (lock_mode == LOCK_PROXY_QUEUE) {
    lock_method_str = "SERVER-BASED/PROXY/QUEUE";
  } else if (lock_mode == LOCK_REMOTE_NOTIFY) {
    lock_method_str = "CLIENT-BASED/DIRECT/NOTIFY";
  }

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

  if (rank == 0) {
    cout << "Lock Method = " << lock_method_str << endl;
    cout << "Type of Workload = " << workload_type_str << endl;
    cout << shared_lock_ratio_str << endl;
    cout << "SHARED -> EXCLUSIVE = " << shared_exclusive_rule_str << endl;
    cout << "EXCLUSIVE -> SHARED = " << exclusive_shared_rule_str << endl;
    cout << "EXCLUSIVE -> EXCLUSIVE = " << exclusive_exclusive_rule_str << endl;
    cout << "Num Tx = " << num_tx << endl;
    cout << "Num Requests per Tx = " << num_request_per_tx << endl;
  }

  LockManager::SetSharedExclusiveRule(shared_exclusive_rule);
  LockManager::SetExclusiveSharedRule(exclusive_shared_rule);
  LockManager::SetExclusiveExclusiveRule(exclusive_exclusive_rule);

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
        (uint32_t)pow(2.0, i), // id
        num_managers,
        num_lock_object,
        num_tx, // num lock requests
        num_request_per_tx,
        seed,
        false, // verbose
        true, // measure lock
        workload_type,
        lock_mode,
        local_workload_ratio,
        shared_lock_ratio,
        0,0,0, // tx delays
        min_backoff_time,
        max_backoff_time
        );
    lock_manager->RegisterUser((uint32_t)pow(2.0, i), simulator);
    users.push_back(simulator);
  }

  sleep(3);

  if (lock_manager->InitializeLockClients()) {
     cerr << "InitializeLockClients() failed." << endl;
     exit(-1);
  }

  sleep(1);

  time_t start_time;
  time_t end_time;

  time(&start_time);

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


  int count = 0;
  for (int i=0;i<users.size();++i) {
    LockSimulator* simulator = users[i];
    while (simulator->GetState() != LockSimulator::STATE_DONE) {
       sleep(1);
       cout << count << " : " << users[0]->GetCount() <<
         "," << users[0]->GetCurrentBackoff() << endl;
       ++count;
       //if (count == 3) {
         //lock_manager->SwitchToLocal();
       //}
    }
  }

  time(&end_time);
  double time_taken = difftime(end_time, start_time);

  for (int i=0;i<num_managers;++i) {
    if (rank==i) {
      cout << "Node = " << rank << endl;
      for (int j=0;j<users.size();++j) {
        LockSimulator* simulator = users[j];
        cout << "Total Lock # = " << simulator->GetTotalNumLocks() << endl;
        cout << "Total Unlock # = " << simulator->GetTotalNumUnlocks() << endl;
        cout << "Time Taken = " << simulator->GetTimeTaken() << " s" << endl;
      }
    }
  }
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  cout << "Avg CPU Usage = " << usage.total_cpu / usage.num_sample << endl;
  cout<< "Total Time Taken = " << time_taken << endl;
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
    sleep(1);
  }
}
