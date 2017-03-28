#include <iostream>
#include <vector>
#include <cmath>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/times.h>
#include <infiniband/verbs.h>
#include "mpi.h"
#include "constants.h"
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

  if (argc != 14) {
    cout << "USAGE: " << argv[0] << " <work_dir>" <<
      "<num_lock_manager> <num_tx> <num_users> <lock_mode> <shared_exclusive_rule> " <<
      "<exclusive_shared_rule> <exclusive_exclusive_rule> "<<
      "<min_backoff_time> <max_backoff_time> <sleep_time> <think_time> <rand_seed>" << endl;
    exit(1);
  }

  int rank = 0;

  if (1 == htons(1)) {
    cout << "The current machine uses BIG ENDIAN" << endl;
  } else {
    cout << "The current machine uses LITTLE ENDIAN" << endl;
  }

  int k = 2;
  int num_managers             = atoi(argv[k++]);
  long num_tx                  = atol(argv[k++]);
  int num_users                = atoi(argv[k++]);
  int lock_mode                = atoi(argv[k++]);
  int shared_exclusive_rule    = atoi(argv[k++]);
  int exclusive_shared_rule    = atoi(argv[k++]);
  int exclusive_exclusive_rule = atoi(argv[k++]);
  int min_backoff_time         = atoi(argv[k++]);
  int max_backoff_time         = atoi(argv[k++]);
  int sleep_time               = atoi(argv[k++]);
  int think_time               = atoi(argv[k++]);
  long seed                    = atol(argv[k++]);

  string workload_type_str, shared_lock_ratio_str;
  workload_type_str = "TPCC";
  shared_lock_ratio_str = "N/A";

  string lock_method_str;
  if (lock_mode == LOCK_REMOTE_POLL) {
    lock_method_str = "CLIENT-BASED/DIRECT/RETRY";
  } else if (lock_mode == LOCK_PROXY_RETRY) {
    lock_method_str = "SERVER-BASED/PROXY/RETRY";
  } else if (lock_mode == LOCK_PROXY_QUEUE) {
    lock_method_str = "SERVER-BASED/PROXY/QUEUE";
  } else if (lock_mode == LOCK_REMOTE_NOTIFY) {
    lock_method_str = "CLIENT-BASED/DIRECT/NOTIFY";
  } else if (lock_mode == LOCK_REMOTE_QUEUE) {
    lock_method_str = "CLIENT-BASED/DIRECT/QUEUE";
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

  cout << "Lock Method = " << lock_method_str << endl;
  cout << "Type of Workload = " << workload_type_str << endl;
  cout << "SHARED -> EXCLUSIVE = " << shared_exclusive_rule_str << endl;
  cout << "EXCLUSIVE -> SHARED = " << exclusive_shared_rule_str << endl;
  cout << "EXCLUSIVE -> EXCLUSIVE = " << exclusive_exclusive_rule_str << endl;
  cout << "Num Tx = " << num_tx << endl;
  cout << "Num Managers = " << num_managers << endl;
  cout << "Num Users Per Manager = " << num_users << endl;

  LockManager::SetSharedExclusiveRule(shared_exclusive_rule);
  LockManager::SetExclusiveSharedRule(exclusive_shared_rule);
  LockManager::SetExclusiveExclusiveRule(exclusive_exclusive_rule);

  vector<LockManager*> managers;
  for (int i = 0; i < num_managers; ++i) {
    LockManager* lock_manager = new LockManager(argv[1], i, num_managers,
        700000, lock_mode);

    if (lock_manager->Initialize()) {
      cerr << "LockManager initialization failure." << endl;
      exit(-1);
    }
    managers.push_back(lock_manager);

    pthread_t lock_manager_thread;
    if (pthread_create(&lock_manager_thread, NULL, RunLockManager,
          (void*)lock_manager)) {
      cerr << "pthread_create() error." << endl;
      exit(-1);
    }
  }

  vector<LockSimulator*> users;
  for (int i = 0; i < num_managers; ++i) {
    for (int j=0;j<num_users;++j) {
      bool verbose = false;
      TPCCLockSimulator* simulator = new TPCCLockSimulator(managers[i],
          //(uint32_t)pow(2.0, rank*num_users+i), // id
          j,
          i,
          WORKLOAD_UNIFORM,
          num_managers,
          num_tx,
          seed,
          verbose, // verbose
          true, // measure lock time
          lock_mode,
          0,0,0,
          min_backoff_time,
          max_backoff_time,
          sleep_time,
          think_time
          );
      //managers[i]->RegisterUser((uint32_t)pow(2.0, rank*num_users+i), simulator);
      managers[i]->RegisterUser(j, simulator);
      users.push_back(simulator);
    }
  }

  for (int i = 0; i < num_managers; ++i) {
    if (managers[i]->InitializeLockClients()) {
      cerr << "InitializeLockClients() failed." << endl;
      exit(-1);
    }
    while (!managers[i]->IsClientsInitialized()) {
      usleep(250000);
    }
  }

  for (int i=0;i<num_users;++i) {
    LockManager::user_to_node_map_[i] = 0;
  }

  time_t start_time;
  time_t end_time;

  cout << "TPC-C Simulation starting.." << endl;
  time(&start_time);

  for (int i=0;i<users.size();++i) {
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
       cout << users[i]->GetID() << "," << count << " : " << users[i]->GetCount() <<
         "," << users[i]->GetCurrentBackoff() << endl;
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
