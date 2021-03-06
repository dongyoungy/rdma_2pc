#include <iostream>
#include <vector>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/times.h>
#include <infiniband/verbs.h>
#include "mpi.h"
#include "lock_simulator.h"
#include "lock_manager.h"

using namespace std;
using namespace rdma::n_cosed;

void* RunLockManager(void* args);
void* RunLockSimulator(void* args);
void* MeasureCPUUsage(void* args);

struct CPUUsage {
  double total_cpu;
  double num_sample;
  bool terminate;
};

int main(int argc, char** argv) {

  if (argc != 9) {
    cout << argv[0] << " <work_dir> <num_lock_object>" <<
      " <num_nodes> <lock_mode> <workload_type> <local_workload_ratio> "<<
      "<shared_lock_ratio> <duration>" << endl;
    exit(1);
  }

  int rank = 0;

  if (1 == htons(1)) {
    cout << "The current machine uses BIG ENDIAN" << endl;
  } else {
    cout << "The current machine uses LITTLE ENDIAN" << endl;
  }

  int num_lock_object         = atoi(argv[2]);
  int num_nodes               = atoi(argv[3]);
  int lock_mode               = atoi(argv[4]);
  int workload_type           = atoi(argv[5]);
  double local_workload_ratio = atof(argv[6]);
  double shared_lock_ratio    = atof(argv[7]);
  int duration                = atoi(argv[8]);

  string workload_type_str;
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
  }

  LockManager** nodes = new LockManager*[num_nodes];

  for (int i = 0; i < num_nodes;++i) {
    if (i == 0) {
      cout << "Type of Workload = " << workload_type_str << endl;
      cout << "Duration = " << duration << " seconds"  << endl;
    }

    LockManager* lock_manager = new LockManager(argv[1], i+1, num_nodes,
        num_lock_object, lock_mode);

    nodes[i] = lock_manager;

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

  vector<LockSimulator*> users;
  for (int i=0;i<num_nodes;++i) {
    LockSimulator* simulator = new LockSimulator(nodes[i],
        i+1, // id
        num_nodes,
        num_lock_object,
        1, // num lock requests
        duration,
        LockManager::PRINT_DEBUG, // verbose
        true, // measure lock
        workload_type,
        lock_mode,
        local_workload_ratio,
        shared_lock_ratio
        );
    nodes[i]->RegisterUser(i+1, simulator);
    users.push_back(simulator);
  }

  sleep(2);
  //sleep(10);

  for (int i=0;i<num_nodes;++i) {
    if (nodes[i]->InitializeLockClients()) {
      cerr << "InitializeLockClients() failed." << endl;
      exit(-1);
    }
  }

  sleep(1);
  //sleep(10);

  for (int i=0;i<users.size();++i) {
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
  bool done = false;
  while (true) {
    for (int i=0;i<users.size();++i) {
      LockSimulator* simulator = users[i];
      if (simulator->GetState() == LockSimulator::STATE_DONE) {
        done = true;
      }
        //++count;
        //if (count == 3) {
        //lock_manager->SwitchToLocal();
        //}
    }
    if (done) break;
    sleep(1);
  }

  for (int j=0;j<users.size();++j) {
    LockSimulator* simulator = users[j];
    cout << "Total Lock # = " << simulator->GetTotalNumLocks() << endl;
    cout << "Total Unlock # = " << simulator->GetTotalNumUnlocks() << endl;
  }
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  cout << "Avg CPU Usage = " << usage.total_cpu / usage.num_sample << endl;
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
