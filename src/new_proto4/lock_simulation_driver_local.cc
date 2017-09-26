#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <pthread.h>
#include <sys/times.h>
#include <cmath>
#include <iostream>
#include <vector>

#include "Poco/Thread.h"

#include "constants.h"
#include "hotspot_lock_simulator.h"
#include "lock_manager.h"
#include "lock_simulator.h"
#include "mpi.h"
#include "tpcc_lock_simulator.h"

using namespace std;
using namespace rdma::proto;

void* MeasureCPUUsage(void* args);

struct CPUUsage {
  double total_cpu;
  double num_sample;
  bool terminate;
};

int main(int argc, char** argv) {
  if (argc != 12) {
    cout << argv[0] << " <work_dir> <num_managers> <num_lock_object> <duration>"
         << " <request_size> <num_users> <lock_mode>"
         << " <num_retry>"
         << " <workload_type> <think_time_type> <enable_random_backoff> "
         << endl;
    exit(1);
  }

  if (1 == htons(1)) {
    cout << "The current machine uses BIG ENDIAN" << endl;
  } else {
    cout << "The current machine uses LITTLE ENDIAN" << endl;
  }

  int k = 1;
  string work_dir = argv[k++];
  int num_managers = atoi(argv[k++]);
  int num_lock_object = atoi(argv[k++]);
  int duration = atoi(argv[k++]);
  int request_size = atoi(argv[k++]);
  int num_users = atoi(argv[k++]);
  string lock_mode_str = argv[k++];
  int num_retry = atoi(argv[k++]);
  string workload_type = argv[k++];
  string think_time_type = argv[k++];
  string random_backoff_str = argv[k++];

  if (num_managers > 32) {
    cerr << "# of nodes must be less than 33" << endl;
    exit(-1);
  }

  LockMode lock_mode = PROXY_RETRY;

  if (lock_mode_str == "traditional") {
    lock_mode = PROXY_QUEUE;
  } else if (lock_mode_str == "simple_retry") {
    lock_mode = REMOTE_POLL;
    LockManager::SetSharedExclusiveRule("fail");
    LockManager::SetExclusiveSharedRule("fail");
    LockManager::SetExclusiveExclusiveRule("fail");
  } else if (lock_mode_str == "drtm") {
    lock_mode = REMOTE_POLL;
    LockManager::SetSharedExclusiveRule("fail");
    LockManager::SetExclusiveSharedRule("poll");
    LockManager::SetExclusiveExclusiveRule("fail");
  } else if (lock_mode_str == "ncosed") {
    lock_mode = REMOTE_NOTIFY;
  } else if (lock_mode_str == "d2lm") {
    lock_mode = REMOTE_QUEUE;
  } else {
    cerr << "Invalid lock mode: " << lock_mode_str << endl;
    exit(ERROR_INVALID_LOCK_MODE);
  }

  bool do_random_backoff = false;
  if (random_backoff_str == "true") {
    do_random_backoff = true;
  } else if (random_backoff_str == "false") {
    do_random_backoff = false;
  } else {
    cerr << "Invalid random backoff option: " << random_backoff_str << endl;
    exit(ERROR_INVALID_RANDOM_BACKOFF);
  }

  cout << "Type of Workload = " << workload_type << endl;
  cout << "Type of Think Time = " << think_time_type << endl;
  cout << "Duration = " << duration << " s" << endl;
  cout << "# Requests per Tx = " << request_size << endl;
  cout << "# Lock Objects = " << num_lock_object << endl;
  cout << "Lock Mode = " << lock_mode_str << endl;

  LockManager::SetFailRetry(num_retry);
  LockManager::SetPollRetry(num_retry);

  std::vector<std::unique_ptr<LockSimulator>> users;
  std::vector<std::unique_ptr<LockManager>> managers;
  for (int i = 0; i < num_managers; ++i) {
    std::unique_ptr<LockManager> lock_manager(
        new LockManager(argv[1], i, num_managers, num_lock_object, lock_mode));

    if (lock_manager->Initialize()) {
      cerr << "LockManager initialization failure." << endl;
      exit(-1);
    }
    managers.push_back(std::move(lock_manager));
  }
  for (int i = 0; i < num_managers; ++i) {
    Poco::Thread manager_thread;
    manager_thread.start(*managers[i]);
  }

  for (int i = 0; i < num_managers; ++i) {
    for (int j = 0; j < num_users; ++j) {
      std::unique_ptr<LockSimulator> simulator;
      // TODO: Add other simulators.
      if (workload_type == "simple") {
        simulator.reset(new LockSimulator(managers[i].get(), num_managers,
                                          num_lock_object, request_size,
                                          think_time_type, do_random_backoff));
      } else if (workload_type == "hotspot") {
        simulator.reset(new HotspotLockSimulator(
            managers[i].get(), num_managers, num_lock_object, request_size,
            think_time_type, do_random_backoff));
      } else if (workload_type == "tpcc-uniform") {
        simulator.reset(new TPCCLockSimulator(managers[i].get(), num_managers,
                                              kTPCCNumObjects, think_time_type,
                                              do_random_backoff, i));
      } else if (workload_type == "tpcc-hotspot") {
        simulator.reset(new TPCCLockSimulator(managers[i].get(), 1,
                                              kTPCCNumObjects, think_time_type,
                                              do_random_backoff, 0));
      } else {
        cerr << "Unknown workload: " << workload_type << endl;
        exit(-2);
      }
      managers[i]->RegisterUser(i + 1, simulator.get());
      users.push_back(std::move(simulator));
    }
  }

  sleep(3);

  for (int i = 0; i < num_managers; ++i) {
    if (managers[i]->InitializeLockClients()) {
      cerr << "InitializeLockClients() failed." << endl;
      exit(-1);
    }

    // wait till all clients are initialized
    while (!managers[i]->IsClientsInitialized()) {
      Poco::Thread::sleep(250);
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

  // Start lock simulators
  std::vector<std::unique_ptr<Poco::Thread>> user_threads;
  for (int i = 0; i < num_users; ++i) {
    std::unique_ptr<Poco::Thread> user_thread(new Poco::Thread);
    user_thread->start(*users[i]);
    user_threads.push_back(std::move(user_thread));
  }

  int count = 0;
  for (unsigned int i = 0; i < users.size(); ++i) {
    while (count <= duration) {
      sleep(1);
      cout << count << " : " << users[0]->GetCount() << endl;
      ++count;
    }
  }

  for (unsigned int j = 0; j < users.size(); ++j) {
    LockSimulator* simulator = users[j].get();
    cout << "Total Tx # = " << simulator->GetCount() << endl;
  }
  usage.terminate = true;
  pthread_join(cpu_measure_thread, NULL);
  cout << "Avg CPU Usage = " << usage.total_cpu / usage.num_sample << endl;
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
  percent = 0;
  while (fgets(line, 128, file) != NULL) {
    if (strncmp(line, "processor", 9) == 0) numProcessors++;
  }
  fclose(file);

  while (!usage->terminate) {
    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
        timeSample.tms_utime < lastUserCPU) {
      // Overflow detection. Just skip this value.
      //            percent = -1.0;
      //
    } else {
      percent = (timeSample.tms_stime - lastSysCPU) +
                (timeSample.tms_utime - lastUserCPU);
      percent /= (now - lastCPU);
      // percent /= numProcessors;
      percent *= 100;
    }
    lastCPU = now;
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;

    usage->total_cpu += percent;
    usage->num_sample += 1;
    sleep(1);
  }
  return NULL;
}
