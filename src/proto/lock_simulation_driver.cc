#include <iostream>
#include <vector>
#include <pthread.h>
#include <infiniband/verbs.h>
#include "lock_simulator.h"
#include "lock_manager.h"

using namespace std;
using namespace rdma::proto;

void* RunLockManager(void* args);

int main(int argc, char** argv) {
  if (argc != 5) {
    cout << argv[0] << " <work_dir> <num_lock_object>" <<
      " <lock_mode> <duration>" << endl;
    exit(1);
  }

  int num_lock_object = atoi(argv[2]);
  int lock_mode = atoi(argv[3]);
  int duration = atoi(argv[4]);

  LockManager* lock_manager = new LockManager(argv[1], 0, 1,
      atoi(argv[2]), atoi(argv[3]));

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

  sleep(1);

  if (lock_manager->InitializeLockClients()) {
     cerr << "InitializeLockClients() failed." << endl;
     exit(-1);
  }

  sleep(1);

  int num_users = 1;

  vector<LockSimulator*> users;
  for (int i=0;i<num_users;++i) {
    LockSimulator* simulator = new LockSimulator(lock_manager, i+1, 1,
        num_lock_object, duration, true);
    lock_manager->RegisterUser(i+1, simulator);
    users.push_back(simulator);
    simulator->Run();
  }

  for (int i=0;i<users.size();++i) {
    LockSimulator* simulator = users[i];
    while (simulator->GetState() != LockSimulator::STATE_DONE) {
       sleep(1);
    }
  }
}

void* RunLockManager(void* args) {
  LockManager* lock_manager = (LockManager*)args;
  lock_manager->Run();
}
