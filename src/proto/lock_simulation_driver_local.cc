#include <iostream>
#include <vector>
#include <arpa/inet.h>
#include <pthread.h>
#include <infiniband/verbs.h>
#include "mpi.h"
#include "lock_simulator.h"
#include "lock_manager.h"

using namespace std;
using namespace rdma::proto;

void* RunLockManager(void* args);

int main(int argc, char** argv) {

  if (argc != 6) {
    cout << argv[0] << " <work_dir> <num_lock_object>" <<
      " <num_users> <lock_mode> <duration>" << endl;
    exit(1);
  }

  int num_managers = 1;
  int rank = 0;

  if (1 == htons(1)) {
    cout << "The current machine uses BIG ENDIAN" << endl;
  } else {
    cout << "The current machine uses LITTLE ENDIAN" << endl;
  }

  int num_lock_object = atoi(argv[2]);
  int num_users = atoi(argv[3]);
  int lock_mode = atoi(argv[4]);
  int duration = atoi(argv[5]);

  if (rank == 0) {
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
        duration,
        false // verbose
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
    users[i]->Run();
  }

  for (int i=0;i<users.size();++i) {
    LockSimulator* simulator = users[i];
    while (simulator->GetState() != LockSimulator::STATE_DONE) {
       sleep(1);
    }
  }

  for (int i=0;i<num_managers;++i) {
    if (rank==i) {
      cout << "Node = " << rank << endl;
      for (int j=0;j<users.size();++j) {
        LockSimulator* simulator = users[j];
        cout << "Total Lock # = " << simulator->GetTotalNumLocks() << endl;
        cout << "Total Unlock # = " << simulator->GetTotalNumUnlocks() << endl;
      }
    }
  }
}

void* RunLockManager(void* args) {
  LockManager* lock_manager = (LockManager*)args;
  lock_manager->Run();
}
